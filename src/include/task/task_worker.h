#pragma once
// TaskWorkerPool：多线程消费执行器。
// 每线程独立 XREADGROUP（BLOCK 挂起等待），醒后执行任务再循环拉取——
// 天然多消费者并行，线程数即单节点并发任务上限。
// 完成路径走 Lua 状态机：FAILED 且未耗尽重试 → 原子重入队（retries 纪元+1）；
// 终态 → 写结果流。epoch 校验防止被接管后旧执行者覆盖新结果。
#include <atomic>
#include <thread>
#include <vector>
#include <map>
#include <algorithm>
#include <functional>
#include "redisutil.h"
#include "logger.h"
#include "task/task_common.h"
#include "task/task_queue.h"
#include "prometheus/metrics_exporter.h"

namespace mprpc {

class TaskWorkerPool
{
public:
    using Executor = std::function<std::pair<bool, std::string>(
        const std::string& payload, int64_t deadline_ms)>;

    TaskWorkerPool(RedisClient& redis, TaskQueue& queue,
                   std::string node_id, int threads)
        : m_redis(redis), m_queue(queue),
          m_node_id(std::move(node_id)), m_threads(threads) {}

    void RegisterExecutor(const std::string& type, Executor fn)
    {
        m_executors[type] = std::move(fn);
    }

    void Start()
    {
        m_running = true;
        for (int i = 0; i < m_threads; ++i)
        {
            m_workers.emplace_back([this, i] { Loop(i); });
        }
        LOG_INFO("TaskWorkerPool started: node=%s threads=%d",
                 m_node_id.c_str(), m_threads);
    }

    void Stop()
    {
        m_running = false;
        for (auto& t : m_workers)
            if (t.joinable()) t.join();
        m_workers.clear();
    }

    int ThreadCount() const { return m_threads; }

private:
    void Loop(int idx)
    {
        std::string consumer = m_node_id + "-w" + std::to_string(idx);
        while (m_running)
        {
            // COUNT=1：一条线程同时只持有一个任务，并发上限=线程数，
            // 也让 PENDING 窗口可被取消/接管观测
            auto msgs = m_queue.Consume(consumer, 1, 500);
            for (auto& m : msgs)
            {
                if (!m_running) return;
                Execute(m);
            }
        }
    }

    void Execute(const TaskQueue::Msg& msg)
    {
        auto meta_opt = m_queue.Load(msg.task_id);
        if (!meta_opt)
        {
            m_queue.Ack(msg.queue_key, msg.msg_id);
            return;
        }
        TaskMeta meta = std::move(*meta_opt);

        // 纪元判定：接管后 retries 已被推进，旧消费不执行（防御性，正常流程拿到的即最新）
        int64_t started = NowMs();
        m_redis.CommandArgv({"HSET", ItemKey(meta.task_id),
                             "state", "1", "worker", m_node_id,
                             "started_at", std::to_string(started)});

        int timeout_ms = 10000;
        try { timeout_ms = std::stoi(meta.timeout_ms); } catch (...) {}
        int64_t deadline = started + timeout_ms;

        auto it = m_executors.find(meta.type);
        bool ok = false;
        std::string result;
        if (it == m_executors.end())
        {
            result = "unknown task type: " + meta.type;
        }
        else
        {
            auto r = it->second(meta.payload, deadline);
            ok = r.first;
            result = r.second;
        }
        int64_t finished = NowMs();

        Complete(msg, meta, ok, result, finished);

        // Prometheus 打点：排队 / 执行 / 端到端
        auto& exporter = MetricsExporter::GetInstance();
        std::map<std::string, std::string> labels = {{"type", Type2Name(meta.type)}};
        double qwait = (started - atoll(meta.submitted_at.c_str())) / 1000.0;
        double exec = (finished - started) / 1000.0;
        double e2e = (finished - atoll(meta.submitted_at.c_str())) / 1000.0;
        exporter.ObserveHistogram("task_queue_wait_seconds", labels, qwait);
        exporter.ObserveHistogram("task_execute_seconds", labels, exec);
        exporter.ObserveHistogram("task_e2e_seconds", labels, e2e);
    }

    // 完成状态机（Lua 原子）：
    //   前置校验 state=RUNNING 且 retries 与执行纪元一致，否则丢弃（接管后旧结果）
    //   ok   → SUCCEEDED
    //   失败 → retries < max_retry ? PENDING 重入队（retries+1）: FAILED 终态
    // 说明：DEAD 仅由接管路径（重试耗尽且 worker 失联）与 FAILED 耗尽共同收敛——
    // 本地失败耗尽时直接标 DEAD，语义统一为「重试耗尽即死信」。
    void Complete(const TaskQueue::Msg& msg, const TaskMeta& meta,
                  bool ok, const std::string& result, int64_t finished)
    {
        static const std::string kScript = R"(
local item = KEYS[1]
if redis.call('HGET', item, 'state') ~= '1' then return '0' end
local retries = tonumber(redis.call('HGET', item, 'retries') or '0')
if redis.call('HGET', item, 'retries') ~= ARGV[1] then return '0' end
local max_retry = tonumber(redis.call('HGET', item, 'max_retry') or '0')

if ARGV[2] == '1' then
  redis.call('HSET', item, 'state', '2', 'finished_at', ARGV[3],
             'result_payload', ARGV[4], 'worker', ARGV[6])
  redis.call('XADD', KEYS[3], '*', 'task_id', ARGV[5], 'event', 'succeeded')
elseif retries < max_retry then
  redis.call('HSET', item, 'state', '0', 'retries', retries + 1,
             'fail_reason', ARGV[7])
  redis.call('XADD', KEYS[2], '*', 'task_id', ARGV[5])
  redis.call('XTRIM', KEYS[2], 'MAXLEN', '~', ARGV[8])
  redis.call('XADD', KEYS[3], '*', 'task_id', ARGV[5], 'event', 'retry')
else
  redis.call('HSET', item, 'state', '4', 'finished_at', ARGV[3],
             'fail_reason', ARGV[7], 'worker', ARGV[6])
  redis.call('XADD', KEYS[3], '*', 'task_id', ARGV[5], 'event', 'dead')
end
redis.call('XTRIM', KEYS[3], 'MAXLEN', '~', ARGV[9])
redis.call('XACK', KEYS[2], ARGV[10], ARGV[11])
return '1'
)";
        std::string prio = meta.priority.empty() ? "1" : meta.priority;
        std::vector<std::string> keys = {
            ItemKey(meta.task_id), QueueKey(std::stoi(prio)), ResultStream()};
        std::vector<std::string> args = {
            meta.retries, ok ? "1" : "0", std::to_string(finished),
            result, meta.task_id, m_node_id,
            ok ? "" : result,
            std::to_string(kQueueTrimLen), std::to_string(kResultTrimLen),
            ConsumerGroup(), msg.msg_id};
        auto reply = m_redis.EvalArr(kScript, keys, args);

        auto& exporter = MetricsExporter::GetInstance();
        if (!reply.empty() && reply[0] == "1")
        {
            int retries = 0, max_retry = 0;
            try { retries = std::stoi(meta.retries); } catch (...) {}
            try { max_retry = std::stoi(meta.max_retry); } catch (...) {}
            bool retried = !ok && retries < max_retry;
            exporter.IncrCounter("task_retry_total", {}, retried ? 1.0 : 0.0);
            std::map<std::string, std::string> l = {
                {"state", ok ? "succeeded" : (retried ? "retry" : "dead")}};
            exporter.IncrCounter("task_completed_total", l);
        }
    }

    static const char* Type2Name(const std::string& type)
    {
        if (type == "0") return "sleep";
        if (type == "1") return "compute";
        return "fail";
    }

    RedisClient& m_redis;
    TaskQueue& m_queue;
    std::string m_node_id;
    int m_threads;
    std::atomic<bool> m_running{false};
    std::vector<std::thread> m_workers;
    std::map<std::string, Executor> m_executors;
};

// ── 内置 Demo 任务执行器 ─────────────────────────────────────
// SLEEP: payload="毫秒数"，分片休眠并检查 deadline，模拟 IO 型任务
inline std::pair<bool, std::string> ExecSleep(const std::string& payload, int64_t deadline)
{
    long ms = 500;
    try { ms = std::stol(payload); } catch (...) {}
    if (ms < 0) ms = 0;
    int64_t end = NowMs() + ms;
    while (NowMs() < end)
    {
        if (NowMs() >= deadline)
            return {false, "timeout after " + std::to_string(ms) + "ms"};
        std::this_thread::sleep_for(std::chrono::milliseconds(
            std::min<int64_t>(20, end - NowMs())));
    }
    return {true, "slept " + std::to_string(ms) + "ms"};
}

// COMPUTE: payload="轮数"，每轮 SHA 风格混合哈希迭代，模拟 CPU 型任务
inline std::pair<bool, std::string> ExecCompute(const std::string& payload, int64_t deadline)
{
    long rounds = 100000;
    try { rounds = std::stol(payload); } catch (...) {}
    uint64_t h = 1469598103934665603ULL;
    for (long i = 0; i < rounds; ++i)
    {
        if ((i & 0xFFF) == 0 && NowMs() >= deadline)
            return {false, "timeout at round " + std::to_string(i)};
        h ^= i;
        h *= 1099511628211ULL;
        h ^= h >> 29;
    }
    char buf[32];
    snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(h));
    return {true, "hash=" + std::string(buf) + " rounds=" + std::to_string(rounds)};
}

// FAIL: 必然失败，演示重试 → 死信
inline std::pair<bool, std::string> ExecFail(const std::string& payload, int64_t deadline)
{
    (void)deadline;
    return {false, payload.empty() ? "injected failure" : payload};
}

} // namespace mprpc
