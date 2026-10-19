#pragma once
// TaskHub 公共约定：key 布局、时间/ID 工具、任务元数据视图。
// 约定集中在此，C++ provider、CLI、Python 管控面三方共用同一套 key 语义。
#include <string>
#include <vector>
#include <chrono>
#include <atomic>
#include <unordered_map>
#include <cstdint>

namespace mprpc {

// ── Redis key 布局 ────────────────────────────────────────
// task:queue:{prio}      Stream，待执行队列（prio: 0=HIGH 1=NORMAL）
// task:item:{task_id}    Hash，任务元数据（平铺字段，跨语言可读），TTL 24h
// task:idem:{key}        SETNX 幂等标记 → task_id，TTL 24h
// task:result            Stream，终态事件流（供管控面落库/对账），XTRIM 控制
inline std::string QueueKey(int priority) { return "task:queue:" + std::to_string(priority); }
inline std::string QueueKey(const std::string& priority) { return "task:queue:" + priority; }
inline std::string ItemKey(const std::string& task_id) { return "task:item:" + task_id; }
inline const char* ResultStream() { return "task:result"; }
inline const char* ConsumerGroup() { return "task:cg"; }

// 队列长度裁剪水位：消息 ACK 后仍留在 Stream 中，靠 XTRIM 控制体积
constexpr int kQueueTrimLen = 5000;
constexpr int kResultTrimLen = 10000;
// 幂等键/元数据 TTL（秒）
constexpr int kTaskTtlSec = 86400;
// 接管判定：消息 idle 超过此值视为 worker 失联（须大于任务超时上限 10s）
constexpr int kClaimMinIdleMs = 15000;

inline int64_t NowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// task_id：t- + 时间戳(hex) + 随机数(hex)，进程内单调，跨节点靠长度碰撞概率可忽略
inline std::string GenTaskId()
{
    static std::atomic<uint64_t> s_seq{0};
    uint64_t now = static_cast<uint64_t>(NowMs());
    uint64_t rnd = static_cast<uint64_t>(::rand()) << 32 | s_seq.fetch_add(1);
    char buf[40];
    snprintf(buf, sizeof(buf), "t-%012llx-%08llx",
             static_cast<unsigned long long>(now),
             static_cast<unsigned long long>(rnd & 0xFFFFFFFFULL));
    return buf;
}

// 任务元数据（task:item:{id} 的 Hash 平铺视图）
struct TaskMeta
{
    std::string task_id;
    std::string type;             // "0"/"1"/"2" 对应 SLEEP/COMPUTE/FAIL
    std::string payload;
    std::string priority;         // 队列优先级
    std::string timeout_ms;
    std::string max_retry;
    std::string idempotency_key;
    std::string state;            // "PENDING"/"RUNNING"/...
    std::string retries;
    std::string worker;
    std::string submitted_at;
    std::string started_at;
    std::string finished_at;
    std::string result_payload;
    std::string fail_reason;

    static TaskMeta FromHash(const std::unordered_map<std::string, std::string>& h,
                             const std::string& task_id)
    {
        TaskMeta m;
        m.task_id = task_id;
        auto get = [&](const char* k) {
            auto it = h.find(k);
            return it == h.end() ? "" : it->second;
        };
        m.type = get("type");             m.payload = get("payload");
        m.priority = get("priority");     m.timeout_ms = get("timeout_ms");
        m.max_retry = get("max_retry");   m.idempotency_key = get("idempotency_key");
        m.state = get("state");           m.retries = get("retries");
        m.worker = get("worker");         m.submitted_at = get("submitted_at");
        m.started_at = get("started_at"); m.finished_at = get("finished_at");
        m.result_payload = get("result_payload");
        m.fail_reason = get("fail_reason");
        return m;
    }
};

} // namespace mprpc
