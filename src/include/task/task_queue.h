#pragma once
// TaskQueue：Redis Stream 队列封装（兼容 Redis 6.0，不用 6.2+ 的 XAUTOCLAIM）。
//  - 提交：Lua 原子「幂等 SETNX + 元数据 + XADD 入队」，单次网络往返
//  - 消费：XREADGROUP 消费组，各节点并行拉取，吞吐随节点数扩展
//  - 完成：Lua 原子「状态机推进 + 失败重入队 + 结果流出队」带 retries 纪元防覆盖
//  - 接管：XPENDING 找 idle 超时 → XCLAIM 取回消息体 → Lua 原子「重入队 or 死信」
#include <string>
#include <vector>
#include <optional>
#include <cstring>
#include <atomic>
#include "redisutil.h"
#include "logger.h"
#include "task/task_common.h"

namespace mprpc {

class TaskQueue
{
public:
    explicit TaskQueue(RedisClient& redis) : m_redis(redis) {}

    // 首次部署时创建消费组（MKSTREAM；BUSYGROUP 说明已存在，忽略）
    void EnsureGroup()
    {
        for (int prio : {0, 1})
        {
            auto reply = m_redis.CommandArgv(
                {"XGROUP", "CREATE", QueueKey(prio), ConsumerGroup(), "0", "MKSTREAM"});
            (void)reply;
        }
    }

private:
    std::atomic<int64_t> m_last_group_repair{0};

public:

    struct SubmitResult
    {
        bool ok = false;
        bool duplicate = false;
        std::string task_id;
    };

    // Lua 原子提交
    SubmitResult Submit(const TaskMeta& meta)
    {
        static const std::string kScript = R"(
local existed = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2])
if not existed then
  return {'0', redis.call('GET', KEYS[1]) or ''}
end
redis.call('HSET', KEYS[2],
  'type', ARGV[3], 'payload', ARGV[4], 'priority', ARGV[5],
  'timeout_ms', ARGV[6], 'max_retry', ARGV[7], 'idempotency_key', ARGV[8],
  'state', '0', 'retries', '0', 'submitted_at', ARGV[9])
redis.call('EXPIRE', KEYS[2], ARGV[2])
redis.call('XADD', KEYS[3], '*', 'task_id', ARGV[1])
redis.call('XTRIM', KEYS[3], 'MAXLEN', '~', ARGV[10])
return {'1', ARGV[1]}
)";
        int prio = 1;
        try { prio = std::stoi(meta.priority); } catch (...) {}
        std::vector<std::string> keys = {
            "task:idem:" + meta.idempotency_key, ItemKey(meta.task_id), QueueKey(prio)};
        std::vector<std::string> args = {
            meta.task_id, std::to_string(kTaskTtlSec),
            meta.type, meta.payload, meta.priority,
            meta.timeout_ms, meta.max_retry, meta.idempotency_key,
            meta.submitted_at, std::to_string(kQueueTrimLen)};

        auto reply = m_redis.EvalArr(keys.empty() ? "" : kScript, keys, args);
        SubmitResult r;
        if (reply.size() < 2) return r;
        r.duplicate = (reply[0] == "0");
        r.task_id = reply[1];
        r.ok = !r.task_id.empty();
        return r;
    }

    // 消费消息（结果带来源队列，ACK 需定位原始 Stream）
    struct Msg
    {
        std::string queue_key;
        std::string msg_id;
        std::string task_id;
    };

    std::vector<Msg> Consume(const std::string& consumer, int count, int block_ms)
    {
        std::vector<Msg> out;
        std::vector<std::string> args = {
            "XREADGROUP", "GROUP", ConsumerGroup(), consumer,
            "COUNT", std::to_string(count), "BLOCK", std::to_string(block_ms),
            "STREAMS", QueueKey(0), QueueKey(1), ">", ">"};
        auto reply = m_redis.CommandArgv(args);
        if (!reply) return out;
        // NOGROUP/NOKEY 自愈：Stream 被删（Redis 重启、主从切换）后重建消费组。
        // 节流：10s 内只重建一次，避免错误风暴。
        if (reply->type == REDIS_REPLY_ERROR)
        {
            int64_t now = NowMs();
            if (now - m_last_group_repair.load() > 10000)
            {
                m_last_group_repair.store(now);
                LOG_WARN("Consumer group missing, re-creating (self-heal)");
                EnsureGroup();
            }
            return out;
        }
        if (reply->type != REDIS_REPLY_ARRAY || reply->elements == 0)
            return out;

        // reply: [ [stream, [[id, [field, value, ...]], ...]], ... ]
        for (size_t s = 0; s < reply->elements; ++s)
        {
            redisReply* stream = reply->element[s];
            if (stream->type != REDIS_REPLY_ARRAY || stream->elements < 2 ||
                stream->element[1]->type != REDIS_REPLY_ARRAY)
                continue;
            std::string queue_key(stream->element[0]->str, stream->element[0]->len);
            redisReply* msgs = stream->element[1];
            for (size_t i = 0; i < msgs->elements; ++i)
            {
                redisReply* entry = msgs->element[i];
                if (entry->type != REDIS_REPLY_ARRAY || entry->elements < 2 ||
                    entry->element[1]->type != REDIS_REPLY_ARRAY)
                    continue;
                Msg m;
                m.queue_key = queue_key;
                m.msg_id.assign(entry->element[0]->str, entry->element[0]->len);
                redisReply* fields = entry->element[1];
                for (size_t f = 0; f + 1 < fields->elements; f += 2)
                {
                    if (fields->element[f]->len == 7 &&
                        memcmp(fields->element[f]->str, "task_id", 7) == 0)
                        m.task_id.assign(fields->element[f + 1]->str, fields->element[f + 1]->len);
                }
                if (!m.task_id.empty()) out.push_back(std::move(m));
            }
        }
        return out;
    }

    bool Ack(const std::string& queue_key, const std::string& msg_id)
    {
        auto reply = m_redis.CommandArgv({"XACK", queue_key, ConsumerGroup(), msg_id});
        return reply && reply->type == REDIS_REPLY_INTEGER && reply->integer > 0;
    }

    int QueueDepth(int prio)
    {
        auto reply = m_redis.CommandArgv({"XLEN", QueueKey(prio)});
        return (reply && reply->type == REDIS_REPLY_INTEGER) ? (int)reply->integer : 0;
    }

    // ── 接管（故障自愈核心）：回收 idle 超时的 pending 消息 ──
    // XPENDING 列出 → XCLAIM 取回消息体（所有权转移到回收者名下，min-idle-time
    // 双保险防竞争）→ Lua 原子判定「retries+1 重入队 or 标 DEAD」并 XACK。
    // 返回回收条数。
    int ReclaimTimedOut(const std::string& recycler, int limit)
    {
        int reclaimed = 0;
        for (int prio : {0, 1})
        {
            std::string qk = QueueKey(prio);

            // 1) pending 总览：[id, consumer, idle_ms, delivery_count]
            auto pend = m_redis.CommandArgv(
                {"XPENDING", qk, ConsumerGroup(), "-", "+", std::to_string(limit)});
            if (!pend || pend->type != REDIS_REPLY_ARRAY) continue;

            std::vector<std::string> stale_ids;
            for (size_t i = 0; i < pend->elements; ++i)
            {
                redisReply* row = pend->element[i];
                if (row->type != REDIS_REPLY_ARRAY || row->elements < 3 ||
                    row->element[0]->type != REDIS_REPLY_STRING)
                    continue;
                if (row->element[2]->integer >= kClaimMinIdleMs)
                    stale_ids.emplace_back(row->element[0]->str, row->element[0]->len);
            }
            if (stale_ids.empty()) continue;

            // 2) XCLAIM 取回消息体（顺带转移所有权）
            std::vector<std::string> claim_args = {
                "XCLAIM", qk, ConsumerGroup(), recycler,
                std::to_string(kClaimMinIdleMs)};
            claim_args.insert(claim_args.end(), stale_ids.begin(), stale_ids.end());
            auto claimed = m_redis.CommandArgv(claim_args);
            if (!claimed || claimed->type != REDIS_REPLY_ARRAY) continue;

            // 3) 逐条接管判定
            for (size_t i = 0; i < claimed->elements; ++i)
            {
                redisReply* entry = claimed->element[i];
                if (entry->type != REDIS_REPLY_ARRAY || entry->elements < 2 ||
                    entry->element[1]->type != REDIS_REPLY_ARRAY)
                    continue;
                std::string msg_id(entry->element[0]->str, entry->element[0]->len);
                std::string task_id;
                redisReply* fields = entry->element[1];
                for (size_t f = 0; f + 1 < fields->elements; f += 2)
                {
                    if (fields->element[f]->len == 7 &&
                        memcmp(fields->element[f]->str, "task_id", 7) == 0)
                        task_id.assign(fields->element[f + 1]->str, fields->element[f + 1]->len);
                }
                if (task_id.empty()) { Ack(qk, msg_id); continue; }
                if (ReclaimOne(task_id, qk, msg_id)) ++reclaimed;
            }
        }
        return reclaimed;
    }

    // 加载任务元数据
    std::optional<TaskMeta> Load(const std::string& task_id)
    {
        auto reply = m_redis.CommandArgv({"HGETALL", ItemKey(task_id)});
        if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements == 0)
            return std::nullopt;
        std::unordered_map<std::string, std::string> hash;
        for (size_t i = 0; i + 1 < reply->elements; i += 2)
            hash.emplace(std::string(reply->element[i]->str, reply->element[i]->len),
                         std::string(reply->element[i + 1]->str, reply->element[i + 1]->len));
        if (hash.empty() || hash.find("state") == hash.end())
            return std::nullopt;
        return TaskMeta::FromHash(hash, task_id);
    }

private:
    // 接管 Lua：RUNNING/PENDING 且未到终态的任务，
    //   retries+1 < max_retry → 回置 PENDING 重入队（计数 +1 形成新纪元）
    //   否则 → 标 DEAD（重试耗尽）
    // XACK 旧消息防重复回收；同时写结果流供管控面对账。
    bool ReclaimOne(const std::string& task_id, const std::string& queue_key,
                    const std::string& msg_id)
    {
        static const std::string kScript = R"(
local item = KEYS[1]
local state = redis.call('HGET', item, 'state')
if state ~= '1' and state ~= '0' then
  redis.call('XACK', KEYS[2], ARGV[4], ARGV[1])
  return '0'
end
local retries = tonumber(redis.call('HGET', item, 'retries') or '0')
local max_retry = tonumber(redis.call('HGET', item, 'max_retry') or '0')
redis.call('XACK', KEYS[2], ARGV[4], ARGV[1])
if retries < max_retry then
  redis.call('HSET', item, 'state', '0', 'retries', retries + 1,
             'fail_reason', 'worker lost, reclaimed')
  redis.call('XADD', KEYS[3], '*', 'task_id', ARGV[2])
  redis.call('XTRIM', KEYS[3], 'MAXLEN', '~', ARGV[5])
  redis.call('XADD', KEYS[4], '*', 'task_id', ARGV[2], 'event', 'reclaimed')
  redis.call('XTRIM', KEYS[4], 'MAXLEN', '~', ARGV[6])
  return '1'
end
redis.call('HSET', item, 'state', '4', 'finished_at', ARGV[3],
           'fail_reason', 'worker lost, retries exhausted')
redis.call('XADD', KEYS[4], '*', 'task_id', ARGV[2], 'event', 'dead')
redis.call('XTRIM', KEYS[4], 'MAXLEN', '~', ARGV[6])
return '2'
)";
        std::vector<std::string> keys = {
            ItemKey(task_id), queue_key, QueueKey(PrioOf(task_id)), ResultStream()};
        std::vector<std::string> args = {
            msg_id, task_id, std::to_string(NowMs()), ConsumerGroup(),
            std::to_string(kQueueTrimLen), std::to_string(kResultTrimLen)};
        auto reply = m_redis.EvalArr(kScript, keys, args);
        return !reply.empty() && reply[0] != "0";
    }

    // 从元数据读优先级（接管时任务已存在，priority 在 item hash 里）
    std::string PrioOf(const std::string& task_id)
    {
        auto reply = m_redis.CommandArgv({"HGET", ItemKey(task_id), "priority"});
        if (reply && reply->type == REDIS_REPLY_STRING && reply->str)
            return std::string(reply->str, reply->len);
        return "1";
    }

    RedisClient& m_redis;
};

} // namespace mprpc
