#pragma once
#include <hiredis/hiredis.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include <mutex>
#include <memory>
#include <atomic>

class RedisClient {
public:
    RedisClient();
    ~RedisClient();

    bool Connect(const std::string& ip, int port,
                 const std::string& password = "",
                 int timeout_sec = 3);
    bool IsConnected() const;
    void Disconnect();
    bool Reconnect();

    void setReconnectCallback(std::function<void()> cb) { m_reconnect_cb = std::move(cb); }

    bool Exists(const std::string& key);
    bool Expire(const std::string& key, int seconds);
    int64_t TTL(const std::string& key);
    bool Del(const std::string& key);

    std::string Get(const std::string& key);
    bool Set(const std::string& key, const std::string& value);
    bool SetEx(const std::string& key, const std::string& value, int ttl_sec);
    bool SetNx(const std::string& key, const std::string& value);
    int64_t Incr(const std::string& key);
    int64_t IncrBy(const std::string& key, int64_t delta);
    int64_t Decr(const std::string& key);

    bool HSet(const std::string& key, const std::string& field, const std::string& value);
    bool HMSet(const std::string& key, const std::unordered_map<std::string, std::string>& fields);
    std::string HGet(const std::string& key, const std::string& field);
    bool HDel(const std::string& key, const std::string& field);
    int64_t HLen(const std::string& key);
    std::unordered_map<std::string, std::string> HGetAll(const std::string& key);

    int64_t LPush(const std::string& key, const std::string& value);
    int64_t RPush(const std::string& key, const std::string& value);
    std::string LPop(const std::string& key);
    std::string RPop(const std::string& key);
    int64_t LLen(const std::string& key);
    std::vector<std::string> LRange(const std::string& key, int start, int stop);

    int64_t SAdd(const std::string& key, const std::string& member);
    int64_t SRem(const std::string& key, const std::string& member);
    bool SIsMember(const std::string& key, const std::string& member);
    int64_t SCard(const std::string& key);
    std::vector<std::string> SMembers(const std::string& key);

    bool ZAdd(const std::string& key, double score, const std::string& member);
    int64_t ZRem(const std::string& key, const std::string& member);
    int64_t ZCard(const std::string& key);
    int64_t ZCount(const std::string& key, double min, double max);
    bool ZRemRangeByScore(const std::string& key, double min, double max);
    bool ZRemRangeByRank(const std::string& key, int start, int stop);
    std::vector<std::string> ZRange(const std::string& key, int start, int stop, bool with_scores = false);

    std::string Eval(const std::string& script,
                     const std::vector<std::string>& keys,
                     const std::vector<std::string>& args);

    // Eval 的数组版：Lua 返回表 {v1, v2, ...} 逐元素转 string（NIL→空串）
    std::vector<std::string> EvalArr(const std::string& script,
                                     const std::vector<std::string>& keys,
                                     const std::vector<std::string>& args);

    // 通用 argv 命令（XADD/XREADGROUP/XPENDING/XCLAIM 等），reply 由调用方
    // 通过 RedisReplyPtr 持有，离开作用域自动 freeReplyObject
    using RedisReplyPtr = std::unique_ptr<redisReply, void (*)(redisReply*)>;
    RedisReplyPtr CommandArgv(const std::vector<std::string>& args);

private:
    std::string m_ip;
    int m_port;
    std::string m_password;
    int m_timeout_sec;
    std::function<void()> m_reconnect_cb;

    // Main thread context (set by Connect)
    redisContext* m_context;
    mutable std::mutex m_mtx;

    // Per-thread context: lazily initialized on first use from worker threads
    static thread_local redisContext* t_context;

    bool auth(redisContext* ctx);
    redisContext* getContext();
    bool ensureConnected();
};
