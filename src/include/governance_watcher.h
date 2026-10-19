#pragma once
// GovernanceWatcher：治理规则热下发接收端。
// 轮询 Redis rules:version，变更后拉取 rules:provider:* 哈希，
// 经 ApplyFn 回调注入（provider 侧热更新 RateLimiter 参数）。
// 控制面在规则 CRUD 后写哈希并 INCR 版本号——页面改规则秒级生效。
#include <atomic>
#include <thread>
#include <functional>
#include <map>
#include <memory>
#include "redisutil.h"
#include "logger.h"
#include "prometheus/metrics_exporter.h"

namespace mprpc {

struct GovernanceRules
{
    int max_qps = 1000;
    int window_sec = 1;
    int algorithm = 3;      // RateLimitAlgorithm
    bool enable = false;
};

class GovernanceWatcher
{
public:
    using ApplyFn = std::function<void(const GovernanceRules&)>;

    explicit GovernanceWatcher(std::shared_ptr<RedisClient> redis)
        : m_redis(std::move(redis)) {}

    ~GovernanceWatcher() { Stop(); }

    void SetApplyCallback(ApplyFn fn) { m_apply = std::move(fn); }

    void Start()
    {
        if (m_running.exchange(true)) return;
        m_thread = std::thread([this] { Loop(); });
        LOG_INFO("GovernanceWatcher started (poll=%dms)", kPollMs);
    }

    void Stop()
    {
        if (!m_running.exchange(false)) return;
        if (m_thread.joinable()) m_thread.join();
    }

    GovernanceRules current() const { return m_current; }

private:
    static constexpr int kPollMs = 2000;
    static constexpr const char* kVersionKey = "rules:version";
    static constexpr const char* kRateKey = "rules:provider:ratelimit";

    void Loop()
    {
        int64_t last_version = -1;
        while (m_running)
        {
            auto ver = m_redis->Get(kVersionKey);
            int64_t v = ver.empty() ? 0 : atoll(ver.c_str());
            if (v != last_version)
            {
                FetchAndApply();
                last_version = v;
            }
            for (int i = 0; i < kPollMs / 100 && m_running; ++i)
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    void FetchAndApply()
    {
        auto reply = m_redis->CommandArgv({"HGETALL", kRateKey});
        if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements == 0)
            return; // 无规则 → 保持现状（enable=false 由控制面显式写入）

        std::map<std::string, std::string> h;
        for (size_t i = 0; i + 1 < reply->elements; i += 2)
            h.emplace(std::string(reply->element[i]->str, reply->element[i]->len),
                      std::string(reply->element[i + 1]->str, reply->element[i + 1]->len));

        GovernanceRules rules;
        rules.max_qps = atoi(h.count("max_qps") ? h["max_qps"].c_str() : "1000");
        rules.window_sec = atoi(h.count("window_sec") ? h["window_sec"].c_str() : "1");
        rules.algorithm = atoi(h.count("algorithm") ? h["algorithm"].c_str() : "3");
        rules.enable = h.count("enable") && h["enable"] == "true";
        if (rules.max_qps <= 0) rules.max_qps = 1000;
        if (rules.window_sec <= 0) rules.window_sec = 1;

        m_current = rules;
        if (m_apply) m_apply(rules);
        MetricsExporter::GetInstance().IncrCounter("governance_reload_total", {});
        LOG_INFO("Governance rules applied: enable=%d max_qps=%d window=%ds algo=%d",
                 rules.enable, rules.max_qps, rules.window_sec, rules.algorithm);
    }

    std::shared_ptr<RedisClient> m_redis;
    std::atomic<bool> m_running{false};
    std::thread m_thread;
    ApplyFn m_apply;
    GovernanceRules m_current;
};

} // namespace mprpc
