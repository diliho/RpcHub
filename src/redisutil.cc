#include "redisutil.h"
#include "logger.h"
#include <cstring>
#include <cstdarg>

thread_local redisContext* RedisClient::t_context = nullptr;

RedisClient::RedisClient() : m_context(nullptr), m_port(6379), m_timeout_sec(3) {}

RedisClient::~RedisClient()
{
    std::lock_guard<std::mutex> lock(m_mtx);
    if (m_context) { redisFree(m_context); m_context = nullptr; }
    if (t_context) { redisFree(t_context); t_context = nullptr; }
}

redisContext* RedisClient::getContext()
{
    if (t_context) return t_context;
    // Lazy init a per-thread context. Never share m_context across threads:
    // hiredis connections are not thread-safe (concurrent use desyncs the
    // reply stream and blocks readers forever).
    if (m_ip.empty()) return nullptr;
    struct timeval tv = {m_timeout_sec, 0};
    t_context = redisConnectWithTimeout(m_ip.c_str(), m_port, tv);
    if (!t_context || t_context->err)
    {
        if (t_context) { redisFree(t_context); t_context = nullptr; }
        return nullptr;
    }
    if (!m_password.empty()) auth(t_context);
    return t_context;
}

bool RedisClient::Connect(const std::string& ip, int port,
                          const std::string& password, int timeout_sec)
{
    std::lock_guard<std::mutex> lock(m_mtx);
    m_ip = ip;
    m_port = port;
    m_password = password;
    m_timeout_sec = timeout_sec;

    struct timeval tv = {timeout_sec, 0};
    m_context = redisConnectWithTimeout(ip.c_str(), port, tv);
    if (!m_context || m_context->err)
    {
        LOG_ERROR("Redis connect failed: %s",
                  m_context ? m_context->errstr : "allocation failed");
        if (m_context) { redisFree(m_context); m_context = nullptr; }
        return false;
    }

    if (!password.empty() && !auth(m_context))
    {
        redisFree(m_context);
        m_context = nullptr;
        return false;
    }

    LOG_INFO("Redis connected to %s:%d", ip.c_str(), port);
    return true;
}

bool RedisClient::auth(redisContext* ctx)
{
    redisReply* reply = (redisReply*)redisCommand(ctx, "AUTH %s", m_password.c_str());
    if (!reply) { LOG_ERROR("Redis AUTH failed (null reply)"); return false; }
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    if (!ok) LOG_ERROR("Redis AUTH failed: %s", reply->str ? reply->str : "unknown");
    freeReplyObject(reply);
    return ok;
}

bool RedisClient::IsConnected() const
{
    redisContext* ctx = t_context ? t_context : m_context;
    return ctx && ctx->err == 0;
}

void RedisClient::Disconnect()
{
    std::lock_guard<std::mutex> lock(m_mtx);
    if (m_context) { redisFree(m_context); m_context = nullptr; }
    if (t_context) { redisFree(t_context); t_context = nullptr; }
}

bool RedisClient::Reconnect()
{
    Disconnect();
    bool ok = Connect(m_ip, m_port, m_password, m_timeout_sec);
    if (ok && m_reconnect_cb) m_reconnect_cb();
    return ok;
}

bool RedisClient::ensureConnected()
{
    redisContext* ctx = getContext();
    if (ctx && ctx->err == 0) return true;
    // Thread-local context disconnected, reconnect
    if (t_context) { redisFree(t_context); t_context = nullptr; }
    return getContext() != nullptr;
}

// ── Key ──

bool RedisClient::Exists(const std::string& key)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "EXISTS %s", key.c_str());
    if (!reply) return false;
    bool exists = (reply->type == REDIS_REPLY_INTEGER && reply->integer == 1);
    freeReplyObject(reply);
    return exists;
}

bool RedisClient::Expire(const std::string& key, int seconds)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "EXPIRE %s %d", key.c_str(), seconds);
    if (!reply) return false;
    bool ok = (reply->type == REDIS_REPLY_INTEGER && reply->integer == 1);
    freeReplyObject(reply);
    return ok;
}

int64_t RedisClient::TTL(const std::string& key)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return -2;
    redisReply* reply = (redisReply*)redisCommand(ctx, "TTL %s", key.c_str());
    if (!reply) return -2;
    int64_t ttl = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : -2;
    freeReplyObject(reply);
    return ttl;
}

bool RedisClient::Del(const std::string& key)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "DEL %s", key.c_str());
    if (!reply) return false;
    freeReplyObject(reply);
    return true;
}

// ── String ──

std::string RedisClient::Get(const std::string& key)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return "";
    redisReply* reply = (redisReply*)redisCommand(ctx, "GET %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_STRING) { if (reply) freeReplyObject(reply); return ""; }
    std::string val = reply->str ? reply->str : "";
    freeReplyObject(reply);
    return val;
}

bool RedisClient::Set(const std::string& key, const std::string& value)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "SET %s %s", key.c_str(), value.c_str());
    if (!reply) return false;
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

bool RedisClient::SetEx(const std::string& key, const std::string& value, int ttl_sec)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "SETEX %s %d %s", key.c_str(), ttl_sec, value.c_str());
    if (!reply) return false;
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

bool RedisClient::SetNx(const std::string& key, const std::string& value)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "SETNX %s %s", key.c_str(), value.c_str());
    if (!reply) return false;
    bool ok = (reply->type == REDIS_REPLY_INTEGER && reply->integer == 1);
    freeReplyObject(reply);
    return ok;
}

int64_t RedisClient::Incr(const std::string& key)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return -1;
    redisReply* reply = (redisReply*)redisCommand(ctx, "INCR %s", key.c_str());
    if (!reply) return -1;
    int64_t val = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : -1;
    freeReplyObject(reply);
    return val;
}

int64_t RedisClient::IncrBy(const std::string& key, int64_t delta)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return -1;
    redisReply* reply = (redisReply*)redisCommand(ctx, "INCRBY %s %ld", key.c_str(), delta);
    if (!reply) return -1;
    int64_t val = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : -1;
    freeReplyObject(reply);
    return val;
}

int64_t RedisClient::Decr(const std::string& key)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return -1;
    redisReply* reply = (redisReply*)redisCommand(ctx, "DECR %s", key.c_str());
    if (!reply) return -1;
    int64_t val = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : -1;
    freeReplyObject(reply);
    return val;
}

// ── Hash ──

bool RedisClient::HSet(const std::string& key, const std::string& field, const std::string& value)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "HSET %s %s %s", key.c_str(), field.c_str(), value.c_str());
    if (!reply) return false;
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

bool RedisClient::HMSet(const std::string& key, const std::unordered_map<std::string, std::string>& fields)
{
    if (fields.empty()) return true;
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return false;

    std::string cmd = "HMSET " + key;
    for (auto& kv : fields)
    {
        cmd += " " + kv.first + " " + kv.second;
    }

    redisReply* reply = (redisReply*)redisCommand(ctx, cmd.c_str());
    if (!reply) return false;
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

std::string RedisClient::HGet(const std::string& key, const std::string& field)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return "";
    redisReply* reply = (redisReply*)redisCommand(ctx, "HGET %s %s", key.c_str(), field.c_str());
    if (!reply || reply->type != REDIS_REPLY_STRING) { if (reply) freeReplyObject(reply); return ""; }
    std::string val = reply->str ? reply->str : "";
    freeReplyObject(reply);
    return val;
}

bool RedisClient::HDel(const std::string& key, const std::string& field)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "HDEL %s %s", key.c_str(), field.c_str());
    if (!reply) return false;
    freeReplyObject(reply);
    return true;
}

int64_t RedisClient::HLen(const std::string& key)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return 0;
    redisReply* reply = (redisReply*)redisCommand(ctx, "HLEN %s", key.c_str());
    if (!reply) return 0;
    int64_t val = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : 0;
    freeReplyObject(reply);
    return val;
}

std::unordered_map<std::string, std::string> RedisClient::HGetAll(const std::string& key)
{
    std::unordered_map<std::string, std::string> result;
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return result;
    redisReply* reply = (redisReply*)redisCommand(ctx, "HGETALL %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_ARRAY) { if (reply) freeReplyObject(reply); return result; }
    for (size_t i = 0; i + 1 < reply->elements; i += 2)
    {
        std::string f = reply->element[i]->str ? reply->element[i]->str : "";
        std::string v = reply->element[i + 1]->str ? reply->element[i + 1]->str : "";
        result[f] = v;
    }
    freeReplyObject(reply);
    return result;
}

// ── List ──

int64_t RedisClient::LPush(const std::string& key, const std::string& value)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return -1;
    redisReply* reply = (redisReply*)redisCommand(ctx, "LPUSH %s %s", key.c_str(), value.c_str());
    if (!reply) return -1;
    int64_t val = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : -1;
    freeReplyObject(reply);
    return val;
}

int64_t RedisClient::RPush(const std::string& key, const std::string& value)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return -1;
    redisReply* reply = (redisReply*)redisCommand(ctx, "RPUSH %s %s", key.c_str(), value.c_str());
    if (!reply) return -1;
    int64_t val = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : -1;
    freeReplyObject(reply);
    return val;
}

std::string RedisClient::LPop(const std::string& key)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return "";
    redisReply* reply = (redisReply*)redisCommand(ctx, "LPOP %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_STRING) { if (reply) freeReplyObject(reply); return ""; }
    std::string val = reply->str ? reply->str : "";
    freeReplyObject(reply);
    return val;
}

std::string RedisClient::RPop(const std::string& key)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return "";
    redisReply* reply = (redisReply*)redisCommand(ctx, "RPOP %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_STRING) { if (reply) freeReplyObject(reply); return ""; }
    std::string val = reply->str ? reply->str : "";
    freeReplyObject(reply);
    return val;
}

int64_t RedisClient::LLen(const std::string& key)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return 0;
    redisReply* reply = (redisReply*)redisCommand(ctx, "LLEN %s", key.c_str());
    if (!reply) return 0;
    int64_t val = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : 0;
    freeReplyObject(reply);
    return val;
}

std::vector<std::string> RedisClient::LRange(const std::string& key, int start, int stop)
{
    std::vector<std::string> result;
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return result;
    redisReply* reply = (redisReply*)redisCommand(ctx, "LRANGE %s %d %d", key.c_str(), start, stop);
    if (!reply || reply->type != REDIS_REPLY_ARRAY) { if (reply) freeReplyObject(reply); return result; }
    for (size_t i = 0; i < reply->elements; ++i)
        if (reply->element[i]->str) result.push_back(reply->element[i]->str);
    freeReplyObject(reply);
    return result;
}

// ── Set ──

int64_t RedisClient::SAdd(const std::string& key, const std::string& member)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return -1;
    redisReply* reply = (redisReply*)redisCommand(ctx, "SADD %s %s", key.c_str(), member.c_str());
    if (!reply) return -1;
    int64_t val = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : -1;
    freeReplyObject(reply);
    return val;
}

int64_t RedisClient::SRem(const std::string& key, const std::string& member)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return -1;
    redisReply* reply = (redisReply*)redisCommand(ctx, "SREM %s %s", key.c_str(), member.c_str());
    if (!reply) return -1;
    int64_t val = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : -1;
    freeReplyObject(reply);
    return val;
}

bool RedisClient::SIsMember(const std::string& key, const std::string& member)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "SISMEMBER %s %s", key.c_str(), member.c_str());
    if (!reply) return false;
    bool ok = (reply->type == REDIS_REPLY_INTEGER && reply->integer == 1);
    freeReplyObject(reply);
    return ok;
}

int64_t RedisClient::SCard(const std::string& key)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return 0;
    redisReply* reply = (redisReply*)redisCommand(ctx, "SCARD %s", key.c_str());
    if (!reply) return 0;
    int64_t val = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : 0;
    freeReplyObject(reply);
    return val;
}

std::vector<std::string> RedisClient::SMembers(const std::string& key)
{
    std::vector<std::string> result;
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return result;
    redisReply* reply = (redisReply*)redisCommand(ctx, "SMEMBERS %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_ARRAY) { if (reply) freeReplyObject(reply); return result; }
    for (size_t i = 0; i < reply->elements; ++i)
        if (reply->element[i]->str) result.push_back(reply->element[i]->str);
    freeReplyObject(reply);
    return result;
}

// ── ZSet ──

bool RedisClient::ZAdd(const std::string& key, double score, const std::string& member)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "ZADD %s %f %s", key.c_str(), score, member.c_str());
    if (!reply) return false;
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

int64_t RedisClient::ZRem(const std::string& key, const std::string& member)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return -1;
    redisReply* reply = (redisReply*)redisCommand(ctx, "ZREM %s %s", key.c_str(), member.c_str());
    if (!reply) return -1;
    int64_t val = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : -1;
    freeReplyObject(reply);
    return val;
}

int64_t RedisClient::ZCard(const std::string& key)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return 0;
    redisReply* reply = (redisReply*)redisCommand(ctx, "ZCARD %s", key.c_str());
    if (!reply) return 0;
    int64_t val = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : 0;
    freeReplyObject(reply);
    return val;
}

int64_t RedisClient::ZCount(const std::string& key, double min, double max)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return 0;
    redisReply* reply = (redisReply*)redisCommand(ctx, "ZCOUNT %s %f %f", key.c_str(), min, max);
    if (!reply) return 0;
    int64_t val = (reply->type == REDIS_REPLY_INTEGER) ? reply->integer : 0;
    freeReplyObject(reply);
    return val;
}

bool RedisClient::ZRemRangeByScore(const std::string& key, double min, double max)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "ZREMRANGEBYSCORE %s %f %f", key.c_str(), min, max);
    if (!reply) return false;
    freeReplyObject(reply);
    return true;
}

bool RedisClient::ZRemRangeByRank(const std::string& key, int start, int stop)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "ZREMRANGEBYRANK %s %d %d", key.c_str(), start, stop);
    if (!reply) return false;
    freeReplyObject(reply);
    return true;
}

std::vector<std::string> RedisClient::ZRange(const std::string& key, int start, int stop, bool with_scores)
{
    std::vector<std::string> result;
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return result;
    const char* cmd = with_scores ? "ZRANGE %s %d %d WITHSCORES" : "ZRANGE %s %d %d";
    redisReply* reply = (redisReply*)redisCommand(ctx, cmd, key.c_str(), start, stop);
    if (!reply || reply->type != REDIS_REPLY_ARRAY) { if (reply) freeReplyObject(reply); return result; }
    for (size_t i = 0; i < reply->elements; ++i)
        if (reply->element[i]->str) result.push_back(reply->element[i]->str);
    freeReplyObject(reply);
    return result;
}

std::string RedisClient::Eval(const std::string& script,
                              const std::vector<std::string>& keys,
                              const std::vector<std::string>& args)
{
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return "";

    std::string numkeys = std::to_string(keys.size());
    int argc = 3 + keys.size() + args.size();
    std::vector<const char*> argv(argc);
    std::vector<size_t> argvlen(argc);

    int idx = 0;
    argv[idx] = "EVAL";         argvlen[idx] = 4;  idx++;
    argv[idx] = script.c_str(); argvlen[idx] = script.size(); idx++;
    argv[idx] = numkeys.c_str(); argvlen[idx] = numkeys.size(); idx++;
    for (const auto& k : keys) {
        argv[idx] = k.c_str();  argvlen[idx] = k.size(); idx++;
    }
    for (const auto& a : args) {
        argv[idx] = a.c_str();  argvlen[idx] = a.size(); idx++;
    }

    redisReply* reply = (redisReply*)redisCommandArgv(ctx, argc, argv.data(), argvlen.data());
    if (!reply || reply->type == REDIS_REPLY_ERROR)
    {
        if (reply) { LOG_ERROR("Redis EVAL error: %s", reply->str ? reply->str : "unknown"); freeReplyObject(reply); }
        return "";
    }
    std::string result;
    if (reply->type == REDIS_REPLY_STRING && reply->str)
        result = reply->str;
    else if (reply->type == REDIS_REPLY_INTEGER)
        result = std::to_string(reply->integer);
    else if (reply->type == REDIS_REPLY_STATUS && reply->str)
        result = reply->str;
    freeReplyObject(reply);
    return result;
}

RedisClient::RedisReplyPtr RedisClient::CommandArgv(const std::vector<std::string>& args)
{
    // 使用 null deleter 占位，出错时返回空指针
    auto deleter = [](redisReply* r) { if (r) freeReplyObject(r); };

    redisContext* ctx = getContext();
    if (!ctx || ctx->err || args.empty())
        return RedisReplyPtr(nullptr, deleter);

    std::vector<const char*> argv(args.size());
    std::vector<size_t> argvlen(args.size());
    for (size_t i = 0; i < args.size(); ++i)
    {
        argv[i] = args[i].c_str();
        argvlen[i] = args[i].size();
    }

    redisReply* reply = (redisReply*)redisCommandArgv(ctx, (int)args.size(), argv.data(), argvlen.data());
    if (!reply)
    {
        // 连接层错误：清空 thread_local 以便下次重建连接
        redisFree(t_context);
        t_context = nullptr;
        return RedisReplyPtr(nullptr, deleter);
    }
    if (reply->type == REDIS_REPLY_ERROR)
        LOG_ERROR("Redis CommandArgv error: %s", reply->str ? reply->str : "unknown");
    return RedisReplyPtr(reply, deleter);
}

std::vector<std::string> RedisClient::EvalArr(const std::string& script,
                                              const std::vector<std::string>& keys,
                                              const std::vector<std::string>& args)
{
    std::vector<std::string> out;
    redisContext* ctx = getContext();
    if (!ctx || ctx->err) return out;

    std::string numkeys = std::to_string(keys.size());
    int argc = 3 + keys.size() + args.size();
    std::vector<const char*> argv(argc);
    std::vector<size_t> argvlen(argc);
    int idx = 0;
    argv[idx] = "EVAL";          argvlen[idx] = 4; idx++;
    argv[idx] = script.c_str();  argvlen[idx] = script.size(); idx++;
    argv[idx] = numkeys.c_str(); argvlen[idx] = numkeys.size(); idx++;
    for (const auto& k : keys) { argv[idx] = k.c_str(); argvlen[idx] = k.size(); idx++; }
    for (const auto& a : args) { argv[idx] = a.c_str(); argvlen[idx] = a.size(); idx++; }

    redisReply* reply = (redisReply*)redisCommandArgv(ctx, argc, argv.data(), argvlen.data());
    if (!reply)
    {
        redisFree(t_context);
        t_context = nullptr;
        return out;
    }
    if (reply->type == REDIS_REPLY_ERROR)
    {
        LOG_ERROR("Redis EVAL error: %s", reply->str ? reply->str : "unknown");
        freeReplyObject(reply);
        return out;
    }
    if (reply->type == REDIS_REPLY_ARRAY)
    {
        for (size_t i = 0; i < reply->elements; ++i)
        {
            redisReply* e = reply->element[i];
            if (e->type == REDIS_REPLY_STRING && e->str)
                out.emplace_back(e->str, e->len);
            else if (e->type == REDIS_REPLY_INTEGER)
                out.push_back(std::to_string(e->integer));
            else
                out.emplace_back("");
        }
    }
    freeReplyObject(reply);
    return out;
}
