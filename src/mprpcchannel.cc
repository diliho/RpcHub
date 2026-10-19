#include "mprpcchannel.h"
#include "rpcheader.pb.h"
#include "mprpcapplication.h"
#include "rpcerror.h"
#include "balance/load_balancer_factory.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <iostream>
#include "mprpccontroller.h"
#include "logger.h"
#include "rpcerror.h"
#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <set>
#include <mutex>

// 生成调用链路 ID：cxx- + 时间戳与随机数混合的 16 位 hex。
// 一次 CallMethod 生成一次，所有重试尝试共用，便于服务端日志串联。
static std::string generateTraceId()
{
    uint64_t now = static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    uint64_t rnd = static_cast<uint64_t>(rand());
    uint64_t mixed = now ^ (rnd << 32) ^ rnd;
    char buf[32];
    snprintf(buf, sizeof(buf), "cxx-%016llx", static_cast<unsigned long long>(mixed));
    return buf;
}

// ─── ZK Watcher ────────────────────────────────────────────────

// 存活 channel 注册表：ZK watch 无法反注册（一次性触发后由服务端语义延续），
// channel 析构后 watcher 仍可能 fire——回调只允许触碰表内存活对象，防悬垂。
static std::mutex s_channels_mtx;
static std::set<MprpcChannel*> s_live_channels;

void MprpcChannel::serviceWatcher(zhandle_t* zh, int type, int state,
                                   const char* path, void* watcherCtx)
{
    (void)watcherCtx;
    // Handle all ZK events that indicate service topology change
    if (type != ZOO_CREATED_EVENT && type != ZOO_DELETED_EVENT
        && type != ZOO_CHANGED_EVENT && type != ZOO_CHILD_EVENT) return;

    std::string path_str = path ? path : "";
    std::lock_guard<std::mutex> lock(s_channels_mtx);
    for (MprpcChannel* ch : s_live_channels)
    {
        LOG_INFO("ZK watcher triggered: type=%d, path=%s", type, path_str.c_str());
        ch->invalidateCache(path_str);
    }
}

void MprpcChannel::invalidateCache(const std::string& method_path)
{
    std::lock_guard<std::mutex> lock(m_cache_mtx);
    m_cache_timestamps.erase(method_path);
    m_watcher_registered[method_path] = false;
    LOG_INFO("Cache invalidated for %s (next call will re-query ZK)", method_path.c_str());
}

void MprpcChannel::registerWatcher(const std::string& method_path)
{
    {
        std::lock_guard<std::mutex> lock(m_cache_mtx);
        if (m_watcher_registered[method_path]) return;
    }

    zhandle_t* zh = m_registry->GetHandle();
    if (!zh || !m_registry->IsConnected()) return;

    // Register child watcher (watches child add/remove under method_path)
    struct String_vector str_vec;
    int rc = zoo_wget_children(zh, method_path.c_str(), serviceWatcher, this, &str_vec);
    if (rc == ZOK)
    {
        deallocate_String_vector(&str_vec);
        std::lock_guard<std::mutex> lock(m_cache_mtx);
        m_watcher_registered[method_path] = true;
        LOG_INFO("ZK child watcher registered for %s", method_path.c_str());
        return;
    }

    // Node doesn't exist — register existence watcher (watches creation)
    if (rc == ZNONODE)
    {
        struct Stat stat;
        rc = zoo_wexists(zh, method_path.c_str(), serviceWatcher, this, &stat);
        if (rc == ZOK)
        {
            std::lock_guard<std::mutex> lock(m_cache_mtx);
            m_watcher_registered[method_path] = true;
            LOG_INFO("ZK existence watcher registered for %s", method_path.c_str());
            return;
        }
    }

    LOG_WARN("Failed to register ZK watcher for %s, rc=%d",
             method_path.c_str(), rc);
}

// ─── Service Discovery ─────────────────────────────────────────

std::string MprpcChannel::discoverService(const std::string& service_name,
                                           const std::string& method_name)
{
    std::string method_path = "/" + service_name + "/" + method_name;

    // ── Direct address mode: bypass ZK entirely ──
    if (!m_direct_addr.empty())
    {
        m_last_provider = m_direct_addr;
        return m_direct_addr;
    }

    // ── Check cache first (avoids ZK query on every call) ──
    {
        std::lock_guard<std::mutex> lock(m_cache_mtx);
        auto ts_it = m_cache_timestamps.find(method_path);
        if (ts_it != m_cache_timestamps.end())
        {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - ts_it->second).count();
            if (elapsed < CACHE_TTL_SEC)
            {
                auto inst_it = m_inst_cache.find(method_path);
                if (inst_it != m_inst_cache.end() && !inst_it->second.empty())
                {
                    std::lock_guard<std::mutex> lb_lock(m_lb_mtx);
                    std::string addr = m_balancer->Select(inst_it->second, service_name, method_name);
                    if (!addr.empty())
                    {
                        m_last_provider = addr;
                        return addr;
                    }
                }
            }
        }
    }

    // ── Try ZK: list children for multi-provider support ──
    if (m_zk_available)
    {
        for (int i = 0; i < MAX_RETRY; ++i)
        {
            std::vector<std::string> children = m_registry->GetChildren(method_path.c_str());
            if (!children.empty())
            {
                // Read all children data to build full instance list
                std::vector<mprpc::ServiceInstance> instances;
                instances.reserve(children.size());
                for (const auto& child : children)
                {
                    std::string child_path = method_path + "/" + child;
                    std::string data = m_registry->GetData(child_path);
                    if (!data.empty())
                    {
                        mprpc::ServiceInstance inst;
                        size_t colon = data.find(':');
                        if (colon != std::string::npos)
                        {
                            inst.host = data.substr(0, colon);
                            inst.port = static_cast<uint16_t>(std::stoi(data.substr(colon + 1)));
                            inst.weight = 1;
                            instances.push_back(inst);
                        }
                    }
                }

                if (!instances.empty())
                {
                    std::string addr;
                    {
                        std::lock_guard<std::mutex> lb_lock(m_lb_mtx);
                        addr = m_balancer->Select(instances, service_name, method_name);
                    }

                    if (!addr.empty())
                    {
                        {
                            std::lock_guard<std::mutex> lock(m_cache_mtx);
                            m_inst_cache[method_path] = std::move(instances);
                            m_cache_timestamps[method_path] = std::chrono::steady_clock::now();
                        }

                        // Register ZK child watcher for real-time service awareness
                        registerWatcher(method_path);

                        m_last_provider = addr;
                        return addr;
                    }
                }
            }

            // Fallback: try direct GetData for backward compatibility with old single-writer registration
            std::string addr = m_registry->GetData(method_path);
            if (!addr.empty())
            {
                // Convert single address to ServiceInstance for consistent load balancing
                mprpc::ServiceInstance inst;
                size_t colon = addr.find(':');
                if (colon != std::string::npos)
                {
                    inst.host = addr.substr(0, colon);
                    inst.port = static_cast<uint16_t>(std::stoi(addr.substr(colon + 1)));
                    inst.weight = 1;
                    std::vector<mprpc::ServiceInstance> single_inst = {inst};
                    {
                        std::lock_guard<std::mutex> lock(m_cache_mtx);
                        m_inst_cache[method_path] = single_inst;
                        m_cache_timestamps[method_path] = std::chrono::steady_clock::now();
                    }
                    registerWatcher(method_path);
                    m_last_provider = addr;
                    return addr;
                }
            }

            if (i < MAX_RETRY - 1)
            {
                int backoff = (1000 << i) + (rand() % 500);
                LOG_WARN("ZK discovery retry %d/%d in %dms for %s", i + 1, MAX_RETRY, backoff, method_path.c_str());
                usleep(backoff * 1000);
            }
        }
        m_zk_available = false;
    }

    // ── Try to register existence watcher (in case node disappeared) ──
    registerWatcher(method_path);

    // ── Degrade: use cached instance ──
    {
        std::lock_guard<std::mutex> lock(m_cache_mtx);
        auto it = m_inst_cache.find(method_path);
        if (it != m_inst_cache.end() && !it->second.empty())
        {
            std::lock_guard<std::mutex> lb_lock(m_lb_mtx);
            std::string addr = m_balancer->Select(it->second, service_name, method_name);
            if (!addr.empty())
            {
                LOG_WARN("ZK unavailable, using cached instances for %s", method_path.c_str());
                m_last_provider = addr;
                return addr;
            }
        }
    }

    return "";
}

// ─── Parse host_data and fill sockaddr ─────────────────────────

void MprpcChannel::parseAndConnect(const std::string& host_data,
                                    struct sockaddr_in& server_addr,
                                    std::string& ip, uint16_t& port)
{
    int idx = host_data.find(":");
    if (idx == -1) return;
    ip = host_data.substr(0, idx);
    port = atoi(host_data.substr(idx + 1).c_str());
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(ip.c_str());
}

// ─── Connect with Timeout ──────────────────────────────────────

bool MprpcChannel::connectWithTimeout(int clientfd, const struct sockaddr_in& addr, int timeout_sec)
{
    struct timeval tv;
    tv.tv_sec = timeout_sec;
    tv.tv_usec = 0;
    setsockopt(clientfd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    if (connect(clientfd, (struct sockaddr*)&addr, sizeof(addr)) == -1)
    {
        LOG_ERROR("connect timeout (%ds), errno=%d", timeout_sec, errno);
        return false;
    }
    return true;
}

// ─── Constructor ───────────────────────────────────────────────

MprpcChannel::MprpcChannel() : m_zk_available(true)
{
    { std::lock_guard<std::mutex> lock(s_channels_mtx); s_live_channels.insert(this); }
    m_registry = std::make_shared<mprpc::ZKRegistry>(MprpcApplication::GetZKClient());
    if (m_registry->IsConnected())
    {
        LOG_INFO("MprpcChannel: ZKRegistry initialized (shared ZKClient)");

        m_registry->AddReconnectCallback([this]() {
            LOG_INFO("ZK reconnected: clearing watcher state and cache");
            std::lock_guard<std::mutex> lock(m_cache_mtx);
            for (auto& kv : m_watcher_registered)
                kv.second = false;
            m_cache_timestamps.clear();
        });
    }
    else
    {
        m_zk_available = false;
        LOG_ERROR("MprpcChannel: ZK init failed, will use cached addresses");
    }

    // ── Initialize load balancer ──
    {
        MprpcConfig& config = MprpcApplication::GetConfig();
        std::string lb_algo = config.Load("loadbalanceralgorithm");
        if (lb_algo.empty()) lb_algo = "round_robin";
        m_balancer = mprpc::LoadBalancerFactory::Create(lb_algo);
        LOG_INFO("MprpcChannel: Load balancer algorithm=%s", lb_algo.c_str());
    }

    MprpcConfig& config = MprpcApplication::GetConfig();
    std::string redis_ip = config.Load("redisip");
    std::string redis_port = config.Load("redisport");
    if (!redis_ip.empty() && !redis_port.empty())
    {
        auto redis = std::make_shared<RedisClient>();
        if (redis->Connect(redis_ip, atoi(redis_port.c_str()),
                           config.Load("redispassword")))
        {
            m_redis = redis;
            LOG_INFO("MprpcChannel: Redis connected at %s:%s", redis_ip.c_str(), redis_port.c_str());

            std::string enable_metrics = config.Load("metricsenable");
            if (enable_metrics.empty() || enable_metrics == "true")
            {
                int ttl = 3600;
                std::string ttl_str = config.Load("metricsttl");
                if (!ttl_str.empty()) ttl = std::stoi(ttl_str);
                m_metrics = std::make_unique<MetricsCollector>(m_redis, ttl);
                LOG_INFO("MprpcChannel: MetricsCollector initialized (ttl=%d)", ttl);
            }

            std::string enable_consumer = config.Load("ratelimitenableconsumer");
            if (enable_consumer == "true")
            {
                int max_qps = std::stoi(
                    config.Load("ratelimitmaxqps").empty() ? "1000"
                    : config.Load("ratelimitmaxqps"));
                int window_sec = std::stoi(
                    config.Load("ratelimitwindowsec").empty() ? "1"
                    : config.Load("ratelimitwindowsec"));
                int algo = std::stoi(
                    config.Load("ratelimitalgorithm").empty() ? "3"
                    : config.Load("ratelimitalgorithm"));

                m_client_rate_limiter = std::make_unique<RateLimiter>(
                    m_redis, max_qps, window_sec,
                    static_cast<RateLimitAlgorithm>(algo));
                LOG_INFO("MprpcChannel: Client rate limiter initialized");
            }
        }
        else
        {
            LOG_ERROR("MprpcChannel: Redis init failed, metrics disabled");
        }
    }
}

// ─── Destructor ────────────────────────────────────────────────

MprpcChannel::~MprpcChannel()
{
    { std::lock_guard<std::mutex> lock(s_channels_mtx); s_live_channels.erase(this); }
    // Connection pools are automatically cleaned up by unique_ptr destructors
}

// ─── CallMethod ────────────────────────────────────────────────

void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
    google::protobuf::RpcController* controller,
    const google::protobuf::Message* request,
    google::protobuf::Message* response,
    google::protobuf::Closure* done)
{
    auto call_start = std::chrono::steady_clock::now();
    constexpr int64_t TOTAL_TIMEOUT_MS = 8000;

    const google::protobuf::ServiceDescriptor* sd = method->service();
    std::string service_name = sd->name();
    std::string method_name = method->name();
    std::string method_path = "/" + service_name + "/" + method_name;

    // ── Serialize request (done once, reused across retries) ──
    uint32_t args_size = 0;
    std::string args_str;
    if (!request->SerializeToString(&args_str))
    {
        auto ec = RpcErrorUtil::createFrameError(FrameErrorCode::SERIALIZE_FAILED, 0, "MPRPC");
        static_cast<MprpcController*>(controller)->SetFailed(RpcError(ec, "serialize request error!"));
        return;
    }
    args_size = args_str.size();

    mprpc::RpcHeader rpcHeader;
    rpcHeader.set_service_name(service_name);
    rpcHeader.set_method_name(method_name);
    rpcHeader.set_args_size(args_size);
    rpcHeader.set_version(1);  // 协议版本1
    std::string trace_id = generateTraceId();
    rpcHeader.set_trace_id(trace_id);
    LOG_INFO("RPC call %s trace_id=%s", method_path.c_str(), trace_id.c_str());

    uint32_t header_size = 0;
    std::string rpc_header_str;
    if (!rpcHeader.SerializeToString(&rpc_header_str))
    {
        auto ec = RpcErrorUtil::createFrameError(FrameErrorCode::SERIALIZE_FAILED, 0, "MPRPC");
        static_cast<MprpcController*>(controller)->SetFailed(RpcError(ec, "serialize rpc header error!"));
        return;
    }
    header_size = rpc_header_str.size();

    std::string send_rpc_str;
    send_rpc_str.insert(0, std::string((char*)&header_size, 4));
    send_rpc_str += rpc_header_str;
    send_rpc_str += args_str;

    // ── Consumer-side rate limiting ──
    if (m_client_rate_limiter)
    {
        if (!m_client_rate_limiter->allow(service_name, method_name, ""))
        {
            std::string err = "client rate limit exceeded for " + method_path;
            LOG_ERROR("%s", err.c_str());
            static_cast<MprpcController*>(controller)->SetFailed(err);

            if (m_metrics)
                m_metrics->recordBlocked(service_name, method_name);
            return;
        }
    }

    // ── Retry loop ──
    std::string last_error;
    bool is_timeout = false;

    LOG_INFO("CallMethod: starting retry loop for %s", method_path.c_str());

    for (int attempt = 0; attempt < MAX_RETRY; ++attempt)
    {
        // Check overall timeout
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - call_start).count();
        if (elapsed > TOTAL_TIMEOUT_MS)
        {
            last_error = "RPC call timeout (overall)";
            is_timeout = true;
            break;
        }

        // ── Check method-level circuit breaker ──
        {
            std::lock_guard<std::mutex> lock(m_cb_mtx);
            auto& cb = m_method_breakers[method_path];
            if (!cb.AllowRequest())
            {
                last_error = "circuit breaker open for " + method_path;
                continue;
            }
        }

        // ── Discover service ──
        std::string host_data = discoverService(service_name, method_name);
        if (host_data.empty())
        {
            last_error = "service discovery failed for " + method_path;
            std::lock_guard<std::mutex> lock(m_cb_mtx);
            m_method_breakers[method_path].RecordFailure();
            continue;
        }

        // ── Parse host_data ──
        struct sockaddr_in server_addr;
        std::string ip;
        uint16_t port = 0;
        parseAndConnect(host_data, server_addr, ip, port);
        if (ip.empty() || port == 0)
        {
            last_error = method_path + " address invalid: " + host_data;
            continue;
        }

        // ── Get or create connection pool ──
        mprpc::ConnectionPool* pool = nullptr;
        {
            std::lock_guard<std::mutex> lock(m_pool_mtx);
            auto it = m_conn_pools.find(host_data);
            if (it == m_conn_pools.end())
            {
                auto inserted = m_conn_pools.emplace(host_data,
                    std::make_unique<mprpc::ConnectionPool>(ip, port, 8, 1000));
                pool = inserted.first->second.get();
            }
            else
            {
                pool = it->second.get();
            }
        }

        int clientfd = pool->Acquire();
        if (clientfd < 0)
        {
            char errtxt[512];
            snprintf(errtxt, sizeof(errtxt), "connect to %s:%d failed! errno:%d", ip.c_str(), port, errno);
            last_error = errtxt;
            {
                std::lock_guard<std::mutex> lock(m_cb_mtx);
                m_method_breakers[method_path].RecordFailure();
            }
            continue;
        }

        // Set receive timeout
        struct timeval tv;
        tv.tv_sec = 5;
        tv.tv_usec = 0;
        setsockopt(clientfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        if (send(clientfd, send_rpc_str.c_str(), send_rpc_str.size(), MSG_NOSIGNAL) == -1)
        {
            pool->Remove(clientfd);
            char errtxt[512];
            snprintf(errtxt, sizeof(errtxt), "send error! errno:%d", errno);
            auto ec = RpcErrorUtil::createFrameError(FrameErrorCode::MESSAGE_SEND_FAILED, errno, "MPRPC");
            static_cast<MprpcController*>(controller)->SetFailed(RpcError(ec, errtxt));
            {
                std::lock_guard<std::mutex> lock(m_cb_mtx);
                m_method_breakers[method_path].RecordFailure();
            }
            continue;
        }

        // ── Receive response (timeout already set before send) ──
        // Step 1: Read 4-byte header length
        uint32_t response_header_size = 0;
        {
            char len_buf[4] = {0};
            int total_read = 0;
            while (total_read < 4)
            {
                int n = recv(clientfd, len_buf + total_read, 4 - total_read, 0);
                if (n <= 0)
                {
                    pool->Remove(clientfd);
                    if (n == 0) last_error = "connection closed by server";
                    else if (errno == EAGAIN || errno == EWOULDBLOCK) { last_error = "RPC recv header timeout (5s)"; is_timeout = true; }
                    else { char errtxt[512]; snprintf(errtxt, sizeof(errtxt), "recv header error! errno:%d", errno);
                           auto ec = RpcErrorUtil::createFrameError(FrameErrorCode::MESSAGE_RECV_FAILED, errno, "MPRPC");
                           static_cast<MprpcController*>(controller)->SetFailed(RpcError(ec, errtxt)); }
                    {
                        std::lock_guard<std::mutex> lock(m_cb_mtx);
                        m_method_breakers[method_path].RecordFailure();
                    }
                    continue;
                }
                total_read += n;
            }
            memcpy(&response_header_size, len_buf, 4);
        }

        // Step 2: Validate header size
        if (response_header_size > 1024 * 1024)
        {
            pool->Remove(clientfd);
            last_error = "invalid response header size: too large";
            continue;
        }

        // Step 3: Read header bytes
        std::string response_rpc_header_str(response_header_size, '\0');
        {
            int total_read = 0;
            while (total_read < static_cast<int>(response_header_size))
            {
                int n = recv(clientfd, &response_rpc_header_str[total_read], 
                            response_header_size - total_read, 0);
                if (n <= 0)
                {
                    pool->Remove(clientfd);
                    if (n == 0) last_error = "connection closed by server";
                    else if (errno == EAGAIN || errno == EWOULDBLOCK) { last_error = "RPC recv header body timeout"; is_timeout = true; }
                    else { char errtxt[512]; snprintf(errtxt, sizeof(errtxt), "recv header body error! errno:%d", errno);
                           auto ec = RpcErrorUtil::createFrameError(FrameErrorCode::MESSAGE_RECV_FAILED, errno, "MPRPC");
                           static_cast<MprpcController*>(controller)->SetFailed(RpcError(ec, errtxt)); }
                    {
                        std::lock_guard<std::mutex> lock(m_cb_mtx);
                        m_method_breakers[method_path].RecordFailure();
                    }
                    continue;
                }
                total_read += n;
            }
        }

        // Step 4: Parse header
        mprpc::RpcHeader response_rpcHeader;
        if (!response_rpcHeader.ParseFromString(response_rpc_header_str))
        {
            pool->Remove(clientfd);
            last_error = "parse response header error";
            continue;
        }

        uint32_t response_args_size = response_rpcHeader.args_size();
        if (response_args_size == 0)
        {
            pool->Remove(clientfd);
            last_error = "server error: empty args";
            continue;
        }

        LOG_INFO("RPC response %s trace_id=%s", method_path.c_str(),
                 response_rpcHeader.trace_id().c_str());

        // Step 5: Read args bytes (dynamic allocation)
        std::string response_str(response_args_size, '\0');
        {
            int total_read = 0;
            while (total_read < static_cast<int>(response_args_size))
            {
                int n = recv(clientfd, &response_str[total_read], 
                            response_args_size - total_read, 0);
                if (n <= 0)
                {
                    pool->Remove(clientfd);
                    if (n == 0) last_error = "connection closed by server";
                    else if (errno == EAGAIN || errno == EWOULDBLOCK) { last_error = "RPC recv args timeout"; is_timeout = true; }
                    else { char errtxt[512]; snprintf(errtxt, sizeof(errtxt), "recv args error! errno:%d", errno);
                           auto ec = RpcErrorUtil::createFrameError(FrameErrorCode::MESSAGE_RECV_FAILED, errno, "MPRPC");
                           static_cast<MprpcController*>(controller)->SetFailed(RpcError(ec, errtxt)); }
                    {
                        std::lock_guard<std::mutex> lock(m_cb_mtx);
                        m_method_breakers[method_path].RecordFailure();
                    }
                    continue;
                }
                total_read += n;
            }
        }

        // Step 6: Parse response
        if (!response->ParseFromString(response_str))
        {
            pool->Remove(clientfd);
            auto ec = RpcErrorUtil::createFrameError(FrameErrorCode::DESERIALIZE_FAILED, 0, "MPRPC");
            static_cast<MprpcController*>(controller)->SetFailed(RpcError(ec, "parse response error"));
            return;
        }

        // ── Success (release FD back to pool for reuse) ──
        pool->Release(clientfd);
        {
            std::lock_guard<std::mutex> lock(m_cb_mtx);
            m_method_breakers[method_path].RecordSuccess();
        }
        m_zk_available = true;

        if (m_metrics)
        {
            auto end = std::chrono::steady_clock::now();
            int64_t latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                end - call_start).count();
            m_metrics->recordCall(service_name, method_name, latency, false, false);
        }
        return;
    }

    // All retries exhausted
    if (!last_error.empty())
    {
        static_cast<MprpcController*>(controller)->SetFailed(last_error);

        if (m_metrics)
        {
            auto end = std::chrono::steady_clock::now();
            int64_t latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                end - call_start).count();
            m_metrics->recordCall(service_name, method_name, latency, true, is_timeout);
        }
    }
}
