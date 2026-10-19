#include "mprpcprovider.h"
#include "mprpccontroller.h"
#include "rpcerror.h"
#include <string>
#include "mprpcapplication.h"
#include <functional>
#include <google/protobuf/descriptor.h>
#include "rpcheader.pb.h"
#include "prometheus/metrics_exporter.h"
#include <signal.h>
#include <vector>
#include <thread>
#include <map>

static RpcProvider* g_shutdown_provider = nullptr;

static void handleGracefulShutdown(int signum)
{
    if (!g_shutdown_provider) return;
    LOG_INFO("Received %s, shutting down gracefully...",
             signum == SIGTERM ? "SIGTERM" : "SIGINT");

    mprpc::ZKRegistry& reg = g_shutdown_provider->getRegistry();
    reg.GetRawClient().setOnReconnectCallback(nullptr);

    // Ephemeral nodes are auto-deleted on disconnect, but explicitly
    // delete them so the shutdown is visible to consumers immediately.
    if (reg.GetHandle() && reg.IsConnected())
    {
        for (auto &znode_path : g_shutdown_provider->getRegisteredZnodes())
        {
            zoo_delete(reg.GetHandle(), znode_path.c_str(), -1);
            LOG_INFO("Deleted ZK instance node: %s", znode_path.c_str());
        }
    }
    g_shutdown_provider->getEventLoop().quit();
    LOG_INFO("Event loop quit signaled");
}

void RpcProvider::NotifyService(google::protobuf::Service *service)
{
    ServiceInfo service_info;
    const google::protobuf::ServiceDescriptor *pserviceDesc = service->GetDescriptor();
    std::string service_name = pserviceDesc->name();
    int methodCnt = pserviceDesc->method_count();

    LOG_INFO("service_name:%s", service_name.c_str());

    for (int i = 0; i < methodCnt; ++i)
    {
        const google::protobuf::MethodDescriptor *pmethodDesc = pserviceDesc->method(i);
        std::string method_name = pmethodDesc->name();
        LOG_INFO("method_name:%s", method_name.c_str());
        service_info.m_methodMap.insert({method_name, pmethodDesc});
    }
    service_info.m_service = service;
    m_serviceMap.insert({service_name, service_info});
}

void RpcProvider::registerServicesToZK()
{
    for (auto &sp : m_serviceMap)
    {
        std::string service_path = "/" + sp.first;
        m_registry.CreateNode(service_path, "", 0);
        for (auto &mp : sp.second.m_methodMap)
        {
            std::string method_path = service_path + "/" + mp.first;
            m_registry.CreateNode(method_path, "", 0);

            std::string instance_prefix = method_path + "/inst_";
            std::string data_str = m_ip + ":" + std::to_string(m_port);
            std::string actual_path = m_registry.CreateNode(
                instance_prefix, data_str, ZOO_EPHEMERAL | ZOO_SEQUENCE);
            if (!actual_path.empty())
            {
                m_registered_znodes.push_back(actual_path);
                LOG_INFO("Registered instance znode: %s -> %s", actual_path.c_str(), data_str.c_str());
            }
        }
    }
    LOG_INFO("Services registered to ZK (%d instance znodes)", (int)m_registered_znodes.size());
}

void RpcProvider::Run()
{
    std::string str_ip = "rpcserverip";
    m_ip = MprpcApplication::GetConfig().Load(str_ip);
    std::string str_port = "rpcserverport";
    m_port = atoi(MprpcApplication::GetConfig().Load(str_port).c_str());
    muduo::net::InetAddress address(m_ip, m_port);

    m_server = std::make_unique<muduo::net::TcpServer>(&m_eventloop, address, "RpcProvider");
    m_server->setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
    m_server->setMessageCallback(std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
    std::string threads_str = MprpcApplication::GetConfig().Load("rpcproviderthreads");
    int io_threads = threads_str.empty() ? 0 : std::stoi(threads_str);
    if (io_threads <= 0) io_threads = std::thread::hardware_concurrency();
    if (io_threads < 1) io_threads = 1;
    m_server->setThreadNum(io_threads);
    LOG_INFO("RpcProvider: using %d IO threads (config=%s)", io_threads,
             threads_str.empty() ? "auto" : threads_str.c_str());

    m_registry.Start("", 0);
    registerServicesToZK();

    std::string redis_ip = MprpcApplication::GetConfig().Load("redisip");
    std::string redis_port = MprpcApplication::GetConfig().Load("redisport");
    if (!redis_ip.empty() && !redis_port.empty())
    {
        auto redis = std::make_shared<RedisClient>();
        if (redis->Connect(redis_ip, atoi(redis_port.c_str()),
                           MprpcApplication::GetConfig().Load("redispassword")))
        {
            m_redis = redis;
            LOG_INFO("RpcProvider: Redis connected at %s:%s", redis_ip.c_str(), redis_port.c_str());

            std::string enable_rate = MprpcApplication::GetConfig().Load("ratelimitenableprovider");
            if (enable_rate.empty() || enable_rate == "true")
            {
                int max_qps = std::stoi(
                    MprpcApplication::GetConfig().Load("ratelimitmaxqps").empty() ? "1000"
                    : MprpcApplication::GetConfig().Load("ratelimitmaxqps"));
                int window_sec = std::stoi(
                    MprpcApplication::GetConfig().Load("ratelimitwindowsec").empty() ? "1"
                    : MprpcApplication::GetConfig().Load("ratelimitwindowsec"));
                int algo = std::stoi(
                    MprpcApplication::GetConfig().Load("ratelimitalgorithm").empty() ? "3"
                    : MprpcApplication::GetConfig().Load("ratelimitalgorithm"));

                m_rate_limiter = std::make_unique<RateLimiter>(
                    m_redis, max_qps, window_sec,
                    static_cast<RateLimitAlgorithm>(algo));
                LOG_INFO("RateLimiter initialized: max_qps=%d, window=%ds", max_qps, window_sec);
            }

            std::string enable_metrics = MprpcApplication::GetConfig().Load("metricsenable");
            if (enable_metrics.empty() || enable_metrics == "true")
            {
                int ttl = 3600;
                std::string ttl_str = MprpcApplication::GetConfig().Load("metricsttl");
                if (!ttl_str.empty()) ttl = std::stoi(ttl_str);
                m_metrics = std::make_unique<MetricsCollector>(m_redis, ttl);
                LOG_INFO("MetricsCollector initialized (ttl=%d)", ttl);
            }

            // config 路径创建的实例同样通过原子指针发布
    if (m_rate_limiter)
        m_rate_limiter_ptr.store(m_rate_limiter.get(), std::memory_order_release);

    // ── 治理规则热下发：管控面改规则 → Redis 版本号 → 此处秒级生效 ──
            // 注：m_rate_limiter 可能因 config 关闭而未创建，回调内惰性创建——
            // 限流开关完全由下发规则驱动，config 仅决定初始状态
            m_ratelimit_enabled.store(enable_rate.empty() || enable_rate == "true");
            m_rule_watcher = std::make_unique<mprpc::GovernanceWatcher>(m_redis);
            m_rule_watcher->SetApplyCallback([this](const mprpc::GovernanceRules& rules) {
                m_ratelimit_enabled.store(rules.enable);
                if (rules.enable)
                {
                    std::lock_guard<std::mutex> lock(m_ratelimit_mtx);
                if (!m_rate_limiter)
                {
                    m_rate_limiter = std::make_unique<RateLimiter>(
                        m_redis, rules.max_qps, rules.window_sec,
                        static_cast<RateLimitAlgorithm>(rules.algorithm));
                    // 原子发布：IO 线程读裸指针，避免 unique_ptr 跨线程读的数据竞争
                    m_rate_limiter_ptr.store(m_rate_limiter.get(),
                                             std::memory_order_release);
                    LOG_INFO("RateLimiter lazily created by governance rule: qps=%d",
                             rules.max_qps);
                }
                    else
                    {
                        m_rate_limiter->UpdateRules({rules.max_qps, rules.window_sec,
                                                     static_cast<RateLimitAlgorithm>(rules.algorithm)});
                    }
                }
            });
            m_rule_watcher->Start();
        }
        else
        {
            LOG_ERROR("RpcProvider: Redis init failed, rate limit/metrics disabled");
        }
    }

    m_registry.AddReconnectCallback([this]() {
        LOG_INFO("ZK reconnected, re-registering services...");
        registerServicesToZK();
    });

    LOG_INFO("RpcProvider start service at ip:%s port:%d", m_ip.c_str(), m_port);

    // Graceful shutdown on SIGINT / SIGTERM
    {
        g_shutdown_provider = this;
        signal(SIGINT, handleGracefulShutdown);
        signal(SIGTERM, handleGracefulShutdown);
    }

    m_server->start();
    m_eventloop.loop();
}

void RpcProvider::OnConnection(const muduo::net::TcpConnectionPtr &conn)
{
    if (!conn->connected())
    {
        conn->shutdown();
    }
}

struct RpcResponseArgs {
    RpcProvider* provider;
    muduo::net::TcpConnectionPtr conn;
    google::protobuf::Message* response;
    MprpcController* controller;
    std::string service_name;
    std::string method_name;
    std::string trace_id;
    int64_t start_ms;
};

void OnRpcResponse(void* args) {
    RpcResponseArgs* responseArgs = static_cast<RpcResponseArgs*>(args);
    bool is_error = responseArgs->controller->Failed();
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    int64_t latency = now_ms - responseArgs->start_ms;

    responseArgs->provider->SendRpcResponse(
        responseArgs->conn,
        responseArgs->response,
        responseArgs->controller,
        responseArgs->trace_id
    );
    if (responseArgs->provider->getMetrics())
    {
        responseArgs->provider->getMetrics()->recordCall(
            responseArgs->service_name,
            responseArgs->method_name,
            latency,
            is_error,
            false);
    }

    // ── Prometheus exporter：counter/histogram/gauge 打点 ──
    std::map<std::string, std::string> labels = {
        {"service", responseArgs->service_name},
        {"method", responseArgs->method_name}};
    auto& exporter = mprpc::MetricsExporter::GetInstance();
    exporter.IncrCounter("mprpc_rpc_calls_total", labels);
    if (is_error)
        exporter.IncrCounter("mprpc_rpc_errors_total", labels);
    exporter.ObserveHistogram("mprpc_rpc_duration_seconds", labels,
                              static_cast<double>(latency) / 1000.0);

    delete responseArgs;
}

void RpcProvider::sendErrorResponse(const muduo::net::TcpConnectionPtr& conn, const std::string& err_msg)
{
    mprpc::RpcHeader rpcHeader;
    rpcHeader.set_service_name("");
    rpcHeader.set_method_name("");
    rpcHeader.set_args_size(0);
    rpcHeader.set_version(1);

    std::string rpc_header_str;
    rpcHeader.SerializeToString(&rpc_header_str);

    std::string send_str;
    uint32_t header_size = rpc_header_str.size();
    send_str.insert(0, std::string((char*)&header_size, 4));
    send_str += rpc_header_str;

    conn->send(send_str);
    conn->shutdown();

    LOG_ERROR("Sent error response to client: %s", err_msg.c_str());
}

void RpcProvider::OnMessage(const muduo::net::TcpConnectionPtr &conn,
                            muduo::net::Buffer *buffer,
                            muduo::Timestamp)
{
    std::string recv_buf = buffer->retrieveAllAsString();
    if (recv_buf.size() < 4) return;

    uint32_t header_size = 0;
    recv_buf.copy((char *)&header_size, 4, 0);

    if (4 + header_size > recv_buf.size())
    {
        sendErrorResponse(conn, "invalid rpc header size");
        return;
    }

    std::string rpc_header_str = recv_buf.substr(4, header_size);
    mprpc::RpcHeader rpcHeader;
    std::string service_name;
    std::string method_name;
    std::string trace_id;
    uint32_t args_size = 0;
    if (rpcHeader.ParseFromString(rpc_header_str))
    {
        service_name = rpcHeader.service_name();
        method_name = rpcHeader.method_name();
        args_size = rpcHeader.args_size();
        trace_id = rpcHeader.trace_id();
    }
    else
    {
        sendErrorResponse(conn, "parse rpc header error");
        return;
    }

    if (4 + header_size + args_size > recv_buf.size())
    {
        sendErrorResponse(conn, "invalid rpc args size");
        return;
    }

    std::string args_str = recv_buf.substr(4 + header_size, args_size);

    auto* limiter = m_rate_limiter_ptr.load(std::memory_order_acquire);
    if (limiter && m_ratelimit_enabled.load(std::memory_order_relaxed))
    {
        std::string client_ip = conn->peerAddress().toIp();
        if (!limiter->allow(service_name, method_name, client_ip))
        {
            LOG_WARN("Rate limit exceeded: %s/%s from %s",
                     service_name.c_str(), method_name.c_str(), client_ip.c_str());
            if (m_metrics)
                m_metrics->recordBlocked(service_name, method_name);
            {
                std::map<std::string, std::string> labels = {
                    {"service", service_name}, {"method", method_name}};
                mprpc::MetricsExporter::GetInstance().IncrCounter(
                    "mprpc_rpc_blocked_total", labels);
            }
            sendErrorResponse(conn, "rate limit exceeded, try again later");
            return;
        }
    }

    auto it = m_serviceMap.find(service_name);
    if (it == m_serviceMap.end())
    {
        LOG_ERROR("service_name:%s not find", service_name.c_str());
        sendErrorResponse(conn, "service " + service_name + " not found");
        return;
    }

    auto mit = it->second.m_methodMap.find(method_name);
    if (mit == it->second.m_methodMap.end())
    {
        LOG_ERROR("method_name:%s not find", method_name.c_str());
        sendErrorResponse(conn, "method " + method_name + " not found");
        return;
    }

    google::protobuf::Service* service = it->second.m_service;
    const google::protobuf::MethodDescriptor* method = mit->second;

    MprpcController* controller = new MprpcController();

    google::protobuf::Message* request = service->GetRequestPrototype(method).New();
    if (!request->ParseFromString(args_str))
    {
        LOG_ERROR("request parse error!");
        controller->SetFailed("request parse error");
        delete controller;
        delete request;
        sendErrorResponse(conn, "request parse error");
        return;
    }

    google::protobuf::Message* response = service->GetResponsePrototype(method).New();

    RpcResponseArgs* args = new RpcResponseArgs();
    args->provider = this;
    args->conn = conn;
    args->response = response;
    args->controller = controller;
    args->service_name = service_name;
    args->method_name = method_name;
    args->trace_id = trace_id;
    args->start_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    LOG_INFO("RPC dispatch %s/%s trace_id=%s",
             service_name.c_str(), method_name.c_str(), trace_id.c_str());

    google::protobuf::Closure* done = google::protobuf::NewPermanentCallback(
        OnRpcResponse,
        static_cast<void*>(args)
    );

    service->CallMethod(method, controller, request, response, done);

    delete request;
}

void RpcProvider::SendRpcResponse(const muduo::net::TcpConnectionPtr& conn,
                                  google::protobuf::Message* response,
                                  MprpcController* controller,
                                  const std::string& trace_id)
{
    std::string response_str;
    std::string send_rpc_str;
    bool is_error = controller->Failed();

    if (is_error) {
        LOG_ERROR("RPC call failed: %s", controller->ErrorText().c_str());
    } else {
        if (response->SerializeToString(&response_str))
        {
            mprpc::RpcHeader rpcHeader;
            rpcHeader.set_service_name("");
            rpcHeader.set_method_name("");
            rpcHeader.set_args_size(response_str.size());
            rpcHeader.set_version(1);
            rpcHeader.set_trace_id(trace_id);  // 回带，客户端可关联整次调用

            std::string rpc_header_str;
            if (rpcHeader.SerializeToString(&rpc_header_str)) {
                uint32_t header_size = rpc_header_str.size();
                send_rpc_str.insert(0, std::string((char*)&header_size, 4));
                send_rpc_str += rpc_header_str;
                send_rpc_str += response_str;
                conn->send(send_rpc_str);
            } else {
                LOG_ERROR("serialize rpc header error!");
            }
        }
        else
        {
            LOG_ERROR("serialize response error!");
        }
    }

    delete response;
    delete controller;
}
