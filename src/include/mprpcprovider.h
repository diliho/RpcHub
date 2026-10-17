#pragma once
#include "google/protobuf/service.h"
#include <memory>
#include <muduo/net/TcpServer.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpConnection.h>
#include <unordered_map>
#include "registry/zk_registry.h"
#include "mprpccontroller.h"
#include "redisutil.h"
#include "ratelimiter.h"
#include "metricscollector.h"

class RpcProvider
{
public:
    struct ServiceInfo
    {
        google::protobuf::Service *m_service;
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor *> m_methodMap;
    };

    void NotifyService(google::protobuf::Service *service);
    void Run();
    
    void SendRpcResponse(const muduo::net::TcpConnectionPtr& conn,
                        google::protobuf::Message* response,
                        MprpcController* controller,
                        const std::string& trace_id = "");

    mprpc::ZKRegistry& getRegistry() { return m_registry; }
    muduo::net::EventLoop& getEventLoop() { return m_eventloop; }
    std::unordered_map<std::string, ServiceInfo>& getServiceMap() { return m_serviceMap; }
    std::vector<std::string>& getRegisteredZnodes() { return m_registered_znodes; }
    std::string getAddress() const { return m_ip + ":" + std::to_string(m_port); }
    MetricsCollector* getMetrics() { return m_metrics.get(); }

private:
    muduo::net::EventLoop m_eventloop;
    mprpc::ZKRegistry m_registry;
    std::string m_ip;
    uint16_t m_port;
    std::unique_ptr<muduo::net::TcpServer> m_server;

    std::unordered_map<std::string, ServiceInfo> m_serviceMap;
    std::vector<std::string> m_registered_znodes;

    std::shared_ptr<RedisClient> m_redis;
    std::unique_ptr<RateLimiter> m_rate_limiter;
    std::unique_ptr<MetricsCollector> m_metrics;

    void OnConnection(const muduo::net::TcpConnectionPtr &);
    void OnMessage(const muduo::net::TcpConnectionPtr &conn, muduo::net::Buffer *, muduo::Timestamp);
    void registerServicesToZK();
    void sendErrorResponse(const muduo::net::TcpConnectionPtr& conn, const std::string& err_msg);
};
