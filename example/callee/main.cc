/*
RPC服务提供者主入口
同时提供 userservice、friendservice 和 taskservice（异步任务引擎）
*/
#include <iostream>
#include <memory>
#include "mprpcapplication.h"
#include "mprpcprovider.h"
#include "user.pb.h"
#include "friend.pb.h"
#include "task.pb.h"
#include "userservice.h"
#include "friendservice.h"
#include "taskservice.h"
#include "redisutil.h"
#include "zookeeperutil.h"
#include "prometheus/metrics_exporter.h"

int main(int argc, char **argv)
{
    MprpcApplication::Init(argc, argv);

    // Start Prometheus metrics exporter
    std::string metrics_port_str = MprpcApplication::GetConfig().Load("metricsexporterport");
    if (!metrics_port_str.empty()) {
        int metrics_port = std::stoi(metrics_port_str);
        mprpc::MetricsExporter::GetInstance().Start(metrics_port);
        std::cout << "Metrics exporter started on port " << metrics_port << std::endl;
    }

    // ── TaskHub 引擎装配：独立 Redis/ZK 实例（线程安全，不与框架内部连接共享）──
    std::string redis_ip = MprpcApplication::GetConfig().Load("redisip");
    std::string redis_port = MprpcApplication::GetConfig().Load("redisport");
    static RedisClient task_redis;
    if (!redis_ip.empty() && !redis_port.empty() &&
        task_redis.Connect(redis_ip, std::stoi(redis_port)))
    {
        static ZKClient task_zk;
        bool zk_ok = task_zk.Start();

        std::string node_id = MprpcApplication::GetConfig().Load("rpcserverip") +
                              ":" + MprpcApplication::GetConfig().Load("rpcserverport");
        int worker_threads = 4;
        std::string threads_conf = MprpcApplication::GetConfig().Load("taskworkerthreads");
        if (!threads_conf.empty()) worker_threads = std::stoi(threads_conf);

        static TaskService task_service(task_redis, task_zk, node_id, worker_threads);
        std::cout << "TaskHub engine started: node=" << node_id
                  << " workers=" << worker_threads
                  << " coordinator=" << (zk_ok ? "electing" : "zk-unavailable")
                  << std::endl;

        RpcProvider provider;
        provider.NotifyService(new UserService());
        provider.NotifyService(new FriendService());
        provider.NotifyService(&task_service);
        provider.Run();
        return 0;
    }

    // Redis 不可用时降级为纯 RPC 服务（任务引擎不可用）
    std::cout << "[WARN] Redis unavailable, starting RPC without TaskHub engine" << std::endl;
    RpcProvider provider;
    provider.NotifyService(new UserService());
    provider.NotifyService(new FriendService());
    provider.Run();
    return 0;
}
