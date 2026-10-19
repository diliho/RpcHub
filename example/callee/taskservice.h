#ifndef TASKSERVICE_H
#define TASKSERVICE_H

#include <memory>
#include <string>
#include "task.pb.h"
#include "mprpcprovider.h"
#include "redisutil.h"
#include "zookeeperutil.h"
#include "task/task_queue.h"
#include "task/task_worker.h"
#include "task/task_coordinator.h"

// 异步任务执行引擎的 RPC 门面 + 引擎装配。
// provider 进程内嵌三层角色：接入层（本类 RPC 方法）、执行层（TaskWorkerPool）、
// 接管调度层（TaskCoordinator，ZK 选主单实例）。
// redis/zk 由外部装配（独立实例，避免与框架内部连接耦合）。
class TaskService : public fixbug::TaskServiceRpc
{
public:
    TaskService(RedisClient& redis, ZKClient& zk,
                const std::string& node_id, int worker_threads);
    ~TaskService() override;

    void SubmitTask(::google::protobuf::RpcController* controller,
                    const ::fixbug::SubmitTaskRequest* request,
                    ::fixbug::SubmitTaskResponse* response,
                    ::google::protobuf::Closure* done) override;

    void QueryTask(::google::protobuf::RpcController* controller,
                   const ::fixbug::QueryTaskRequest* request,
                   ::fixbug::QueryTaskResponse* response,
                   ::google::protobuf::Closure* done) override;

    void CancelTask(::google::protobuf::RpcController* controller,
                    const ::fixbug::CancelTaskRequest* request,
                    ::fixbug::CancelTaskResponse* response,
                    ::google::protobuf::Closure* done) override;

private:
    mprpc::TaskMeta ToMeta(const fixbug::TaskSpec& spec, int batch_index);

    RedisClient& m_redis;
    std::unique_ptr<mprpc::TaskQueue> m_queue;
    std::unique_ptr<mprpc::TaskWorkerPool> m_workers;
    std::unique_ptr<mprpc::TaskCoordinator> m_coordinator;
    std::string m_node_id;
};

#endif // TASKSERVICE_H
