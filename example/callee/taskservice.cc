#include "taskservice.h"
#include "mprpccontroller.h"
#include "logger.h"
#include <algorithm>
#include <map>

using namespace fixbug;
using mprpc::TaskMeta;
using mprpc::GenTaskId;
using mprpc::NowMs;

TaskService::TaskService(RedisClient& redis, ZKClient& zk,
                         const std::string& node_id, int worker_threads)
    : m_redis(redis), m_node_id(node_id)
{
    m_queue = std::make_unique<mprpc::TaskQueue>(m_redis);
    m_queue->EnsureGroup();

    m_workers = std::make_unique<mprpc::TaskWorkerPool>(
        m_redis, *m_queue, node_id, worker_threads);
    m_workers->RegisterExecutor("0", mprpc::ExecSleep);
    m_workers->RegisterExecutor("1", mprpc::ExecCompute);
    m_workers->RegisterExecutor("2", mprpc::ExecFail);
    m_workers->Start();

    m_coordinator = std::make_unique<mprpc::TaskCoordinator>(zk, *m_queue, node_id);
    m_coordinator->Start();

    LOG_INFO("TaskService initialized: node=%s workers=%d",
             node_id.c_str(), worker_threads);
}

TaskService::~TaskService()
{
    if (m_coordinator) m_coordinator->Stop();
    if (m_workers) m_workers->Stop();
}

TaskMeta TaskService::ToMeta(const TaskSpec& spec, int batch_index)
{
    TaskMeta m;
    m.task_id = GenTaskId();
    m.type = std::to_string(static_cast<int>(spec.type()));
    m.payload = spec.payload();
    m.priority = std::to_string(std::clamp(spec.priority(), 0, 1));
    int timeout = spec.timeout_ms() > 0 ? std::min(spec.timeout_ms(), 10000) : 5000;
    m.timeout_ms = std::to_string(timeout);
    m.max_retry = std::to_string(std::clamp(spec.max_retry() >= 0 ? spec.max_retry() : 3, 0, 10));
    // 批量时幂等键带序号；未提供幂等键则生成独立键（该任务无幂等语义）
    m.idempotency_key = spec.idempotency_key().empty()
        ? "auto-" + m.task_id
        : spec.idempotency_key() + (spec.batch_count() > 1
              ? "-" + std::to_string(batch_index) : "");
    m.submitted_at = std::to_string(NowMs());
    return m;
}

void TaskService::SubmitTask(::google::protobuf::RpcController* controller,
                             const SubmitTaskRequest* request,
                             SubmitTaskResponse* response,
                             ::google::protobuf::Closure* done)
{
    (void)controller;
    int64_t begin = NowMs();
    const TaskSpec& spec = request->spec();
    int batch = std::clamp(spec.batch_count() <= 0 ? 1 : spec.batch_count(), 1, 1000);

    for (int i = 0; i < batch; ++i)
    {
        TaskMeta meta = ToMeta(spec, i);
        auto r = m_queue->Submit(meta);
        if (!r.ok)
        {
            auto error = RpcErrorUtil::createSystemError(
                SystemErrorCode::SYSTEM_RESOURCE_LIMIT, 0, "MPRPC");
            RpcError error_obj(error, "redis unavailable, task not accepted");
            static_cast<MprpcController*>(controller)->SetFailed(error_obj);
            response->mutable_result()->set_errorcode(
                static_cast<int>(SystemErrorCode::SYSTEM_RESOURCE_LIMIT));
            response->mutable_result()->set_errormsg("redis unavailable");
            done->Run();
            return;
        }
        if (batch == 1)
        {
            response->set_task_id(r.task_id);
            response->set_duplicate(r.duplicate);
        }
        response->add_task_ids(r.task_id);
    }
    response->mutable_result()->set_errorcode(0);

    auto& exporter = mprpc::MetricsExporter::GetInstance();
    std::map<std::string, std::string> labels = {
        {"type", std::to_string(static_cast<int>(spec.type()))}};
    exporter.IncrCounter("task_submitted_total", labels,
                         static_cast<double>(batch));
    exporter.ObserveHistogram("task_submit_seconds", labels,
                              (NowMs() - begin) / 1000.0);
    done->Run();
}

void TaskService::QueryTask(::google::protobuf::RpcController* controller,
                            const QueryTaskRequest* request,
                            QueryTaskResponse* response,
                            ::google::protobuf::Closure* done)
{
    (void)controller;
    std::string task_id = request->task_id();
    auto meta = m_queue->Load(task_id);
    if (!meta)
    {
        response->mutable_result()->set_errorcode(
            static_cast<int>(BusinessErrorCode::RESOURCE_NOT_FOUND));
        response->mutable_result()->set_errormsg("task not found: " + task_id);
        done->Run();
        return;
    }
    int state_code = 0;
    try { state_code = std::stoi(meta->state); } catch (...) {}
    response->mutable_result()->set_errorcode(0);
    response->set_task_id(meta->task_id);
    response->set_state(static_cast<TaskState>(state_code));
    response->set_retries(std::atoi(meta->retries.c_str()));
    response->set_worker(meta->worker);
    response->set_result_payload(meta->result_payload);
    response->set_fail_reason(meta->fail_reason);
    int64_t submitted = 0, finished = 0;
    try { submitted = std::stoll(meta->submitted_at); } catch (...) {}
    try { finished = std::stoll(meta->finished_at); } catch (...) {}
    response->set_submitted_at_ms(submitted);
    response->set_finished_at_ms(finished);
    done->Run();
}

void TaskService::CancelTask(::google::protobuf::RpcController* controller,
                             const CancelTaskRequest* request,
                             CancelTaskResponse* response,
                             ::google::protobuf::Closure* done)
{
    (void)controller;
    std::string task_id = request->task_id();
    // 仅 PENDING 可取消；返回任务当前状态码（枚举值），-1 表示任务不存在
    static const std::string kScript = R"(
local state = redis.call('HGET', KEYS[1], 'state')
if not state then return {'-1', '0'} end
if state == '0' then
  redis.call('HSET', KEYS[1], 'state', '5', 'finished_at', ARGV[1],
             'fail_reason', 'canceled by user')
  redis.call('XADD', KEYS[2], '*', 'task_id', ARGV[2], 'event', 'canceled')
  redis.call('XTRIM', KEYS[2], 'MAXLEN', '~', ARGV[3])
  return {'1', '5'}
end
if state == '1' then return {'0', '1'} end
if state == '2' then return {'0', '2'} end
if state == '3' then return {'0', '3'} end
if state == '4' then return {'0', '4'} end
return {'0', '5'}
)";
    std::vector<std::string> keys = {mprpc::ItemKey(task_id), mprpc::ResultStream()};
    std::vector<std::string> args = {
        std::to_string(NowMs()), task_id,
        std::to_string(mprpc::kResultTrimLen)};
    auto reply = m_redis.EvalArr(kScript, keys, args);

    response->mutable_result()->set_errorcode(0);
    if (reply.size() < 2 || reply[0] == "-1")
    {
        response->set_success(false);
        response->set_state(TaskState::PENDING);
    }
    else
    {
        response->set_success(reply[0] == "1");
        response->set_state(static_cast<TaskState>(std::atoi(reply[1].c_str())));
    }
    done->Run();
}
