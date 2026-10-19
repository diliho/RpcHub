/*
TaskHub 压测与操作 CLI：
  submit  提交单个任务并打印 task_id
  query   查询任务状态
  cancel  取消任务
  bench   并发提交 N 任务 → ack 延迟分位 → 轮询终态 → e2e 分位 + 状态分布
  watch   轮询单个任务直到终态
*/
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <iomanip>
#include <cstring>
#include <map>
#include <mutex>
#include "mprpcapplication.h"
#include "task.pb.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"

using namespace fixbug;

static const char* StateName(int s)
{
    switch (s)
    {
    case TaskState::PENDING: return "PENDING";
    case TaskState::RUNNING: return "RUNNING";
    case TaskState::SUCCEEDED: return "SUCCEEDED";
    case TaskState::FAILED: return "FAILED";
    case TaskState::DEAD: return "DEAD";
    case TaskState::CANCELED: return "CANCELED";
    }
    return "UNKNOWN";
}

static TaskType ParseType(const std::string& s)
{
    if (s == "sleep" || s == "0") return TaskType::SLEEP;
    if (s == "compute" || s == "1") return TaskType::COMPUTE;
    return TaskType::FAIL;
}

// 每线程独立 Channel/Stub（hiredis/连接语义按连接隔离，避免共享状态）
struct Client
{
    std::unique_ptr<MprpcChannel> channel;
    std::unique_ptr<TaskServiceRpc_Stub> stub;
    Client()
    {
        channel.reset(new MprpcChannel());
        stub.reset(new TaskServiceRpc_Stub(channel.get()));
    }
};

static const double kPS[4] = {0.5, 0.9, 0.95, 0.99};

static std::vector<double> Percentiles(std::vector<double> v)
{
    std::sort(v.begin(), v.end());
    std::vector<double> out;
    if (v.empty()) return out;
    for (int i = 0; i < 4; ++i)
        out.push_back(v[std::min(v.size() - 1, (size_t)(v.size() * kPS[i]))]);
    return out;
}

static void PrintPercentiles(const std::vector<double>& p)
{
    if (p.size() < 4) return;
    std::cout << "  P50=" << p[0] << "ms  P90=" << p[1]
              << "ms  P95=" << p[2] << "ms  P99=" << p[3] << "ms\n";
}

static void SetSpec(TaskSpec* spec, const std::map<std::string, std::string>& opt)
{
    auto get = [&](const std::string& k, const std::string& d) {
        auto it = opt.find(k);
        return it == opt.end() ? d : it->second;
    };
    spec->set_type(ParseType(get("type", "sleep")));
    spec->set_payload(get("payload", "500"));
    spec->set_priority(std::stoi(get("prio", "1")));
    spec->set_timeout_ms(std::stoi(get("timeout", "10000")));
    spec->set_max_retry(std::stoi(get("retry", "3")));
    if (opt.count("idem")) spec->set_idempotency_key(opt.at("idem"));
}

static int CmdSubmit(const std::map<std::string, std::string>& opt)
{
    Client c;
    SubmitTaskRequest req;
    SetSpec(req.mutable_spec(), opt);
    SubmitTaskResponse resp;
    MprpcController ctrl;
    c.stub->SubmitTask(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed())
    {
        std::cout << "submit failed: " << ctrl.ErrorText() << std::endl;
        return 1;
    }
    if (resp.result().errorcode() != 0)
    {
        std::cout << "submit rejected: " << resp.result().errormsg() << std::endl;
        return 1;
    }
    std::cout << "task_id=" << resp.task_id()
              << (resp.duplicate() ? " (idempotent hit)" : "") << std::endl;
    return 0;
}

static int CmdQuery(const std::string& task_id)
{
    Client c;
    QueryTaskRequest req;
    req.set_task_id(task_id);
    QueryTaskResponse resp;
    MprpcController ctrl;
    c.stub->QueryTask(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed() || resp.result().errorcode() != 0)
    {
        std::cout << "query failed: "
                  << (ctrl.Failed() ? ctrl.ErrorText() : resp.result().errormsg())
                  << std::endl;
        return 1;
    }
    std::cout << "task_id=" << resp.task_id()
              << " state=" << StateName(resp.state())
              << " retries=" << resp.retries();
    if (!resp.worker().empty()) std::cout << " worker=" << resp.worker();
    if (!resp.result_payload().empty()) std::cout << " result=" << resp.result_payload();
    if (!resp.fail_reason().empty()) std::cout << " reason=" << resp.fail_reason();
    std::cout << std::endl;
    return 0;
}

static int CmdCancel(const std::string& task_id)
{
    Client c;
    CancelTaskRequest req;
    req.set_task_id(task_id);
    CancelTaskResponse resp;
    MprpcController ctrl;
    c.stub->CancelTask(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed() || resp.result().errorcode() != 0)
    {
        std::cout << "cancel failed" << std::endl;
        return 1;
    }
    std::cout << (resp.success() ? "canceled" : "not cancelable")
              << " state=" << StateName(resp.state()) << std::endl;
    return 0;
}

static int CmdWatch(const std::string& task_id)
{
    Client c;
    while (true)
    {
        QueryTaskRequest req;
        req.set_task_id(task_id);
        QueryTaskResponse resp;
        MprpcController ctrl;
        c.stub->QueryTask(&ctrl, &req, &resp, nullptr);
        if (ctrl.Failed() || resp.result().errorcode() != 0)
        {
            std::cout << "watch failed" << std::endl;
            return 1;
        }
        if (resp.state() >= TaskState::SUCCEEDED)
        {
            std::cout << "final state=" << StateName(resp.state())
                      << " retries=" << resp.retries();
            if (!resp.result_payload().empty()) std::cout << " result=" << resp.result_payload();
            if (!resp.fail_reason().empty()) std::cout << " reason=" << resp.fail_reason();
            std::cout << std::endl;
            return 0;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

// ── bench：并发提交 + 终态轮询 + 延迟分位 + 状态分布 ──
struct BenchResult
{
    std::atomic<int64_t> ok{0}, err{0}, dup{0};
    std::vector<double> ack_ms;                 // 提交往返延迟
    std::vector<std::string> task_ids;
    std::mutex mtx;
};

static void SubmitWorker(Client* c, int count, const TaskSpec& spec, BenchResult* out)
{
    for (int i = 0; i < count; ++i)
    {
        SubmitTaskRequest req;
        *req.mutable_spec() = spec;
        SubmitTaskResponse resp;
        MprpcController ctrl;
        auto t0 = std::chrono::steady_clock::now();
        c->stub->SubmitTask(&ctrl, &req, &resp, nullptr);
        double ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - t0).count();
        if (ctrl.Failed() || resp.result().errorcode() != 0) out->err++;
        else
        {
            out->ok++;
            if (resp.duplicate()) out->dup++;
            std::lock_guard<std::mutex> lk(out->mtx);
            out->ack_ms.push_back(ms);
            out->task_ids.push_back(resp.task_id());
        }
    }
}

static int CmdBench(const std::map<std::string, std::string>& opt)
{
    int n = 10000, cthr = 8;
    try { n = std::stoi(opt.at("n")); } catch (...) {}
    try { cthr = std::stoi(opt.at("c")); } catch (...) {}
    n = std::clamp(n, 1, 2000000);
    cthr = std::clamp(cthr, 1, 64);

    TaskSpec spec;
    SetSpec(&spec, opt);

    std::cout << "=== TaskHub bench ===\n  tasks=" << n << " concurrency=" << cthr
              << " type=" << TaskType_Name(spec.type())
              << " payload=" << spec.payload() << "\n";

    // Phase 1: 并发提交（Client 构造含 ZK 连接，不计入提交吞吐）
    BenchResult res;
    double submit_sec = 0;
    {
        std::vector<std::thread> threads;
        std::vector<std::unique_ptr<Client>> clients;
        for (int i = 0; i < cthr; ++i) clients.push_back(std::make_unique<Client>());
        auto t0 = std::chrono::steady_clock::now();
        int per = n / cthr, rem = n % cthr;
        for (int i = 0; i < cthr; ++i)
        {
            int cnt = per + (i < rem ? 1 : 0);
            threads.emplace_back(SubmitWorker, clients[i].get(), cnt, spec, &res);
        }
        for (auto& t : threads) t.join();
        submit_sec = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - t0).count();
    }

    std::cout << "-- submit --\n  ok=" << res.ok << " err=" << res.err
              << " dup=" << res.dup
              << "  throughput=" << (int64_t)(res.ok / std::max(submit_sec, 1e-6))
              << "/s\n  ack: ";
    PrintPercentiles(Percentiles(res.ack_ms));

    // Phase 2: 轮询终态（4 线程批量查询）
    std::atomic<size_t> cursor{0};
    std::atomic<int64_t> final_states[6] = {};
    std::vector<double> e2e_ms;
    std::mutex e2e_mtx;
    int64_t submitted_base = 0;
    auto poll = [&]()
    {
        Client c;
        while (true)
        {
            size_t i = cursor.fetch_add(64);
            if (i >= res.task_ids.size()) return;
            size_t end = std::min(res.task_ids.size(), i + 64);
            for (size_t j = i; j < end; ++j)
            {
                const std::string& id = res.task_ids[j];
                for (int attempt = 0; attempt < 3000; ++attempt)
                {
                    QueryTaskRequest req;
                    req.set_task_id(id);
                    QueryTaskResponse resp;
                    MprpcController ctrl;
                    c.stub->QueryTask(&ctrl, &req, &resp, nullptr);
                    if (!ctrl.Failed() && resp.result().errorcode() == 0 &&
                        resp.state() >= TaskState::SUCCEEDED)
                    {
                        final_states[resp.state()]++;
                        double ms = static_cast<double>(resp.finished_at_ms() - resp.submitted_at_ms());
                        std::lock_guard<std::mutex> lk(e2e_mtx);
                        e2e_ms.push_back(ms);
                        break;
                    }
                    if (attempt == 2999) final_states[0]++; // 超时未终态
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }
        }
    };
    {
        std::vector<std::thread> threads;
        for (int i = 0; i < 4; ++i) threads.emplace_back(poll);
        for (auto& t : threads) t.join();
    }

    int64_t succeeded = final_states[TaskState::SUCCEEDED];
    int64_t failed = final_states[TaskState::FAILED];
    int64_t dead = final_states[TaskState::DEAD];
    int64_t canceled = final_states[TaskState::CANCELED];
    int64_t stuck = final_states[TaskState::PENDING];

    std::cout << "-- result --\n"
              << "  succeeded=" << succeeded << " dead=" << dead
              << " canceled=" << canceled << " stuck=" << stuck << "\n"
              << "  e2e: ";
    PrintPercentiles(Percentiles(e2e_ms));
    // 准确性对账：提交数 == 终态数 + 未终态数
    int64_t accounted = succeeded + failed + dead + canceled + stuck;
    std::cout << "  reconcile: submitted=" << res.ok << " accounted=" << accounted
              << " " << (accounted == res.ok ? "[OK]" : "[MISMATCH]") << "\n";
    return 0;
}

int main(int argc, char **argv)
{
    // 框架 Init 只认 "-i <conf>"：先拆出配置参数，其余作为业务参数
    std::vector<char*> init_args;
    std::vector<std::string> biz;
    init_args.push_back(argv[0]);
    for (int i = 1; i < argc; ++i)
    {
        std::string a = argv[i];
        if (a == "-i" && i + 1 < argc)
        {
            init_args.push_back(argv[i]);
            init_args.push_back(argv[++i]);
        }
        else biz.push_back(a);
    }
    if (init_args.size() < 3 || biz.empty())
    {
        std::cout << "usage: task_bench -i <configfile> <command> [options]\n"
                  << "  submit [-t sleep|compute|fail] [-p payload] [--prio 0|1]\n"
                  << "         [--timeout ms] [--retry n] [--idem key] [--batch n]\n"
                  << "  query  <task_id>\n  cancel <task_id>\n  watch  <task_id>\n"
                  << "  bench  -n 10000 -c 8 [-t type -p payload]\n";
        return 1;
    }
    int init_argc = (int)init_args.size();
    MprpcApplication::Init(init_argc, init_args.data());

    std::string cmd = biz[0];
    std::map<std::string, std::string> opt;
    std::vector<std::string> pos;
    for (size_t i = 1; i < biz.size(); ++i)
    {
        const std::string& a = biz[i];
        if (a == "-t" && i + 1 < biz.size()) opt["type"] = biz[++i];
        else if (a == "-p" && i + 1 < biz.size()) opt["payload"] = biz[++i];
        else if (a == "--prio" && i + 1 < biz.size()) opt["prio"] = biz[++i];
        else if (a == "--timeout" && i + 1 < biz.size()) opt["timeout"] = biz[++i];
        else if (a == "--retry" && i + 1 < biz.size()) opt["retry"] = biz[++i];
        else if (a == "--idem" && i + 1 < biz.size()) opt["idem"] = biz[++i];
        else if (a == "--batch" && i + 1 < biz.size()) opt["batch"] = biz[++i];
        else if (a == "-n" && i + 1 < biz.size()) opt["n"] = biz[++i];
        else if (a == "-c" && i + 1 < biz.size()) opt["c"] = biz[++i];
        else pos.push_back(a);
    }

    if (cmd == "submit") return CmdSubmit(opt);
    if (cmd == "query") return pos.empty() ? 1 : CmdQuery(pos[0]);
    if (cmd == "cancel") return pos.empty() ? 1 : CmdCancel(pos[0]);
    if (cmd == "watch") return pos.empty() ? 1 : CmdWatch(pos[0]);
    if (cmd == "bench") return CmdBench(opt);
    std::cout << "unknown command: " << cmd << std::endl;
    return 1;
}
