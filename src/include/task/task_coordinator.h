#pragma once
// TaskCoordinator：接管调度器，三重职责
//   1. ZK 选主（ephemeral+sequential，序号最小者当选；watch 前驱避免惊群）
//   2. 当选后周期回收 idle 超时的 pending 消息（worker 失联 → 重入队 / 死信）
//   3. 队列深度 gauge 打点
// 主从切换演示：kill 掉 leader 所在进程，ZK 会话过期 → 前驱节点消失 →
// 次序节点当选 → 新 leader 接着回收，任务不丢。
#include <atomic>
#include <thread>
#include <algorithm>
#include <functional>
#include "zookeeperutil.h"
#include "logger.h"
#include "task/task_common.h"
#include "task/task_queue.h"
#include "prometheus/metrics_exporter.h"

namespace mprpc {

class TaskCoordinator
{
public:
    TaskCoordinator(ZKClient& zk, TaskQueue& queue, std::string node_id)
        : m_zk(zk), m_queue(queue), m_node_id(std::move(node_id)) {}

    ~TaskCoordinator() { Stop(); }

    void Start()
    {
        m_running = true;
        m_thread = std::thread([this] { ElectLoop(); });
    }

    void Stop()
    {
        if (!m_running.exchange(false)) return;
        if (m_thread.joinable()) m_thread.join();
    }

    bool IsLeader() const { return m_is_leader; }

private:
    // 选主目录：sequential 节点是扁平前缀（/mprpc/tasks/leader_0000000001），
    // 父目录为 /mprpc/tasks，候选节点均带 "leader_" 前缀便于过滤
    static const char* ElectDir() { return "/mprpc/tasks"; }
    static const char* ElectPrefix() { return "leader_"; }

    // 选主主循环：创建顺序临时节点 → 比较序号 → 最小者当选；
    // 非领导者 watch 前驱，前驱消失（ZOO_DELETED）触发重新评估。
    void ElectLoop()
    {
        std::string my_path;
        while (m_running)
        {
            if (!m_zk.isConnected())
            {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }

            // 1) 创建候选节点（重新参选时先清掉旧节点）
            if (!my_path.empty())
            {
                m_zk.Delete(my_path.c_str());
                my_path.clear();
            }
            std::string cand = std::string(ElectDir()) + "/" + ElectPrefix();
            m_zk.Create(cand.c_str(), m_node_id.c_str(),
                        (int)m_node_id.size(),
                        ZOO_EPHEMERAL | ZOO_SEQUENCE,
                        nullptr, 0);
            // Create 用固定 path 无法取回 sequential 实际路径 → 列子节点找自己
            auto children = m_zk.GetChildren(ElectDir());
            for (auto& c : children)
            {
                if (c.rfind(ElectPrefix(), 0) != 0) continue; // 只看候选节点
                std::string full = std::string(ElectDir()) + "/" + c;
                if (m_zk.GetData(full.c_str()) == m_node_id)
                {
                    my_path = full;
                    break;
                }
            }
            if (my_path.empty())
            {
                LOG_ERROR("Coordinator: cannot locate own election node, retrying");
                std::this_thread::sleep_for(std::chrono::seconds(2));
                continue;
            }

            Evaluate(children, my_path);

            // 2) 领导者：进入回收循环直到掉主
            while (m_running && m_is_leader)
            {
                ReclaimOnce();
                for (int i = 0; i < 20 && m_running && m_is_leader; ++i)
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                // 会话丢失即失主（临时节点会话过期自动删除）
                if (!m_zk.isConnected()) m_is_leader = false;
            }

            // 3) 落选/失主：watch 前驱，等待触发后重新参选
            while (m_running && !m_is_leader)
            {
                children = m_zk.GetChildren(ElectDir());
                if (children.empty()) break; // 全部退出，立即重参选

                std::string pred = Predecessor(children, my_path);
                if (pred.empty()) { break; } // 自己已是最小，重评

                // watch 前驱（一次性，触发后回到外层重新评估）
                std::string data;
                m_zk.WGet(pred.c_str(), data,
                          [](zhandle_t*, int type, int, const char*, void*) {
                              // 轻量通知即可，主循环负责重评
                              (void)type;
                          }, nullptr);
                std::this_thread::sleep_for(std::chrono::milliseconds(300));
            }
        }
    }

    void Evaluate(std::vector<std::string> children, const std::string& my_path)
    {
        // 只取候选节点（leader_ 前缀），序号最小者当选（data=node_id 校验）
        std::vector<std::string> cand;
        for (auto& c : children)
            if (c.rfind(ElectPrefix(), 0) == 0) cand.push_back(c);
        if (cand.empty()) { m_is_leader = false; return; }
        std::sort(cand.begin(), cand.end());
        std::string smallest_full = std::string(ElectDir()) + "/" + cand.front();
        bool leader = (m_zk.GetData(smallest_full.c_str()) == m_node_id);
        if (leader && !m_is_leader)
        {
            m_is_leader = true;
            LOG_INFO("Coordinator: elected LEADER, node=%s", m_node_id.c_str());
        }
        else if (!leader && m_is_leader)
        {
            m_is_leader = false;
            LOG_INFO("Coordinator: lost leadership, node=%s", m_node_id.c_str());
        }
    }

    static std::string Predecessor(const std::vector<std::string>& children,
                                   const std::string& my_path)
    {
        std::string my_name = my_path.substr(my_path.find_last_of('/') + 1);
        std::vector<std::string> cand;
        for (auto& c : children)
            if (c.rfind(ElectPrefix(), 0) == 0) cand.push_back(c);
        std::sort(cand.begin(), cand.end());
        std::string prev;
        for (auto& c : cand)
        {
            if (c == my_name) return prev;
            prev = std::string(ElectDir()) + "/" + c;
        }
        return "";
    }

    // 回收循环：接管超时消息 + 队列深度 gauge
    void ReclaimOnce()
    {
        int n = m_queue.ReclaimTimedOut(m_node_id + "-coord", 256);
        if (n > 0)
        {
            LOG_INFO("Coordinator reclaimed %d timed-out task(s)", n);
            MetricsExporter::GetInstance().IncrCounter(
                "task_reclaimed_total", {}, static_cast<double>(n));
        }
        auto& exporter = MetricsExporter::GetInstance();
        std::map<std::string, std::string> lh = {{"priority", "0"}};
        std::map<std::string, std::string> ln = {{"priority", "1"}};
        exporter.SetGauge("task_queue_depth", lh, m_queue.QueueDepth(0));
        exporter.SetGauge("task_queue_depth", ln, m_queue.QueueDepth(1));
    }

    ZKClient& m_zk;
    TaskQueue& m_queue;
    std::string m_node_id;
    std::atomic<bool> m_running{false};
    std::atomic<bool> m_is_leader{false};
    std::thread m_thread;
};

} // namespace mprpc
