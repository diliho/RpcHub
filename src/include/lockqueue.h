#pragma once
#include <mutex>
#include <condition_variable>
#include <cstddef>
#include "pool/lock_free_queue.h"

// MPSC 阻塞队列（异步日志等场景）：数据路径无锁，底层是 LockFreeQueue 环形缓冲；
// mutex 仅用于消费者休眠/唤醒——队列为空时消费者在条件变量上睡眠，
// 生产者入队后持锁 notify，避免无锁队列非阻塞语义导致的空转烧 CPU。
// notify 在持锁下进行，与 wait 的"检查-睡眠"原子交接，保证不丢唤醒。
template<typename T, size_t Capacity = 8192>
class LockQueue
{
public:
    // 队列满时返回 false（日志场景：丢日志优于阻塞业务线程）
    bool Push(const T& data)
    {
        if (!m_queue.Push(data))
        {
            return false;
        }
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_cond.notify_one();
        }
        return true;
    }

    // 阻塞 Pop；Shutdown 且队列已排空后返回 T{}
    T Pop()
    {
        for (;;)
        {
            if (auto item = m_queue.Pop())
            {
                return std::move(*item);
            }

            std::unique_lock<std::mutex> lock(m_mutex);
            m_cond.wait(lock, [this] { return !m_queue.Empty() || !m_running; });
            if (!m_running && m_queue.Empty())
            {
                return T{};
            }
        }
    }

    void Shutdown()
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_running = false;
        }
        m_cond.notify_all();
    }

    bool Empty()
    {
        return m_queue.Empty();
    }

    size_t Size()
    {
        return m_queue.Size();
    }

private:
    mprpc::LockFreeQueue<T, Capacity> m_queue;
    std::mutex m_mutex;
    std::condition_variable m_cond;
    bool m_running = true;
};
