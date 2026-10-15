#pragma once
#include <atomic>
#include <array>
#include <optional>
#include <cstddef>

namespace mprpc {

// 有界无锁环形队列（Dmitry Vyukov bounded MPMC 算法）
// 多生产者/多消费者并发安全：head/tail 用 CAS 抢占槽位，每个槽位自带序号
// 区分"可写/可读"状态，数据读写路径完全无锁。
// memory_order：seq 的 acquire/release 配对保证槽内数据写入对消费者可见；
// head/tail 的 CAS 用 relaxed 即可，顺序正确性由 seq 承担。
template<typename T, size_t Capacity = 1024>
class LockFreeQueue {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be power of 2");

    struct Cell {
        std::atomic<size_t> seq;
        T data;
    };

public:
    LockFreeQueue()
    {
        for (size_t i = 0; i < Capacity; ++i)
            m_buffer[i].seq.store(i, std::memory_order_relaxed);
    }

    LockFreeQueue(const LockFreeQueue&) = delete;
    LockFreeQueue& operator=(const LockFreeQueue&) = delete;

    bool Push(const T& item) {
        size_t pos = m_head.load(std::memory_order_relaxed);
        Cell* cell = nullptr;
        for (;;) {
            cell = &m_buffer[pos & (Capacity - 1)];
            size_t seq = cell->seq.load(std::memory_order_acquire);
            auto dif = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos);
            if (dif == 0) {
                // 槽位可写：CAS 抢占，失败说明被其他生产者抢先，重试
                if (m_head.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                    break;
            } else if (dif < 0) {
                return false;  // Queue full
            } else {
                pos = m_head.load(std::memory_order_relaxed);
            }
        }
        cell->data = item;
        cell->seq.store(pos + 1, std::memory_order_release);  // 发布：消费者可读
        return true;
    }

    std::optional<T> Pop() {
        size_t pos = m_tail.load(std::memory_order_relaxed);
        Cell* cell = nullptr;
        for (;;) {
            cell = &m_buffer[pos & (Capacity - 1)];
            size_t seq = cell->seq.load(std::memory_order_acquire);
            auto dif = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos + 1);
            if (dif == 0) {
                // 槽位有数据：CAS 抢占，失败说明被其他消费者抢先，重试
                if (m_tail.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                    break;
            } else if (dif < 0) {
                return std::nullopt;  // Queue empty
            } else {
                pos = m_tail.load(std::memory_order_relaxed);
            }
        }
        T item = std::move(cell->data);
        cell->seq.store(pos + Capacity, std::memory_order_release);  // 归还槽位给下一轮生产者
        return item;
    }

    bool Empty() const {
        return m_head.load(std::memory_order_acquire) ==
               m_tail.load(std::memory_order_acquire);
    }

    size_t Size() const {
        return m_head.load(std::memory_order_acquire) -
               m_tail.load(std::memory_order_acquire);
    }

private:
    std::array<Cell, Capacity> m_buffer;
    alignas(64) std::atomic<size_t> m_head{0};
    alignas(64) std::atomic<size_t> m_tail{0};
};

} // namespace mprpc
