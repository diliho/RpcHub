#include <iostream>
#include <cassert>
#include <thread>
#include <vector>
#include "pool/lock_free_queue.h"

using namespace mprpc;

void TestLockFreeQueueBasic() {
    std::cout << "Testing LockFreeQueue Basic..." << std::endl;
    
    LockFreeQueue<int, 16> queue;
    
    assert(queue.Empty());
    assert(queue.Size() == 0);
    
    assert(queue.Push(1));
    assert(queue.Push(2));
    assert(queue.Push(3));
    
    assert(!queue.Empty());
    assert(queue.Size() == 3);
    
    auto val = queue.Pop();
    assert(val.has_value() && val.value() == 1);
    
    val = queue.Pop();
    assert(val.has_value() && val.value() == 2);
    
    val = queue.Pop();
    assert(val.has_value() && val.value() == 3);
    
    assert(queue.Empty());
    
    val = queue.Pop();
    assert(!val.has_value());
    
    std::cout << "LockFreeQueue Basic: PASSED" << std::endl;
}

void TestLockFreeQueueFull() {
    std::cout << "Testing LockFreeQueue Full..." << std::endl;
    
    LockFreeQueue<int, 4> queue;
    
    assert(queue.Push(1));
    assert(queue.Push(2));
    assert(queue.Push(3));
    assert(queue.Push(4));
    assert(!queue.Push(5));
    
    assert(queue.Size() == 4);
    
    auto val = queue.Pop();
    assert(val.has_value() && val.value() == 1);
    assert(queue.Push(5));
    
    std::cout << "LockFreeQueue Full: PASSED" << std::endl;
}

void TestLockFreeQueueConcurrency() {
    std::cout << "Testing LockFreeQueue Concurrency..." << std::endl;
    
    LockFreeQueue<int, 1024> queue;
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    std::atomic<int> consumed{0};
    
    for (int t = 0; t < 2; ++t) {
        producers.emplace_back([&queue, t]() {
            for (int i = 0; i < 1000; ++i) {
                while (!queue.Push(t * 1000 + i)) {
                    std::this_thread::yield();
                }
            }
        });
    }
    
    for (int t = 0; t < 2; ++t) {
        consumers.emplace_back([&queue, &consumed]() {
            int count = 0;
            while (count < 1000) {
                auto val = queue.Pop();
                if (val.has_value()) {
                    count++;
                    consumed++;
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }
    
    for (auto& t : producers) t.join();
    for (auto& t : consumers) t.join();
    
    assert(consumed == 2000);
    
    std::cout << "LockFreeQueue Concurrency: PASSED" << std::endl;
}

int main() {
    TestLockFreeQueueBasic();
    TestLockFreeQueueFull();
    TestLockFreeQueueConcurrency();
    
    std::cout << "\nAll lock-free queue tests PASSED!" << std::endl;
    return 0;
}
