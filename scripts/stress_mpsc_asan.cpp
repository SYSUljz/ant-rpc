#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <cassert>
#include <string>
#include <memory>
#include <chrono>
#include <sstream>

#include "daking/MPSC_queue.hpp"

using daking::MPSC_queue;

// Tracking object to verify destructor and constructor calls
struct TrackedObject {
    static inline std::atomic<int64_t> live_count{0};
    static inline std::atomic<int64_t> total_constructed{0};
    static inline std::atomic<int64_t> total_destructed{0};

    int id{0};
    std::string data;

    TrackedObject() : id(0), data("default") {
        live_count.fetch_add(1, std::memory_order_relaxed);
        total_constructed.fetch_add(1, std::memory_order_relaxed);
    }

    TrackedObject(int id, std::string d) : id(id), data(std::move(d)) {
        live_count.fetch_add(1, std::memory_order_relaxed);
        total_constructed.fetch_add(1, std::memory_order_relaxed);
    }

    TrackedObject(const TrackedObject& other) : id(other.id), data(other.data) {
        live_count.fetch_add(1, std::memory_order_relaxed);
        total_constructed.fetch_add(1, std::memory_order_relaxed);
    }

    TrackedObject(TrackedObject&& other) noexcept : id(other.id), data(std::move(other.data)) {
        live_count.fetch_add(1, std::memory_order_relaxed);
        total_constructed.fetch_add(1, std::memory_order_relaxed);
    }

    TrackedObject& operator=(const TrackedObject& other) {
        if (this != &other) {
            id = other.id;
            data = other.data;
        }
        return *this;
    }

    TrackedObject& operator=(TrackedObject&& other) noexcept {
        if (this != &other) {
            id = other.id;
            data = std::move(other.data);
        }
        return *this;
    }

    ~TrackedObject() {
        live_count.fetch_sub(1, std::memory_order_relaxed);
        total_destructed.fetch_add(1, std::memory_order_relaxed);
    }

    static void Reset() {
        live_count.store(0);
        total_constructed.store(0);
        total_destructed.store(0);
    }
};

void Test1_BasicTrackedObjects() {
    std::cout << "[Test 1] Testing TrackedObject Lifecycle with SPSC..." << std::endl;
    TrackedObject::Reset();
    {
        MPSC_queue<TrackedObject> queue;
        for (int i = 0; i < 1000; ++i) {
            queue.enqueue(TrackedObject(i, "payload_" + std::to_string(i)));
        }

        for (int i = 0; i < 1000; ++i) {
            TrackedObject obj;
            bool ok = queue.try_dequeue(obj);
            assert(ok);
            assert(obj.id == i);
            assert(obj.data == "payload_" + std::to_string(i));
        }
        TrackedObject dummy;
        assert(!queue.try_dequeue(dummy));
    }
    assert(TrackedObject::live_count.load() == 0);
    std::cout << "  Passed! (Live count = " << TrackedObject::live_count.load() << ")" << std::endl;
}

void Test2_LeftoverDestruction() {
    std::cout << "[Test 2] Testing queue destruction with leftover un-dequeued elements..." << std::endl;
    TrackedObject::Reset();
    {
        MPSC_queue<TrackedObject> queue;
        for (int i = 0; i < 5000; ++i) {
            queue.enqueue(TrackedObject(i, "leftover_" + std::to_string(i)));
        }
        // Only dequeue 1000, leaving 4000 in the queue
        for (int i = 0; i < 1000; ++i) {
            TrackedObject obj;
            assert(queue.try_dequeue(obj));
        }
        // Destructor of queue must cleanly destroy remaining 4000 objects
    }
    assert(TrackedObject::live_count.load() == 0);
    std::cout << "  Passed! (All leftover elements destroyed cleanly, live=" << TrackedObject::live_count.load() << ")" << std::endl;
}

void Test3_MultiProducerHighConcurrency() {
    std::cout << "[Test 3] Testing 16 concurrent producers pushing 50,000 items each..." << std::endl;
    TrackedObject::Reset();
    constexpr int kProducers = 16;
    constexpr int kItemsPerProducer = 50000;
    constexpr int kTotalItems = kProducers * kItemsPerProducer;

    MPSC_queue<TrackedObject> queue;
    std::atomic<bool> start_signal{false};
    std::atomic<bool> producers_done{false};

    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p]() {
            while (!start_signal.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int i = 0; i < kItemsPerProducer; ++i) {
                queue.enqueue(TrackedObject(p, "data_" + std::to_string(i)));
            }
        });
    }

    uint64_t dequeued_count = 0;
    std::vector<int> count_per_producer(kProducers, 0);

    auto consumer = std::thread([&]() {
        while (!start_signal.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        while (dequeued_count < kTotalItems) {
            TrackedObject obj;
            if (queue.try_dequeue(obj)) {
                assert(obj.id >= 0 && obj.id < kProducers);
                count_per_producer[obj.id]++;
                dequeued_count++;
            } else {
                std::this_thread::yield();
            }
        }
    });

    start_signal.store(true, std::memory_order_release);

    for (auto& t : producers) {
        t.join();
    }
    consumer.join();

    assert(dequeued_count == kTotalItems);
    for (int p = 0; p < kProducers; ++p) {
        assert(count_per_producer[p] == kItemsPerProducer);
    }
    std::cout << "  Passed! (Total dequeued: " << dequeued_count << ", live=" << TrackedObject::live_count.load() << ")" << std::endl;
}

void Test4_ThreadChurnAndHookDestruction() {
    std::cout << "[Test 4] Testing thread creation and destruction churn with thread_hook recycling..." << std::endl;
    MPSC_queue<int> queue;
    constexpr int kRounds = 50;
    constexpr int kThreadsPerRound = 8;
    constexpr int kItems = 1000;

    for (int round = 0; round < kRounds; ++round) {
        std::vector<std::thread> threads;
        for (int i = 0; i < kThreadsPerRound; ++i) {
            threads.emplace_back([&]() {
                for (int j = 0; j < kItems; ++j) {
                    queue.enqueue(j);
                }
            });
        }
        for (auto& t : threads) {
            t.join();
        }

        // Dequeue items produced in this round
        int val = 0;
        int dequeued = 0;
        while (queue.try_dequeue(val)) {
            dequeued++;
        }
        assert(dequeued == kThreadsPerRound * kItems);
    }
    std::cout << "  Passed! (Successfully recycled thread_hook across " << (kRounds * kThreadsPerRound) << " threads)" << std::endl;
}

void Test5_MultipleQueueInstancesConcurrent() {
    std::cout << "[Test 5] Testing multiple independent queue instances created/destroyed concurrently..." << std::endl;
    constexpr int kThreads = 8;
    std::vector<std::thread> workers;

    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([t]() {
            for (int iter = 0; iter < 100; ++iter) {
                MPSC_queue<std::string> local_q;
                local_q.enqueue("thread_" + std::to_string(t) + "_iter_" + std::to_string(iter));
                std::string res;
                bool ok = local_q.try_dequeue(res);
                assert(ok);
            }
        });
    }

    for (auto& w : workers) {
        w.join();
    }
    std::cout << "  Passed! (All independent queues created and destroyed safely)" << std::endl;
}

struct SharedResource {
    static inline std::atomic<int> alive{0};
    int val{0};
    SharedResource(int v) : val(v) { alive.fetch_add(1); }
    ~SharedResource() { alive.fetch_sub(1); }
};

void Test6_SharedPtrPayload() {
    std::cout << "[Test 6] Testing std::shared_ptr payload (simulating IoCommand)..." << std::endl;
    SharedResource::alive.store(0);
    {
        MPSC_queue<std::shared_ptr<SharedResource>> queue;
        constexpr int kN = 10000;
        for (int i = 0; i < kN; ++i) {
            queue.enqueue(std::make_shared<SharedResource>(i));
        }
        assert(SharedResource::alive.load() == kN);

        for (int i = 0; i < kN; ++i) {
            std::shared_ptr<SharedResource> ptr;
            assert(queue.try_dequeue(ptr));
            assert(ptr->val == i);
        }
    }
    assert(SharedResource::alive.load() == 0);
    std::cout << "  Passed! (shared_ptr lifecycle fully intact, live=" << SharedResource::alive.load() << ")" << std::endl;
}

int main() {
    std::cout << "=== Starting MPSC_queue AddressSanitizer Stress Tests ===" << std::endl;
    auto start = std::chrono::steady_clock::now();

    Test1_BasicTrackedObjects();
    Test2_LeftoverDestruction();
    Test3_MultiProducerHighConcurrency();
    Test4_ThreadChurnAndHookDestruction();
    Test5_MultipleQueueInstancesConcurrent();
    Test6_SharedPtrPayload();

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    std::cout << "=== All MPSC_queue ASan Tests PASSED in " << elapsed << " ms! ===" << std::endl;
    return 0;
}
