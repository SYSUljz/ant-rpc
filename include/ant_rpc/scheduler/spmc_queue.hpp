#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

#include "ant_rpc/constants.hpp"
#include "ant_rpc/type.hpp"

// ============================================================
// Chase-Lev SPMC (Single-Producer Multi-Consumer) Lock-Free Deque
// Reference:
//   - Chase & Lev, "Dynamic Circular Work-Stealing Deque", SPAA 2005
//   - Lê et al., "Correct and Efficient Work-Stealing for Weak Memory Models", PPoPP 2013
//   - Taskflow BoundedWSQ (verified under C++20 memory model)
// Owner Thread: LIFO Push & TryPop at the Bottom end
// Stealer Threads: FIFO Steal at the Top end
// ============================================================
template <typename T = TaskNode*, std::size_t kMaxSize = ant_rpc::constants::kDefaultSpmcCapacity>
class SPMCQueue {
  static_assert((kMaxSize & (kMaxSize - 1)) == 0, "kMaxSize must be a power of 2");
  static constexpr std::size_t kMask = kMaxSize - 1;

 public:
  SPMCQueue() = default;

  SPMCQueue(const SPMCQueue&) = delete;
  SPMCQueue(SPMCQueue&&) = delete;
  SPMCQueue& operator=(const SPMCQueue&) = delete;
  SPMCQueue& operator=(SPMCQueue&&) = delete;

  // 1. Owner Thread Push operation (Bottom end LIFO, zero CAS)
  bool Push(const T& o) noexcept {
    int64_t b = bottom_.load(std::memory_order_relaxed);
    int64_t size = b - cached_top_;
    if (size >= static_cast<int64_t>(kMaxSize)) {
      cached_top_ = top_.load(std::memory_order_acquire);
      size = b - cached_top_;
      if (size >= static_cast<int64_t>(kMaxSize)) {
        return false;
      }
    }

    buffer_[b & kMask].store(o, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_release);
    bottom_.store(b + 1, std::memory_order_release);
    return true;
  }

  // 2. Owner Thread TryPop operation (Bottom end LIFO)
  T TryPop() noexcept {
    int64_t b = bottom_.load(std::memory_order_relaxed) - 1;
    bottom_.store(b, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    int64_t t = top_.load(std::memory_order_relaxed);

    T item = nullptr;

    if (t <= b) {
      item = buffer_[b & kMask].load(std::memory_order_relaxed);
      if (t == b) {
        if (!top_.compare_exchange_strong(t, t + 1,
              std::memory_order_seq_cst, std::memory_order_relaxed)) {
          item = nullptr;
        }
        bottom_.store(b + 1, std::memory_order_relaxed);
      }
    } else {
      bottom_.store(b + 1, std::memory_order_relaxed);
    }

    return item;
  }

  // 3. Stealer Thread Steal operation (Top end FIFO, CAS contention)
  T Steal() noexcept {
    int64_t t = top_.load(std::memory_order_acquire);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    int64_t b = bottom_.load(std::memory_order_acquire);

    T item = nullptr;

    if (t < b) {
      item = buffer_[t & kMask].load(std::memory_order_relaxed);
      if (!top_.compare_exchange_strong(t, t + 1,
            std::memory_order_seq_cst, std::memory_order_relaxed)) {
        return nullptr;
      }
    }

    return item;
  }

  int64_t Size() const noexcept {
    int64_t t = top_.load(std::memory_order_relaxed);
    int64_t b = bottom_.load(std::memory_order_relaxed);
    return b >= t ? (b - t) : 0;
  }

  bool Empty() const noexcept {
    int64_t t = top_.load(std::memory_order_relaxed);
    int64_t b = bottom_.load(std::memory_order_relaxed);
    return b <= t;
  }

 private:
  alignas(kCacheLineSize) std::atomic<int64_t> top_ {0};
  alignas(kCacheLineSize) std::atomic<int64_t> bottom_ {0};
  int64_t cached_top_ {0};
  alignas(kCacheLineSize) std::atomic<T> buffer_[kMaxSize];
};
