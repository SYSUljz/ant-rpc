#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>

#include "ant_rpc/type.hpp"

// ============================================================
// Chase-Lev SPMC (Single-Producer Multi-Consumer) Lock-Free Deque
// Owner Thread: LIFO Push & TryPop at the Bottom end
// Stealer Threads: FIFO Steal at the Top end
// Uses std::array<T, kMaxSize> inline storage for zero pointer allocation
// ============================================================
template <typename T = TaskNode*, std::size_t kMaxSize = ant_rpc::constants::kDefaultSpmcCapacity>
class SPMCQueue {
  static_assert((kMaxSize & (kMaxSize - 1)) == 0, "kMaxSize must be a power of 2");

 public:
  SPMCQueue() = default;

  SPMCQueue(const SPMCQueue&) = delete;
  SPMCQueue(SPMCQueue&&) = delete;
  SPMCQueue& operator=(const SPMCQueue&) = delete;
  SPMCQueue& operator=(SPMCQueue&&) = delete;

  // 1. Owner Thread Push operation (Bottom end LIFO, zero CAS)
  bool Push(const T& node) {
    int64_t b = bottom_.load(std::memory_order_relaxed);
    int64_t t = top_.load(std::memory_order_acquire);
    if (b - t < static_cast<int64_t>(kMaxSize)) [[likely]] {
      buffer_[b & kMask] = node;
      // Release memory barrier ensures task node write is visible to Stealer threads
      bottom_.store(b + 1, std::memory_order_release);
      return true;
    } else {
      return false;  // Queue full
    }
  }

  // 2. Owner Thread TryPop operation (Bottom end LIFO, zero CAS when items > 1)
  T TryPop() {
    int64_t b = bottom_.load(std::memory_order_relaxed) - 1;
    bottom_.store(b, std::memory_order_relaxed);
    int64_t t = top_.load(std::memory_order_acquire);

    if (t <= b) {
      T task = buffer_[b & kMask];
      if (t == b) {
        // Single remaining task contention: Owner upgrades to CAS to compete with Stealers for top pointer
        if (!top_.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst, std::memory_order_relaxed)) {
          task = nullptr;  // Stolen by a Stealer thread
        }
        bottom_.store(b + 1, std::memory_order_relaxed);
      }
      return task;
    } else {
      // Queue empty, restore bottom_ pointer
      bottom_.store(b + 1, std::memory_order_relaxed);
      return nullptr;
    }
  }

  // 3. Stealer Thread Steal operation (Top end FIFO, CAS contention)
  T Steal() {
    int64_t t = top_.load(std::memory_order_acquire);
    int64_t b = bottom_.load(std::memory_order_acquire);
    if (t < b) {
      T task = buffer_[t & kMask];
      if (top_.compare_exchange_weak(t, t + 1, std::memory_order_seq_cst, std::memory_order_relaxed)) {
        return task;  // Steal successful
      }
    }
    return nullptr;
  }

  int64_t Size() const noexcept {
    int64_t b = bottom_.load(std::memory_order_relaxed);
    int64_t t = top_.load(std::memory_order_relaxed);
    int64_t sz = b - t;
    return sz > 0 ? sz : 0;
  }

  bool Empty() const noexcept { return Size() == 0; }

  // 4. Owner Thread TakeHalf operation (Extract half of local tasks to offload to global queue)
  std::size_t TakeHalf(std::array<T, kMaxSize>& out_batch) {
    int64_t b = bottom_.load(std::memory_order_relaxed);
    int64_t t = top_.load(std::memory_order_acquire);
    int64_t size = b - t;
    if (size <= 0) {
      return 0;
    }

    int64_t num_to_take = size / 2;
    if (num_to_take == 0) {
      num_to_take = 1;
    }

    if (top_.compare_exchange_strong(t, t + num_to_take, std::memory_order_seq_cst, std::memory_order_relaxed)) {
      for (int64_t i = 0; i < num_to_take; ++i) {
        out_batch[i] = buffer_[(t + i) & kMask];
      }
      return static_cast<std::size_t>(num_to_take);
    }
    return 0;
  }

 private:
  static constexpr int64_t kMask = static_cast<int64_t>(kMaxSize - 1);
  std::array<T, kMaxSize> buffer_ {};
  alignas(kCacheLineSize) std::atomic<int64_t> bottom_ {0};
  alignas(kCacheLineSize) std::atomic<int64_t> top_ {0};
};
