#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <utility>

#include "daking/MPSC_queue.hpp"

// A generic MPSC transport backed by daking::MPSC_queue (zero-malloc, chunk-pooled).
// Each consumer selects its own concrete command type (for example ChannelCommand).
template <typename T>
class MpscQueue {
 public:
  MpscQueue() = default;
  MpscQueue(const MpscQueue&) = delete;
  MpscQueue& operator=(const MpscQueue&) = delete;
  ~MpscQueue() {
    Drain([](T&&) {});
  }

  void Push(T value) {
    size_.fetch_add(1, std::memory_order_relaxed);
    queue_.enqueue(std::move(value));
  }

  [[nodiscard]] std::size_t ApproximateSize() const noexcept { return size_.load(std::memory_order_relaxed); }

  template <typename Consumer>
  void Drain(Consumer&& consumer) {
    T item;
    while (queue_.try_dequeue(item)) {
      size_.fetch_sub(1, std::memory_order_relaxed);
      consumer(std::move(item));
    }
  }

 private:
  daking::MPSC_queue<T> queue_;
  std::atomic<std::size_t> size_ {0};
};

// The single deliberate dynamic boundary: Context wakes and drains mailboxes,
// while each mailbox retains concrete commands and switch-based handling.
class IoCommandMailbox {
 public:
  virtual ~IoCommandMailbox() = default;
  virtual void DrainCommandsOnIoThread() = 0;
};
