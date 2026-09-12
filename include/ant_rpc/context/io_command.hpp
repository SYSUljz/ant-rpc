#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <utility>

// A generic MPSC transport, but never a type-erased task queue. Each consumer
// selects its own concrete command type (for example ChannelCommand).
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
    auto* node = new Node {std::move(value), nullptr};
    size_.fetch_add(1, std::memory_order_relaxed);
    Node* head = head_.load(std::memory_order_relaxed);
    do {
      node->next = head;
    } while (!head_.compare_exchange_weak(head, node, std::memory_order_release, std::memory_order_relaxed));
  }
  [[nodiscard]] std::size_t ApproximateSize() const noexcept { return size_.load(std::memory_order_relaxed); }

  template <typename Consumer>
  void Drain(Consumer&& consumer) {
    Node* list = head_.exchange(nullptr, std::memory_order_acquire);
    Node* fifo = nullptr;
    while (list) {
      Node* next = list->next;
      list->next = fifo;
      fifo = list;
      list = next;
    }
    while (fifo) {
      Node* next = fifo->next;
      consumer(std::move(fifo->value));
      delete fifo;
      size_.fetch_sub(1, std::memory_order_relaxed);
      fifo = next;
    }
  }

 private:
  struct Node {
    T value;
    Node* next;
  };
  std::atomic<Node*> head_ {nullptr};
  std::atomic<std::size_t> size_ {0};
};

// The single deliberate dynamic boundary: Context wakes and drains mailboxes,
// while each mailbox retains concrete commands and switch-based handling.
class IoCommandMailbox {
 public:
  virtual ~IoCommandMailbox() = default;
  virtual void DrainCommandsOnIoThread() = 0;
};
