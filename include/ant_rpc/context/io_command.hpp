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

#include "butil/iobuf.h"

// Flat value-semantic IO command. Stored contiguously in MpscQueue chunks
// without heap allocation or pointer indirection.
struct IoCommand {
  enum class Type : uint8_t {
    kServerStart = 0,
    kServerCompleteInbound = 1,
    kServerClose = 2,
    kChannelStart = 3,
    kChannelSendFrame = 4,
    kChannelClose = 5,
  };

  Type type {Type::kServerStart};
  bool close_after {false};
  uint16_t reserved {0};
  void* connection {nullptr};
  butil::IOBuf payload {};
  union {
    uint64_t correlation_id {0};
    void* inbound_call_state;
  };

  static IoCommand ServerStart(void* conn) {
    IoCommand cmd;
    cmd.type = Type::kServerStart;
    cmd.connection = conn;
    return cmd;
  }

  static IoCommand ServerCompleteInbound(void* conn, void* call_state, butil::IOBuf response, bool close_after = false) {
    IoCommand cmd;
    cmd.type = Type::kServerCompleteInbound;
    cmd.connection = conn;
    cmd.inbound_call_state = call_state;
    cmd.payload = std::move(response);
    cmd.close_after = close_after;
    return cmd;
  }

  static IoCommand ServerClose(void* conn) {
    IoCommand cmd;
    cmd.type = Type::kServerClose;
    cmd.connection = conn;
    return cmd;
  }

  static IoCommand ChannelStart(void* conn) {
    IoCommand cmd;
    cmd.type = Type::kChannelStart;
    cmd.connection = conn;
    return cmd;
  }

  static IoCommand ChannelSendFrame(void* conn, uint64_t correlation_id, butil::IOBuf frame) {
    IoCommand cmd;
    cmd.type = Type::kChannelSendFrame;
    cmd.connection = conn;
    cmd.correlation_id = correlation_id;
    cmd.payload = std::move(frame);
    return cmd;
  }

  static IoCommand ChannelClose(void* conn) {
    IoCommand cmd;
    cmd.type = Type::kChannelClose;
    cmd.connection = conn;
    return cmd;
  }
};
