#pragma once

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <utility>

#include <sys/socket.h>

#include "ant_server/awaiter/socket_awaiter.hpp"
#include "ant_server/context/context.hpp"
#include "ant_server/coroutine/task.hpp"
#include "ant_server/rpc/core/channel_state.hpp"
#include "ant_server/rpc/error_code.hpp"
#include "ant_server/scheduler/timer_keeper.hpp"
#include "ant_server/type.hpp"
#include "butil/iobuf.h"

namespace ant_server::rpc::detail {

// IO-thread-only coordinator for one channel. Connect, receive, and write
// operations live in their own headers below so each state machine can be
// reviewed independently.
class RpcChannelIoDriver : public IoCommandMailbox, public std::enable_shared_from_this<RpcChannelIoDriver> {
 public:
  struct OutboundFrame {
    uint64_t correlation_id;
    std::shared_ptr<butil::IOBuf> buffer;
  };

  struct ChannelCommand {
    enum class Type : uint8_t { kStart, kSendFrame, kClose };
    Type type;
    OutboundFrame frame {};

    static ChannelCommand Start() { return {Type::kStart, {}}; }
    static ChannelCommand SendFrame(OutboundFrame frame) { return {Type::kSendFrame, std::move(frame)}; }
    static ChannelCommand Close() { return {Type::kClose, {}}; }
  };

  RpcChannelIoDriver(Context& context, std::shared_ptr<ChannelState> state, int fd, const sockaddr* address,
                     socklen_t address_len, TimerKeeper& timer_keeper, std::chrono::milliseconds connect_timeout,
                     std::size_t max_frame_bytes, std::size_t max_outbound_bytes)
      : context_(context),
        state_(std::move(state)),
        fd_(fd),
        timer_keeper_(timer_keeper),
        connect_timeout_(connect_timeout),
        max_frame_bytes_(max_frame_bytes),
        max_outbound_bytes_(max_outbound_bytes),
        connect_address_len_(address_len) {
    std::memcpy(&connect_address_, address, address_len);
  }

  int fd() const noexcept { return fd_.load(std::memory_order_acquire); }
  void Start() { PostCommand(ChannelCommand::Start()); }

  // Reserve before publishing a command so the limit covers both the MPSC
  // mailbox and the IO-owned outbound queue.
  bool TryReserveOutboundBytes(std::size_t bytes) noexcept {
    if (bytes > max_outbound_bytes_) {
      return false;
    }
    std::size_t current = outbound_bytes_.load(std::memory_order_acquire);
    while (current <= max_outbound_bytes_ - bytes) {
      if (outbound_bytes_.compare_exchange_weak(current, current + bytes, std::memory_order_acq_rel,
                                                std::memory_order_acquire)) {
        return true;
      }
    }
    return false;
  }
  void ReleaseReservedOutboundBytes(std::size_t bytes) noexcept {
    outbound_bytes_.fetch_sub(bytes, std::memory_order_acq_rel);
  }
  // Requires a successful TryReserveOutboundBytes() for frame.buffer->size().
  void EnqueueReserved(OutboundFrame frame) { PostCommand(ChannelCommand::SendFrame(std::move(frame))); }
  void RequestClose();
  void WaitClosed();
  void DrainCommandsOnIoThread() override;

 private:
  void PostCommand(ChannelCommand command);
  void StartOnIoThread();
  void BeginCloseOnIoThread();
  void StartNextWriteOnIoThread();
  void FailAndClearOutboundOnIoThread(int error_code, const char* error_message);
  void TryFinishCloseOnIoThread();
  void FinishConnectOnIoThread(int result);
  void CancelPendingConnectOnIoThread();

  Task<int> AwaitConnectCompletion(std::shared_ptr<RpcChannelIoDriver> self);
  Task<int> AwaitConnectTimeout();
  Task<int> AsyncConnect(std::shared_ptr<RpcChannelIoDriver> self);
  DetachedTask ConnectOnIoThread(std::shared_ptr<RpcChannelIoDriver> self);
  DetachedTask ReceiveLoop(std::shared_ptr<RpcChannelIoDriver> self);
  DetachedTask WriteFrame(std::shared_ptr<RpcChannelIoDriver> self, OutboundFrame frame);

  Context& context_;
  std::shared_ptr<ChannelState> state_;
  std::atomic<int> fd_ {-1};
  TimerKeeper& timer_keeper_;
  std::chrono::milliseconds connect_timeout_;
  const std::size_t max_frame_bytes_;
  const std::size_t max_outbound_bytes_;
  std::atomic<std::size_t> outbound_bytes_ {0};
  sockaddr_storage connect_address_ {};
  socklen_t connect_address_len_ {0};
  MpscQueue<ChannelCommand> commands_;
  std::deque<OutboundFrame> outbound_;
  bool receiver_started_ {false};
  bool receiver_exited_ {false};
  bool writing_ {false};
  bool connect_started_ {false};
  bool connect_operation_exited_ {false};
  BaseAwaiter* active_connect_ {nullptr};
};

inline void RpcChannelIoDriver::RequestClose() {
  if (state_->running.exchange(false, std::memory_order_acq_rel)) {
    PostCommand(ChannelCommand::Close());
  }
}

inline void RpcChannelIoDriver::WaitClosed() {
  if (g_local_context == &context_) {
    return;
  }
  std::unique_lock<std::mutex> lock(state_->close_mu);
  state_->close_cv.wait(lock, [this] { return state_->closed; });
}

inline void RpcChannelIoDriver::PostCommand(ChannelCommand command) {
  commands_.Push(std::move(command));
  context_.Notify(shared_from_this());
}

inline void RpcChannelIoDriver::DrainCommandsOnIoThread() {
  commands_.Drain([this](ChannelCommand&& command) {
    switch (command.type) {
      case ChannelCommand::Type::kStart:
        StartOnIoThread();
        break;
      case ChannelCommand::Type::kSendFrame:
        if (!state_->running.load(std::memory_order_acquire)) {
          if (command.frame.buffer) {
            ReleaseReservedOutboundBytes(command.frame.buffer->size());
          }
          state_->slots.FailSlot(command.frame.correlation_id, RPC_ECONN_FAILED, "Connection closed");
          break;
        }
        outbound_.push_back(std::move(command.frame));
        StartNextWriteOnIoThread();
        break;
      case ChannelCommand::Type::kClose:
        BeginCloseOnIoThread();
        break;
    }
  });
}

inline void RpcChannelIoDriver::BeginCloseOnIoThread() {
  CancelPendingConnectOnIoThread();
  if (const int current_fd = fd(); current_fd >= 0) {
    shutdown(current_fd, SHUT_RDWR);
  }
  if (!receiver_started_) {
    receiver_exited_ = true;
  }
  state_->slots.FailAllActiveSlots(RPC_ECONN_FAILED, "Connection closed");
  FailAndClearOutboundOnIoThread(RPC_ECONN_FAILED, "Connection closed");
  TryFinishCloseOnIoThread();
}

inline void RpcChannelIoDriver::TryFinishCloseOnIoThread() {
  if (state_->running.load(std::memory_order_acquire) || !receiver_exited_ || writing_ ||
      (connect_started_ && !connect_operation_exited_)) {
    return;
  }
  if (const int current_fd = fd_.exchange(-1, std::memory_order_acq_rel); current_fd >= 0) {
    close(current_fd);
  }
  std::lock_guard<std::mutex> lock(state_->close_mu);
  state_->closed = true;
  state_->close_cv.notify_all();
}

inline void RpcChannelIoDriver::FinishConnectOnIoThread(int result) {
  std::lock_guard<std::mutex> lock(state_->connect_mu);
  if (!state_->connect_finished) {
    state_->connect_result = result;
    state_->connect_finished = true;
    state_->connect_cv.notify_all();
  }
}

inline void RpcChannelIoDriver::FailAndClearOutboundOnIoThread(int error_code, const char* error_message) {
  for (const OutboundFrame& frame : outbound_) {
    if (frame.buffer) {
      ReleaseReservedOutboundBytes(frame.buffer->size());
    }
    state_->slots.FailSlot(frame.correlation_id, error_code, error_message);
  }
  outbound_.clear();
}

}  // namespace ant_server::rpc::detail

#include "ant_server/rpc/io_driver/channel_connect.hpp"
#include "ant_server/rpc/io_driver/channel_receive.hpp"
#include "ant_server/rpc/io_driver/channel_write.hpp"
