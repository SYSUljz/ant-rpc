#pragma once

#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <utility>

#include <sys/socket.h>

#include "ant_server/awaiter/socket_awaiter.hpp"
#include "ant_server/context/context.hpp"
#include "ant_server/rpc/core/channel_state.hpp"
#include "ant_server/rpc/protocol.hpp"
#include "ant_server/type.hpp"
#include "butil/iobuf.h"

namespace ant_server::rpc::detail {

// IO-thread-only state machine for one connected RPC channel.
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

  RpcChannelIoDriver(Context& context, std::shared_ptr<ChannelState> state, int fd)
      : context_(context), state_(std::move(state)), fd_(fd) {}

  int fd() const noexcept { return fd_.load(std::memory_order_acquire); }
  void Start() { PostCommand(ChannelCommand::Start()); }
  void Enqueue(OutboundFrame frame) { PostCommand(ChannelCommand::SendFrame(std::move(frame))); }

  void RequestClose() {
    if (state_->running.exchange(false, std::memory_order_acq_rel)) {
      PostCommand(ChannelCommand::Close());
    }
  }

  void WaitClosed() {
    if (g_local_context == &context_) {
      return;
    }
    std::unique_lock<std::mutex> lock(state_->close_mu);
    state_->close_cv.wait(lock, [this] { return state_->closed; });
  }

  void DrainCommandsOnIoThread() override {
    commands_.Drain([this](ChannelCommand&& command) {
      switch (command.type) {
        case ChannelCommand::Type::kStart:
          StartOnIoThread();
          break;
        case ChannelCommand::Type::kSendFrame:
          if (!state_->running.load(std::memory_order_acquire)) {
            state_->slots.TimeoutSlot(command.frame.correlation_id, "Connection closed");
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

 private:
  void PostCommand(ChannelCommand command) {
    commands_.Push(std::move(command));
    context_.Notify(shared_from_this());
  }

  void StartOnIoThread() {
    if (!state_->running.load(std::memory_order_acquire)) {
      receiver_exited_ = true;
      TryFinishCloseOnIoThread();
      return;
    }
    receiver_started_ = true;
    ReceiveLoop(shared_from_this());
  }

  void BeginCloseOnIoThread() {
    if (const int current_fd = fd(); current_fd >= 0) {
      shutdown(current_fd, SHUT_RDWR);
    }
    if (!receiver_started_) {
      receiver_exited_ = true;
    }
    outbound_.clear();
    TryFinishCloseOnIoThread();
  }

  DetachedTask ReceiveLoop(std::shared_ptr<RpcChannelIoDriver> self) {
    butil::IOBuf recv_buffer;
    while (self->state_->running.load(std::memory_order_acquire)) {
      const int bytes_read = co_await ReadAwaiter(self->context_, self->fd(), recv_buffer, false);
      if (bytes_read <= 0) {
        break;
      }
      while (true) {
        FrameParseResult result = TryParseRpcFrame(recv_buffer);
        if (result.status == FrameParseStatus::NEED_MORE_DATA) {
          break;
        }
        if (result.status != FrameParseStatus::SUCCESS) {
          recv_buffer.clear();
          break;
        }
        recv_buffer.pop_front(result.total_frame_bytes);
        self->state_->slots.CompleteSlot(result.meta.correlation_id(), result.body_iobuf, std::move(result.meta),
                                         result.attachment_iobuf);
      }
    }
    self->state_->running.store(false, std::memory_order_release);
    self->receiver_exited_ = true;
    self->outbound_.clear();
    self->TryFinishCloseOnIoThread();
  }

  DetachedTask WriteFrame(std::shared_ptr<RpcChannelIoDriver> self, OutboundFrame frame) {
    while (!frame.buffer->empty() && self->state_->running.load(std::memory_order_acquire)) {
      const int written = co_await IOBufWriteAwaiter(self->context_, self->fd(), *frame.buffer, false);
      if (written <= 0) {
        self->state_->slots.TimeoutSlot(frame.correlation_id, "Failed to send RPC request");
        self->state_->running.store(false, std::memory_order_release);
        if (const int current_fd = self->fd(); current_fd >= 0) {
          shutdown(current_fd, SHUT_RDWR);
        }
        break;
      }
      frame.buffer->pop_front(static_cast<size_t>(written));
    }
    self->writing_ = false;
    if (!self->outbound_.empty()) {
      self->outbound_.pop_front();
    }
    self->StartNextWriteOnIoThread();
    self->TryFinishCloseOnIoThread();
  }

  void StartNextWriteOnIoThread() {
    if (writing_ || outbound_.empty() || !state_->running.load(std::memory_order_acquire)) {
      return;
    }
    writing_ = true;
    WriteFrame(shared_from_this(), outbound_.front());
  }

  void TryFinishCloseOnIoThread() {
    if (state_->running.load(std::memory_order_acquire) || !receiver_exited_ || writing_) {
      return;
    }
    if (const int current_fd = fd_.exchange(-1, std::memory_order_acq_rel); current_fd >= 0) {
      close(current_fd);
    }
    std::lock_guard<std::mutex> lock(state_->close_mu);
    state_->closed = true;
    state_->close_cv.notify_all();
  }

  Context& context_;
  std::shared_ptr<ChannelState> state_;
  std::atomic<int> fd_ {-1};
  MpscQueue<ChannelCommand> commands_;
  std::deque<OutboundFrame> outbound_;
  bool receiver_started_ {false};
  bool receiver_exited_ {false};
  bool writing_ {false};
};

}  // namespace ant_server::rpc::detail
