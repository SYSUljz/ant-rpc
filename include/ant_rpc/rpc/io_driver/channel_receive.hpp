#pragma once

#include "ant_rpc/rpc/protocol.hpp"

namespace ant_rpc::rpc::detail {

inline DetachedTask RpcChannelIoDriver::ReceiveLoop(std::shared_ptr<RpcChannelIoDriver> self) {
  butil::IOBuf recv_buffer;
  while (self->state_->running.load(std::memory_order_acquire)) {
    ReadAwaiter read_awaiter(self->context_, self->fd(), recv_buffer, false);
    self->active_read_ = &read_awaiter;
    const int bytes_read = co_await read_awaiter;
    self->active_read_ = nullptr;
    if (bytes_read <= 0) {
      self->receiver_exited_ = true;
      if (self->state_->running.load(std::memory_order_acquire)) {
        self->HandleTerminalFailureOnIoThread(
            RPC_ECONN_FAILED, bytes_read == 0 ? "RPC peer closed connection" : "Failed to read RPC response");
      } else {
        self->TryFinishCloseOnIoThread();
      }
      co_return;
    }
    while (true) {
      FrameParseResult result = TryParseRpcFrame(recv_buffer, self->max_frame_bytes_);
      if (result.status == FrameParseStatus::NEED_MORE_DATA) {
        break;
      }
      if (result.status != FrameParseStatus::SUCCESS) {
        self->receiver_exited_ = true;
        self->HandleTerminalFailureOnIoThread(RPC_ECONN_FAILED, "Invalid RPC response frame");
        co_return;
      }
      if (result.meta.msg_type() != RPC_RESPONSE) {
        self->receiver_exited_ = true;
        self->HandleTerminalFailureOnIoThread(RPC_ECONN_FAILED, "Received a non-response RPC frame on client channel");
        co_return;
      }
      recv_buffer.pop_front(result.total_frame_bytes);
      self->state_->slots.CompleteResponseSlot(result.meta.correlation_id(), result.body_iobuf, std::move(result.meta),
                                               result.attachment_iobuf);
    }
  }
  self->receiver_exited_ = true;
  if (self->state_->running.load(std::memory_order_acquire)) {
    self->HandleTerminalFailureOnIoThread(RPC_ECONN_FAILED, "Connection closed while receiving response");
  } else {
    self->TryFinishCloseOnIoThread();
  }
}

}  // namespace ant_rpc::rpc::detail
