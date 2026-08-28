#pragma once

#include "ant_server/rpc/protocol.hpp"

namespace ant_server::rpc::detail {

inline DetachedTask RpcChannelIoDriver::ReceiveLoop(std::shared_ptr<RpcChannelIoDriver> self) {
  butil::IOBuf recv_buffer;
  while (self->state_->running.load(std::memory_order_acquire)) {
    const int bytes_read = co_await ReadAwaiter(self->context_, self->fd(), recv_buffer, false);
    if (bytes_read <= 0) {
      self->receiver_exited_ = true;
      self->HandleTerminalFailureOnIoThread(
          RPC_ECONN_FAILED, bytes_read == 0 ? "RPC peer closed connection" : "Failed to read RPC response");
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
      recv_buffer.pop_front(result.total_frame_bytes);
      self->state_->slots.CompleteSlot(result.meta.correlation_id(), result.body_iobuf, std::move(result.meta),
                                       result.attachment_iobuf);
    }
  }
  self->receiver_exited_ = true;
  self->HandleTerminalFailureOnIoThread(RPC_ECONN_FAILED, "Connection closed while receiving response");
}

}  // namespace ant_server::rpc::detail
