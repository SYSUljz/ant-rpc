#pragma once

#include "ant_server/rpc/protocol.hpp"

namespace ant_server::rpc::detail {

inline DetachedTask RpcChannelIoDriver::ReceiveLoop(std::shared_ptr<RpcChannelIoDriver> self) {
  butil::IOBuf recv_buffer;
  while (self->state_->running.load(std::memory_order_acquire)) {
    const int bytes_read = co_await ReadAwaiter(self->context_, self->fd(), recv_buffer, false);
    if (bytes_read <= 0) {
      break;
    }
    while (true) {
      FrameParseResult result = TryParseRpcFrame(recv_buffer, self->max_frame_bytes_);
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
  self->FailAndClearOutboundOnIoThread(RPC_ECONN_FAILED, "Connection closed");
  self->TryFinishCloseOnIoThread();
}

}  // namespace ant_server::rpc::detail
