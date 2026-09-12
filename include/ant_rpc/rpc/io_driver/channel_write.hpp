#pragma once

namespace ant_rpc::rpc::detail {

inline DetachedTask RpcChannelIoDriver::WriteFrame(std::shared_ptr<RpcChannelIoDriver> self, OutboundFrame frame) {
  while (!frame.buffer->empty() && self->state_->running.load(std::memory_order_acquire)) {
    const int written = co_await IOBufWriteAwaiter(self->context_, self->fd(), *frame.buffer, false);
    // BeginCloseOnIoThread() releases the IO queue's remaining reservation.
    // Do not release the copied front frame a second time after a close.
    if (!self->state_->running.load(std::memory_order_acquire)) {
      break;
    }
    if (written <= 0) {
      self->HandleTerminalFailureOnIoThread(RPC_ECONN_FAILED, "Failed to write RPC request");
      break;
    }
    const std::size_t consumed = static_cast<std::size_t>(written);
    frame.buffer->pop_front(consumed);
    self->ReleaseReservedOutboundBytes(consumed);
  }
  self->writing_ = false;
  if (!self->outbound_.empty()) {
    self->outbound_.pop_front();
  }
  self->StartNextWriteOnIoThread();
  self->TryFinishCloseOnIoThread();
}

inline void RpcChannelIoDriver::StartNextWriteOnIoThread() {
  if (writing_ || outbound_.empty() || !state_->running.load(std::memory_order_acquire)) {
    return;
  }
  writing_ = true;
  WriteFrame(shared_from_this(), outbound_.front());
}

}  // namespace ant_rpc::rpc::detail
