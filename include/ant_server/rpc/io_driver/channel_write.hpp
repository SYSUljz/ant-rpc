#pragma once

namespace ant_server::rpc::detail {

inline DetachedTask RpcChannelIoDriver::WriteFrame(std::shared_ptr<RpcChannelIoDriver> self, OutboundFrame frame) {
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

inline void RpcChannelIoDriver::StartNextWriteOnIoThread() {
  if (writing_ || outbound_.empty() || !state_->running.load(std::memory_order_acquire)) {
    return;
  }
  writing_ = true;
  WriteFrame(shared_from_this(), outbound_.front());
}

}  // namespace ant_server::rpc::detail
