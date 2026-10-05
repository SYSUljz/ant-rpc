#pragma once

namespace ant_rpc::rpc::detail {

inline DetachedTask RpcChannelIoDriver::WriteFrame(std::shared_ptr<RpcChannelIoDriver> self, OutboundFrame frame) {
  while (!frame.buffer.empty() && self->state_->running.load(std::memory_order_acquire)) {
    IOBufWriteAwaiter write_awaiter(self->context_, self->fd(), frame.buffer, false);
    self->active_write_ = &write_awaiter;
    const int written = co_await write_awaiter;
    self->active_write_ = nullptr;
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
    frame.buffer.pop_front(consumed);
    self->ReleaseReservedOutboundBytes(consumed);
  }
  self->writing_ = false;
  self->StartNextWriteOnIoThread();
  self->TryFinishCloseOnIoThread();
}

inline void RpcChannelIoDriver::StartNextWriteOnIoThread() {
  if (writing_ || outbound_.empty() || !state_->running.load(std::memory_order_acquire)) {
    return;
  }
  writing_ = true;
  OutboundFrame frame = std::move(outbound_.front());
  outbound_.pop_front();

  // Batch-merge subsequent queued frames to send them in a single write/writev
  static constexpr std::size_t kMaxBatchBytes = 64 * 1024;
  while (!outbound_.empty() && frame.buffer.size() < kMaxBatchBytes) {
    frame.buffer.append(outbound_.front().buffer);
    outbound_.pop_front();
  }

  WriteFrame(shared_from_this(), std::move(frame));
}

}  // namespace ant_rpc::rpc::detail
