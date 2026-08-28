#pragma once

#include <cerrno>

#include "ant_server/awaiter/connect_awaiter.hpp"
#include "ant_server/awaiter/context_timer_awaiter.hpp"
#include "ant_server/coroutine/operator/when_any.hpp"

namespace ant_server::rpc::detail {

inline void RpcChannelIoDriver::StartOnIoThread() {
  if (!state_->running.load(std::memory_order_acquire)) {
    FinishConnectOnIoThread(-ECANCELED);
    receiver_exited_ = true;
    TryFinishCloseOnIoThread();
    return;
  }
  connect_started_ = true;
  ConnectOnIoThread(shared_from_this());
}

inline Task<int> RpcChannelIoDriver::AwaitConnectCompletion(std::shared_ptr<RpcChannelIoDriver> self) {
  ConnectAwaiter connect_awaiter(self->context_, self->fd(), reinterpret_cast<const sockaddr*>(&self->connect_address_),
                                 self->connect_address_len_);
  self->active_connect_ = &connect_awaiter;
  const int result = co_await connect_awaiter;
  self->active_connect_ = nullptr;
  self->connect_operation_exited_ = true;
  self->TryFinishCloseOnIoThread();
  co_return result;
}

inline Task<int> RpcChannelIoDriver::AwaitConnectTimeout() {
  co_await ContextTimerAwaiter(context_, timer_keeper_, connect_timeout_);
  co_return -ETIMEDOUT;
}

inline Task<int> RpcChannelIoDriver::AsyncConnect(std::shared_ptr<RpcChannelIoDriver> self) {
  auto result = co_await when_any(self->AwaitConnectCompletion(self), self->AwaitConnectTimeout());
  co_return result.index == 0 ? std::get<0>(result.value) : std::get<1>(result.value);
}

inline DetachedTask RpcChannelIoDriver::ConnectOnIoThread(std::shared_ptr<RpcChannelIoDriver> self) {
  const int connect_result = co_await self->AsyncConnect(self);
  if (connect_result != 0 || !self->state_->running.load(std::memory_order_acquire)) {
    self->HandleTerminalFailureOnIoThread(RPC_ECONN_FAILED, "Failed to connect RPC channel");
    self->FinishConnectOnIoThread(connect_result == 0 ? -ECANCELED : connect_result);
    self->receiver_exited_ = true;
    self->TryFinishCloseOnIoThread();
    co_return;
  }
  self->FinishConnectOnIoThread(0);
  self->receiver_started_ = true;
  self->ReceiveLoop(self);
}

inline void RpcChannelIoDriver::CancelPendingConnectOnIoThread() {
  if (active_connect_) {
    context_.UseService<IOuringSocketService>().SubmitCancel(active_connect_);
  }
}

}  // namespace ant_server::rpc::detail
