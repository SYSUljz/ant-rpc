#pragma once

#include <coroutine>
#include <cstring>

#include <sys/socket.h>

#include "ant_server/awaiter/socket_awaiter.hpp"

// Raw-fd connect primitive. Its CQE resumes on the Context which owns the
// io_uring instance, exactly like ReadAwaiter and WriteAwaiter.
struct ConnectAwaiter : public BaseAwaiter {
  ConnectAwaiter(Context& context, int fd, const sockaddr* address, socklen_t address_len)
      : BaseAwaiter(context), fd_(fd), address_len_(address_len) {
    std::memcpy(&address_, address, address_len_);
  }

  bool await_ready() const noexcept { return false; }

  template <typename PromiseType>
  void await_suspend(std::coroutine_handle<PromiseType> handle) {
    this->handle = handle;
    if constexpr (requires(PromiseType& promise) { promise.get_stop_token(); }) {
      this->token_ = handle.promise().get_stop_token();
    }
    socket_service_.SubmitConnect(fd_, reinterpret_cast<const sockaddr*>(&address_), address_len_, this);
    bind_stop_callback();
  }

  int await_resume() const noexcept { return this->res_; }
  void on_cancel() override { socket_service_.SubmitCancel(this); }

 private:
  int fd_;
  sockaddr_storage address_ {};
  socklen_t address_len_ {0};
};
