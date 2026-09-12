#pragma once

#include <coroutine>
#include <cstring>

#include <sys/socket.h>

#include "ant_rpc/awaiter/io_uring_socket_awaiter.hpp"

class ConnectAwaiter final : public BaseAwaiter {
 public:
  ConnectAwaiter(Context& context, SocketHandle socket, const sockaddr* address, socklen_t length)
      : BaseAwaiter(context), socket(socket), length(length) {
    std::memcpy(&storage, address, length);
  }
  ConnectAwaiter(Context& context, int fd, const sockaddr* address, socklen_t length)
      : ConnectAwaiter(context, SocketHandle::Native(fd), address, length) {}
  bool await_ready() const noexcept { return false; }
  template <typename Promise>
  void await_suspend(std::coroutine_handle<Promise> coroutine) {
    handle = coroutine;
    CaptureStopToken(coroutine);
    auto* sqe = Sqe();
    io_uring_prep_connect(sqe, socket.value, reinterpret_cast<const sockaddr*>(&storage), length);
    io_uring_sqe_set_data(sqe, this);
    Submit();
    BindStopCallback();
  }
  int await_resume() const noexcept { return res_; }

 private:
  SocketHandle socket;
  sockaddr_storage storage {};
  socklen_t length {0};
};
