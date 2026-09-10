#pragma once

#include <fcntl.h>

#include <cerrno>
#include <coroutine>
#include <cstring>

#include <sys/socket.h>

#include "ant_server/awaiter/epoll_socket_awaiter.hpp"

class ConnectAwaiter final : public ant_server::epoll_detail::BaseAwaiter {
 public:
  ConnectAwaiter(Context& context, SocketHandle socket, const sockaddr* address, socklen_t length)
      : BaseAwaiter(context), socket(socket), length(length) {
    std::memcpy(&storage, address, length);
  }
  bool await_ready() const noexcept { return false; }
  template <typename Promise>
  bool await_suspend(std::coroutine_handle<Promise> coroutine) {
    handle = coroutine;
    CaptureStopToken(coroutine);
    ant_server::epoll_detail::SetNonBlocking(socket.value);
    if (!StartConnect()) {
      return false;
    }
    BindStopCallback();
    return true;
  }
  int await_resume() const noexcept { return res_; }
  void on_complete() override {
    int error = 0;
    socklen_t error_size = sizeof(error);
    if (getsockopt(socket.value, SOL_SOCKET, SO_ERROR, &error, &error_size) != 0) {
      error = errno;
    }
    prepare_complete(error == 0 ? 0 : -error, 0);
    Finish();
  }

 private:
  bool StartConnect() {
    if (connect(socket.value, reinterpret_cast<const sockaddr*>(&storage), length) == 0) {
      prepare_complete(0, 0);
      return false;
    }
    if (errno == EINPROGRESS || errno == EALREADY || errno == EAGAIN) {
      context.RegisterWritable(socket.value, this);
      return true;
    }
    prepare_complete(-errno, 0);
    return false;
  }
  SocketHandle socket;
  sockaddr_storage storage {};
  socklen_t length {0};
};
