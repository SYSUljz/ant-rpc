#pragma once

#include <fcntl.h>

#include <functional>

#include <sys/socket.h>

#include "ant_server/context/context.hpp"
#include "ant_server/context/socket_handle.hpp"
#include "ant_server/type.hpp"

struct Acceptor final : IOHandler {
  Acceptor(Context& context, int listener, std::function<void(int)> callback)
      : context_(context), listener_(listener), callback_(std::move(callback)) {}
  void Start() {
    const int flags = fcntl(listener_, F_GETFL, 0);
    if (flags >= 0) {
      fcntl(listener_, F_SETFL, flags | O_NONBLOCK);
    }
    context_.RegisterReadable(listener_, this);
  }
  void on_complete() override {
    const int client = accept4(listener_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (client >= 0) {
      callback_(client);
    }
    Start();
  }

 private:
  Context& context_;
  int listener_;
  std::function<void(int)> callback_;
};
