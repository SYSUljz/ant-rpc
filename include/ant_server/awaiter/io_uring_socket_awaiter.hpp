#pragma once

#include <algorithm>
#include <atomic>
#include <coroutine>
#include <functional>
#include <memory>
#include <optional>

#include "ant_server/context/context.hpp"
#include "ant_server/context/socket_handle.hpp"
#include "ant_server/type.hpp"
#include "butil/iobuf.h"
#include "liburing.h"

struct BaseAwaiter : IOHandler {
  explicit BaseAwaiter(Context& context) : context_(context) {}
  std::coroutine_handle<> handle;
  template <typename Promise>
  void CaptureStopToken(std::coroutine_handle<Promise> coroutine) {
    if constexpr (requires(Promise& promise) { promise.get_stop_token(); }) {
      token_ = coroutine.promise().get_stop_token();
    }
  }
  void BindStopCallback() {
    if (!token_.stop_possible()) {
      return;
    }
    callback_.emplace(token_, [this] { SubmitCancel(this); });
  }
  void on_complete() override {
    if (handle) {
      handle.resume();
    }
  }

 protected:
  io_uring_sqe* Sqe() { return context_.GetSqe(); }
  void Submit() { context_.Submit(); }
  static void SetSocket(io_uring_sqe* sqe, SocketHandle socket) {
    if (socket.kind == SocketHandle::Kind::kRegisteredFile) {
      sqe->flags |= IOSQE_FIXED_FILE;
    }
  }
  void SubmitCancel(IOHandler* target) {
    auto* sqe = Sqe();
    io_uring_prep_cancel(sqe, target, 0);
    io_uring_sqe_set_data(sqe, nullptr);
    Submit();
  }
  Context& context_;

 private:
  std::stop_token token_;
  std::optional<std::stop_callback<std::function<void()>>> callback_;
};

namespace butil::iobuf {
IOBuf::Block* acquire_tls_block();
}

class UringReadAwaiter final : public BaseAwaiter {
 public:
  UringReadAwaiter(Context& context, SocketHandle socket, char* data, std::size_t size)
      : BaseAwaiter(context), socket(socket), data(data), size(size) {}
  UringReadAwaiter(Context& context, int fd, char* data, std::size_t size, bool fixed = false)
      : UringReadAwaiter(context, fixed ? SocketHandle::Registered(fd) : SocketHandle::Native(fd), data, size) {}
  UringReadAwaiter(Context& context, SocketHandle socket, butil::IOBuf& target)
      : BaseAwaiter(context), socket(socket), target(&target) {}
  UringReadAwaiter(Context& context, int fd, butil::IOBuf& target, bool fixed = false)
      : UringReadAwaiter(context, fixed ? SocketHandle::Registered(fd) : SocketHandle::Native(fd), target) {}
  bool await_ready() const noexcept { return false; }
  template <typename Promise>
  void await_suspend(std::coroutine_handle<Promise> coroutine) {
    handle = coroutine;
    CaptureStopToken(coroutine);
    if (target) {
      block = butil::iobuf::acquire_tls_block();
    }
    auto* sqe = Sqe();
    io_uring_prep_recv(sqe, socket.value, target ? block->data + block->size : data,
                       target ? block->left_space() : size, 0);
    SetSocket(sqe, socket);
    io_uring_sqe_set_data(sqe, this);
    Submit();
    BindStopCallback();
  }
  int await_resume() {
    if (target) {
      if (res_ > 0) {
        char* pointer = block->data + block->size;
        block->inc_ref();
        target->append_user_data(pointer, res_, [value = block](void*) { value->dec_ref(); });
      }
      block->dec_ref();
    }
    return res_;
  }

 private:
  SocketHandle socket;
  char* data {nullptr};
  std::size_t size {0};
  butil::IOBuf* target {nullptr};
  butil::IOBuf::Block* block {nullptr};
};

class UringWriteAwaiter final : public BaseAwaiter {
 public:
  UringWriteAwaiter(Context& context, SocketHandle socket, const char* data, std::size_t size)
      : BaseAwaiter(context), socket(socket), data(data), size(size) {}
  UringWriteAwaiter(Context& context, int fd, const char* data, std::size_t size, bool fixed = false)
      : UringWriteAwaiter(context, fixed ? SocketHandle::Registered(fd) : SocketHandle::Native(fd), data, size) {}
  bool await_ready() const noexcept { return false; }
  template <typename Promise>
  void await_suspend(std::coroutine_handle<Promise> coroutine) {
    handle = coroutine;
    CaptureStopToken(coroutine);
    auto* sqe = Sqe();
    io_uring_prep_send(sqe, socket.value, data, size, 0);
    SetSocket(sqe, socket);
    io_uring_sqe_set_data(sqe, this);
    Submit();
    BindStopCallback();
  }
  int await_resume() const noexcept { return res_; }

 private:
  SocketHandle socket;
  const char* data;
  std::size_t size;
};

class UringIOBufWriteAwaiter final : public BaseAwaiter {
 public:
  UringIOBufWriteAwaiter(Context& context, SocketHandle socket, const butil::IOBuf& source)
      : BaseAwaiter(context), socket(socket), source(source) {
    Init();
  }
  UringIOBufWriteAwaiter(Context& context, int fd, const butil::IOBuf& source, bool fixed = false)
      : UringIOBufWriteAwaiter(context, fixed ? SocketHandle::Registered(fd) : SocketHandle::Native(fd), source) {}
  bool await_ready() const noexcept { return count == 0; }
  template <typename Promise>
  void await_suspend(std::coroutine_handle<Promise> coroutine) {
    handle = coroutine;
    CaptureStopToken(coroutine);
    auto* sqe = Sqe();
    io_uring_prep_writev(sqe, socket.value, iov.get(), static_cast<int>(count), 0);
    SetSocket(sqe, socket);
    io_uring_sqe_set_data(sqe, this);
    Submit();
    BindStopCallback();
  }
  int await_resume() const noexcept { return res_; }

 private:
  void Init() {
    count = std::min(source.backing_block_num(), std::size_t {64});
    if (!count) {
      return;
    }
    iov = std::make_unique<iovec[]>(count);
    for (std::size_t i = 0; i < count; ++i) {
      auto b = source.backing_block(i);
      iov[i] = {const_cast<char*>(b.data()), b.size()};
    }
  }
  SocketHandle socket;
  const butil::IOBuf& source;
  std::unique_ptr<iovec[]> iov;
  std::size_t count {0};
};

using ReadAwaiter = UringReadAwaiter;
using IOBufReadAwaiter = UringReadAwaiter;
using WriteAwaiter = UringWriteAwaiter;
using IOBufWriteAwaiter = UringIOBufWriteAwaiter;

inline void ShutdownSocket(Context& context, SocketHandle socket, int how) {
  auto* sqe = context.GetSqe();
  io_uring_prep_shutdown(sqe, socket.value, how);
  if (socket.kind == SocketHandle::Kind::kRegisteredFile) {
    sqe->flags |= IOSQE_FIXED_FILE;
  }
  io_uring_sqe_set_data(sqe, nullptr);
  context.Submit();
}
inline void CloseSocket(Context& context, SocketHandle socket) {
  auto* sqe = context.GetSqe();
  if (socket.kind == SocketHandle::Kind::kRegisteredFile) {
    io_uring_prep_close_direct(sqe, socket.value);
  } else {
    io_uring_prep_close(sqe, socket.value);
  }
  io_uring_sqe_set_data(sqe, nullptr);
  context.Submit();
}
inline constexpr SocketHandle AcceptedSocket(int index) { return SocketHandle::Registered(index); }
inline void CancelSocketOperation(Context& context, IOHandler* handler) {
  if (!handler) {
    return;
  }
  auto* sqe = context.GetSqe();
  io_uring_prep_cancel(sqe, handler, 0);
  io_uring_sqe_set_data(sqe, nullptr);
  context.Submit();
}
