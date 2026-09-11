#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <coroutine>
#include <functional>
#include <memory>
#include <optional>

#include <sys/socket.h>
#include <sys/uio.h>

#include "ant_server/context/context.hpp"
#include "ant_server/context/socket_handle.hpp"
#include "ant_server/type.hpp"
#include "butil/iobuf.h"
#if defined(ANT_SERVER_ENABLE_TEST_SOCKET_FAULT_INJECTION)
#include "ant_server/testing/socket_fault_injector.hpp"
#endif

namespace ant_server::epoll_detail {
class BaseAwaiter;
struct CancelAnchor {
  std::atomic<BaseAwaiter*> awaiter {nullptr};
};
struct CancelMailbox final : IoCommandMailbox {
  explicit CancelMailbox(std::shared_ptr<CancelAnchor> anchor) : anchor(std::move(anchor)) {}
  void DrainCommandsOnIoThread() override;
  std::shared_ptr<CancelAnchor> anchor;
};

class BaseAwaiter : public IOHandler {
 public:
  explicit BaseAwaiter(Context& context) : context(context), anchor(std::make_shared<CancelAnchor>()) {
    anchor->awaiter.store(this, std::memory_order_release);
  }
  virtual ~BaseAwaiter() { anchor->awaiter.store(nullptr, std::memory_order_release); }
  void BindStopCallback() {
    if (!token.stop_possible()) {
      return;
    }
    callback.emplace(token, [state = anchor, &ctx = context] { ctx.Notify(std::make_shared<CancelMailbox>(state)); });
  }
  template <typename Promise>
  void CaptureStopToken(std::coroutine_handle<Promise> coroutine) {
    if constexpr (requires(Promise& promise) { promise.get_stop_token(); }) {
      token = coroutine.promise().get_stop_token();
    }
  }
  void Finish() {
    if (!done.exchange(true, std::memory_order_acq_rel) && handle) {
      handle.resume();
    }
  }
  void CancelOnIoThread() {
    if (done.load(std::memory_order_acquire)) {
      return;
    }
    context.CancelWait(this);
    prepare_complete(-ECANCELED, 0);
    Finish();
  }
  Context& context;
  std::coroutine_handle<> handle;

 private:
  std::stop_token token;
  std::optional<std::stop_callback<std::function<void()>>> callback;
  std::atomic<bool> done {false};
  std::shared_ptr<CancelAnchor> anchor;
};
inline void CancelMailbox::DrainCommandsOnIoThread() {
  if (auto* live = anchor->awaiter.load(std::memory_order_acquire)) {
    live->CancelOnIoThread();
  }
}

inline void SetNonBlocking(int fd) {
  const int flags = fcntl(fd, F_GETFL, 0);
  if (flags >= 0) {
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  }
}
}  // namespace ant_server::epoll_detail

namespace butil::iobuf {
IOBuf::Block* acquire_tls_block();
}

class EpollReadAwaiter final : public ant_server::epoll_detail::BaseAwaiter {
 public:
  EpollReadAwaiter(Context& context, SocketHandle socket, char* data, std::size_t size)
      : BaseAwaiter(context), socket(socket), data(data), size(size) {}
  EpollReadAwaiter(Context& context, int fd, char* data, std::size_t size, bool = false)
      : EpollReadAwaiter(context, SocketHandle::Native(fd), data, size) {}
  EpollReadAwaiter(Context& context, SocketHandle socket, butil::IOBuf& target)
      : BaseAwaiter(context), socket(socket), target(&target) {}
  EpollReadAwaiter(Context& context, int fd, butil::IOBuf& target, bool = false)
      : EpollReadAwaiter(context, SocketHandle::Native(fd), target) {}
  bool await_ready() const noexcept { return false; }
  template <typename Promise>
  bool await_suspend(std::coroutine_handle<Promise> coroutine) {
    handle = coroutine;
    CaptureStopToken(coroutine);
    ant_server::epoll_detail::SetNonBlocking(socket.value);
    if (target) {
      block = butil::iobuf::acquire_tls_block();
    }
    if (!TryRead()) {
      return false;
    }
    BindStopCallback();
    return true;
  }
  int await_resume() {
    if (target) {
      CommitBlock();
    }
    return res_;
  }
  void on_complete() override {
    if (!TryRead()) {
      Finish();
    }
  }

 private:
  bool TryRead() {
    const ssize_t bytes =
        recv(socket.value, target ? block->data + block->size : data, target ? block->left_space() : size, 0);
    if (bytes >= 0) {
      prepare_complete(static_cast<int>(bytes), 0);
      return false;
    }
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      prepare_complete(-errno, 0);
      return false;
    }
    context.RegisterReadable(socket.value, this);
    return true;
  }
  void CommitBlock() {
    if (res_ > 0) {
      char* pointer = block->data + block->size;
      block->inc_ref();
      target->append_user_data(pointer, res_, [value = block](void*) { value->dec_ref(); });
    }
    block->dec_ref();
  }
  SocketHandle socket;
  char* data {nullptr};
  std::size_t size {0};
  butil::IOBuf* target {nullptr};
  butil::IOBuf::Block* block {nullptr};
};

class EpollWriteAwaiter final : public ant_server::epoll_detail::BaseAwaiter {
 public:
  EpollWriteAwaiter(Context& context, SocketHandle socket, const char* data, std::size_t size)
      : BaseAwaiter(context), socket(socket), data(data), size(size) {}
  EpollWriteAwaiter(Context& context, int fd, const char* data, std::size_t size, bool = false)
      : EpollWriteAwaiter(context, SocketHandle::Native(fd), data, size) {}
  bool await_ready() const noexcept { return false; }
  template <typename Promise>
  bool await_suspend(std::coroutine_handle<Promise> coroutine) {
    handle = coroutine;
    CaptureStopToken(coroutine);
    ant_server::epoll_detail::SetNonBlocking(socket.value);
    if (!TryWrite()) {
      return false;
    }
    BindStopCallback();
    return true;
  }
  int await_resume() const noexcept { return res_; }
  void on_complete() override {
    if (!TryWrite()) {
      Finish();
    }
  }

 private:
  bool TryWrite() {
    const ssize_t bytes = send(socket.value, data, size, MSG_NOSIGNAL);
    if (bytes >= 0) {
      prepare_complete(static_cast<int>(bytes), 0);
      return false;
    }
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      prepare_complete(-errno, 0);
      return false;
    }
    context.RegisterWritable(socket.value, this);
    return true;
  }
  SocketHandle socket;
  const char* data;
  std::size_t size;
};

class EpollIOBufWriteAwaiter final : public ant_server::epoll_detail::BaseAwaiter {
 public:
  EpollIOBufWriteAwaiter(Context& context, SocketHandle socket, const butil::IOBuf& source)
      : BaseAwaiter(context), socket(socket), source(source) {
    InitIov();
  }
  EpollIOBufWriteAwaiter(Context& context, int fd, const butil::IOBuf& source, bool = false)
      : EpollIOBufWriteAwaiter(context, SocketHandle::Native(fd), source) {}
  bool await_ready() const noexcept { return count == 0; }
  template <typename Promise>
  bool await_suspend(std::coroutine_handle<Promise> coroutine) {
    handle = coroutine;
    CaptureStopToken(coroutine);
    ant_server::epoll_detail::SetNonBlocking(socket.value);
    if (!TryWrite()) {
      return false;
    }
    BindStopCallback();
    return true;
  }
  int await_resume() const noexcept { return res_; }
  void on_complete() override {
    if (!TryWrite()) {
      Finish();
    }
  }

 private:
  void InitIov() {
    const std::size_t capacity = std::min(source.backing_block_num(), std::size_t {64});
    if (!capacity) {
      return;
    }
    iov = std::make_unique<iovec[]>(capacity);
    std::size_t max_bytes = 0;
#if defined(ANT_SERVER_ENABLE_TEST_SOCKET_FAULT_INJECTION)
    max_bytes = ant_server::testing::SocketFaultInjector::MaxWriteBytes();
#endif
    std::size_t remaining = max_bytes;
    for (std::size_t i = 0; i < capacity && (max_bytes == 0 || remaining > 0); ++i) {
      auto b = source.backing_block(i);
      const std::size_t bytes = max_bytes == 0 ? b.size() : std::min(b.size(), remaining);
      if (bytes == 0) {
        break;
      }
      iov[count++] = {const_cast<char*>(b.data()), bytes};
      if (max_bytes != 0) {
        remaining -= bytes;
      }
    }
  }
  bool TryWrite() {
    const ssize_t bytes = writev(socket.value, iov.get(), static_cast<int>(count));
    if (bytes >= 0) {
      prepare_complete(static_cast<int>(bytes), 0);
      return false;
    }
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      prepare_complete(-errno, 0);
      return false;
    }
    context.RegisterWritable(socket.value, this);
    return true;
  }
  SocketHandle socket;
  const butil::IOBuf& source;
  std::unique_ptr<iovec[]> iov;
  std::size_t count {0};
};

using ReadAwaiter = EpollReadAwaiter;
using IOBufReadAwaiter = EpollReadAwaiter;
using WriteAwaiter = EpollWriteAwaiter;
using IOBufWriteAwaiter = EpollIOBufWriteAwaiter;

inline void ShutdownSocket(Context&, SocketHandle socket, int how) {
  if (socket.valid()) {
    shutdown(socket.value, how);
  }
}
inline void CloseSocket(Context&, SocketHandle socket) {
  if (socket.valid()) {
    close(socket.value);
  }
}
inline constexpr SocketHandle AcceptedSocket(int fd) { return SocketHandle::Native(fd); }
inline void CancelSocketOperation(Context& context, IOHandler* handler) {
  if (handler && context.CancelWait(handler)) {
    handler->prepare_complete(-ECANCELED, 0);
    handler->on_complete();
  }
}
