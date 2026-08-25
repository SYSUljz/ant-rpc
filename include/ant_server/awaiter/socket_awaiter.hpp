#ifndef ANT_AWAITER_HPP
#define ANT_AWAITER_HPP

#include <liburing.h>

#include <chrono>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <memory>

#include "ant_server/context/context.hpp"
#include "ant_server/context/service.hpp"
#include "ant_server/scheduler/scheduler.hpp"
#include "ant_server/type.hpp"
#include "butil/iobuf.h"

// Socket awaiters are IO primitives: their CQE continuation always resumes on
// the Context thread that consumed the CQE. Moving into business execution is
// an explicit co_await resume_on(...) decision made at a runtime boundary.
struct BaseAwaiter : public IOHandler {
  explicit BaseAwaiter(Context& ctx = GetCurrentContext()) : socket_service_(ctx.UseService<IOuringSocketService>()) {}
  std::coroutine_handle<> handle;

  IOuringSocketService& socket_service_;
  std::stop_token token_;
  std::atomic<CancelState> state_;
  std::optional<std::stop_callback<std::function<void()>>> cb_;

  void bind_stop_callback() {
    if (!token_.stop_possible()) {
      return;
    }
    cb_.emplace(token_, [this] {
      CancelState expected = CancelState::pending;
      if (state_.compare_exchange_strong(expected, CancelState::canceled, std::memory_order_acq_rel)) {
        on_cancel();
      }
    });
  }
  virtual void on_cancel() {};
  void on_complete() override {
    if (!handle) {
      return;
    }
    handle.resume();
  }
};

namespace butil::iobuf {
IOBuf::Block* acquire_tls_block();
}  // namespace butil::iobuf

// Unified high-performance ReadAwaiter (supports raw buffer reads & zero-copy butil::IOBuf reads)
struct ReadAwaiter : public BaseAwaiter {
  // 1. Raw buffer mode
  ReadAwaiter(Context& ctx, int client_fd, char* buffer, int buffer_size, bool is_fixed = true)
      : BaseAwaiter(ctx), fd_(client_fd), buffer_(buffer), len_(buffer_size), is_fixed_(is_fixed) {}
  ReadAwaiter(int client_fd, char* buffer, int buffer_size, bool is_fixed = true, Context& ctx = GetCurrentContext())
      : BaseAwaiter(ctx), fd_(client_fd), buffer_(buffer), len_(buffer_size), is_fixed_(is_fixed) {}

  // 2. IOBuf zero-copy mode (optimal implementation, direct to butil TLS Block)
  ReadAwaiter(Context& ctx, int client_fd, butil::IOBuf& target_iobuf, bool is_fixed = true)
      : BaseAwaiter(ctx), fd_(client_fd), target_iobuf_(&target_iobuf), is_fixed_(is_fixed) {}
  ReadAwaiter(int client_fd, butil::IOBuf& target_iobuf, bool is_fixed = true, Context& ctx = GetCurrentContext())
      : BaseAwaiter(ctx), fd_(client_fd), target_iobuf_(&target_iobuf), is_fixed_(is_fixed) {}

  bool await_ready() { return false; }

  template <typename PromiseType>
  void await_suspend(std::coroutine_handle<PromiseType> handle) {
    this->handle = handle;
    if constexpr (requires(PromiseType& p) { p.get_stop_token(); }) {
      this->token_ = handle.promise().get_stop_token();
    }
    if (target_iobuf_) {
      block_ = butil::iobuf::acquire_tls_block();
      char* write_ptr = block_->data + block_->size;
      std::size_t max_bytes = block_->left_space();
      socket_service_.SubmitRead(fd_, write_ptr, max_bytes, static_cast<IOHandler*>(this), is_fixed_);
    } else {
      socket_service_.SubmitRead(fd_, buffer_, len_, static_cast<IOHandler*>(this), is_fixed_);
    }
    bind_stop_callback();
  }

  int await_resume() {
    int read_bytes = this->res_;
    if (target_iobuf_) {
      if (read_bytes > 0) {
        char* data_ptr = block_->data + block_->size;
        block_->inc_ref();
        target_iobuf_->append_user_data(data_ptr, read_bytes, [b = block_](void*) { b->dec_ref(); });
        block_->dec_ref();
      } else {
        block_->dec_ref();
      }
    }
    return read_bytes;
  }

  void on_cancel() override { socket_service_.SubmitCancel(this); }

 private:
  int fd_;
  char* buffer_ {nullptr};
  std::size_t len_ {0};
  butil::IOBuf* target_iobuf_ {nullptr};
  bool is_fixed_ {true};
  butil::IOBuf::Block* block_ {nullptr};
};

using IOBufReadAwaiter = ReadAwaiter;

struct WriteAwaiter : public BaseAwaiter {
  WriteAwaiter(Context& ctx, int client_fd, char* buffer, int buffer_size, bool is_fixed = true)
      : BaseAwaiter(ctx), fd_(client_fd), buffer_(buffer), len_(buffer_size), is_fixed_(is_fixed) {}
  WriteAwaiter(int client_fd, char* buffer, int buffer_size, bool is_fixed = true, Context& ctx = GetCurrentContext())
      : BaseAwaiter(ctx), fd_(client_fd), buffer_(buffer), len_(buffer_size), is_fixed_(is_fixed) {}
  bool await_ready() { return false; }

  template <typename PromiseType>
  void await_suspend(std::coroutine_handle<PromiseType> handle) {
    this->handle = handle;
    if constexpr (requires(PromiseType& p) { p.get_stop_token(); }) {
      this->token_ = handle.promise().get_stop_token();
    }
    socket_service_.SubmitWrite(fd_, buffer_, len_, static_cast<IOHandler*>(this), is_fixed_);
    bind_stop_callback();
  }
  int await_resume() { return this->res_; }
  void on_cancel() override { socket_service_.SubmitCancel(this); }

 private:
  int fd_;
  char* buffer_;
  std::size_t len_;
  bool is_fixed_;
};

// Specialized write awaiter for butil::IOBuf (supports zero-copy io_uring_prep_writev scatter/gather writes)
struct IOBufWriteAwaiter : public BaseAwaiter {
  static constexpr size_t kMaxIovCount = 64;

  IOBufWriteAwaiter(Context& ctx, int client_fd, const butil::IOBuf& source_iobuf, bool is_fixed = true)
      : BaseAwaiter(ctx), fd_(client_fd), source_iobuf_(source_iobuf), is_fixed_(is_fixed) {
    init_iov();
  }
  IOBufWriteAwaiter(int client_fd, const butil::IOBuf& source_iobuf, bool is_fixed = true,
                    Context& ctx = GetCurrentContext())
      : BaseAwaiter(ctx), fd_(client_fd), source_iobuf_(source_iobuf), is_fixed_(is_fixed) {
    init_iov();
  }

  bool await_ready() { return total_len_ == 0; }

  template <typename PromiseType>
  void await_suspend(std::coroutine_handle<PromiseType> handle) {
    this->handle = handle;
    if constexpr (requires(PromiseType& p) { p.get_stop_token(); }) {
      this->token_ = handle.promise().get_stop_token();
    }
    if (iov_count_ == 1) {
      socket_service_.SubmitWrite(fd_, static_cast<char*>(iovs_[0].iov_base), iovs_[0].iov_len,
                                  static_cast<IOHandler*>(this), is_fixed_);
    } else {
      socket_service_.SubmitWritev(fd_, iovs_.get(), iov_count_, static_cast<IOHandler*>(this), is_fixed_);
    }
    bind_stop_callback();
  }

  int await_resume() { return this->res_; }
  void on_cancel() override { socket_service_.SubmitCancel(this); }

 private:
  void init_iov() {
    iov_count_ = std::min(source_iobuf_.backing_block_num(), kMaxIovCount);
    total_len_ = 0;
    if (iov_count_ > 0) {
      iovs_ = std::make_unique<struct iovec[]>(iov_count_);
      for (size_t i = 0; i < iov_count_; ++i) {
        butil::StringPiece sp = source_iobuf_.backing_block(i);
        iovs_[i].iov_base = const_cast<char*>(sp.data());
        iovs_[i].iov_len = sp.size();
        total_len_ += sp.size();
      }
    }
  }

  int fd_;
  const butil::IOBuf& source_iobuf_;
  bool is_fixed_;
  std::unique_ptr<struct iovec[]> iovs_;
  size_t iov_count_ {0};
  size_t total_len_ {0};
};

struct CloseAwaiter : public BaseAwaiter {
  CloseAwaiter(Context& ctx, int client_fd, bool is_fixed = true)
      : BaseAwaiter(ctx), fd_(client_fd), is_fixed_(is_fixed) {}
  explicit CloseAwaiter(int client_fd, bool is_fixed = true, Context& ctx = GetCurrentContext())
      : BaseAwaiter(ctx), fd_(client_fd), is_fixed_(is_fixed) {}
  bool await_ready() { return false; }
  void await_suspend(std::coroutine_handle<> handle) {
    this->handle = handle;
    socket_service_.SubmitClose(fd_, static_cast<IOHandler*>(this), is_fixed_);
  }
  int await_resume() { return this->res_; }

 private:
  int fd_;
  bool is_fixed_;
};

#endif
