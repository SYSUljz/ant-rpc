#pragma once

#include <functional>

#include "ant_server/context/context.hpp"
#include "ant_server/type.hpp"
#include "liburing.h"

struct Acceptor final : IOHandler {
  Acceptor(Context& context, int listener, std::function<void(int)> callback)
      : context_(context), listener_(listener), callback_(std::move(callback)) {}
  void Start() {
    auto* sqe = context_.GetSqe();
    io_uring_prep_multishot_accept_direct(sqe, listener_, nullptr, nullptr, 0);
    sqe->file_index = IORING_FILE_INDEX_ALLOC;
    io_uring_sqe_set_data(sqe, this);
    context_.Submit();
  }
  void on_complete() override {
    if (res_ >= 0) {
      callback_(res_);
    }
    if (res_ >= 0 && !(flags_ & IORING_CQE_F_MORE)) {
      Start();
    }
  }

 private:
  Context& context_;
  int listener_;
  std::function<void(int)> callback_;
};
