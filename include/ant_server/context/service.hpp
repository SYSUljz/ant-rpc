#ifndef ANT_SERVICE_HPP
#define ANT_SERVICE_HPP

#include <liburing.h>

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>

#include "ant_server/context/context.hpp"
#include "ant_server/type.hpp"

struct IOuringSocketService : public BaseService {
  explicit IOuringSocketService(Context& ctx) : ctx_(ctx) {}

  void submit_multishot_accept(int server_socket, void* handler) {
    io_uring_sqe* sqe = ctx_.GetSqe();
    io_uring_prep_multishot_accept_direct(sqe, server_socket, nullptr, nullptr, 0);
    sqe->file_index = IORING_FILE_INDEX_ALLOC;
    io_uring_sqe_set_data(sqe, handler);
    ctx_.Submit();
  }

  void SubmitRead(int fd, char* buffer, std::size_t len, void* handler, bool is_fixed = true) {
    struct io_uring_sqe* sqe = ctx_.GetSqe();
    io_uring_prep_recv(sqe, fd, buffer, len, 0);
    if (is_fixed) {
      sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_sqe_set_data(sqe, handler);
    ctx_.Submit();
  }

  void SubmitWrite(int fd, char* buffer, std::size_t len, void* handler, bool is_fixed = true) {
    struct io_uring_sqe* sqe = ctx_.GetSqe();
    io_uring_prep_send(sqe, fd, buffer, len, 0);
    if (is_fixed) {
      sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_sqe_set_data(sqe, handler);
    ctx_.Submit();
  }

  void SubmitWritev(int fd, const struct iovec* iovs, int nr_iovs, void* handler, bool is_fixed = true) {
    struct io_uring_sqe* sqe = ctx_.GetSqe();
    io_uring_prep_writev(sqe, fd, iovs, nr_iovs, 0);
    if (is_fixed) {
      sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_sqe_set_data(sqe, handler);
    ctx_.Submit();
  }

  void SubmitConnect(int fd, const sockaddr* address, socklen_t address_len, void* handler) {
    io_uring_sqe* sqe = ctx_.GetSqe();
    io_uring_prep_connect(sqe, fd, address, address_len);
    io_uring_sqe_set_data(sqe, handler);
    ctx_.Submit();
  }

  void SubmitClose(int fd, void* handler, bool is_fixed = true) {
    struct io_uring_sqe* sqe = ctx_.GetSqe();
    if (is_fixed) {
      io_uring_prep_close_direct(sqe, fd);
    } else {
      io_uring_prep_close(sqe, fd);
    }
    io_uring_sqe_set_data(sqe, handler);
    ctx_.Submit();
  }

  // accept_direct returns a registered-file index rather than a process fd.
  // ServerConnection uses this operation to make a protocol/close event
  // visible to the peer before releasing that registered-file slot.
  void SubmitShutdown(int fd, int how, bool is_fixed = true) {
    struct io_uring_sqe* sqe = ctx_.GetSqe();
    io_uring_prep_shutdown(sqe, fd, how);
    if (is_fixed) {
      sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_sqe_set_data(sqe, nullptr);
    ctx_.Submit();
  }

  void SubmitCancel(void* handler) {
    struct io_uring_sqe* sqe = ctx_.GetSqe();
    io_uring_prep_cancel(sqe, handler, 0);
    // no callback for cancel SQE itself
    io_uring_sqe_set_data(sqe, nullptr);
    ctx_.Submit();
  }

 private:
  Context& ctx_;
};

#endif
