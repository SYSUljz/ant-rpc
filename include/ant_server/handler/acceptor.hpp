#ifndef ANT_ACCEPTOR_HPP
#define ANT_ACCEPTOR_HPP

#include <functional>
#include <stdexcept>

#include "ant_server/context/context.hpp"
#include "ant_server/context/service.hpp"
#include "ant_server/type.hpp"

struct Acceptor : public IOHandler {
  Acceptor(Context& ctx, int server_socket, std::function<void(int)> on_accept)
      : ctx_(ctx),
        server_socket_(server_socket),
        on_accept_cb_(std::move(on_accept)),
        service_(ctx_.UseService<IOuringSocketService>()) {}

  void Start() { service_.submit_multishot_accept(server_socket_, static_cast<IOHandler*>(this)); }

  void on_complete() override {
    if (res_ >= 0) {
      on_accept_cb_(res_);
    }

    if (res_ >= 0 && !(flags_ & IORING_CQE_F_MORE)) {
      service_.submit_multishot_accept(server_socket_, static_cast<IOHandler*>(this));
    }
  }

 private:
  Context& ctx_;
  int server_socket_;
  std::function<void(int)> on_accept_cb_;
  IOuringSocketService& service_;
};

#endif
