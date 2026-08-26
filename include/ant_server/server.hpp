#ifndef ANT_SERVER
#define ANT_SERVER

#include <liburing.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <coroutine>
#include <cstring>
#include <format>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>

#include <butil/endpoint.h>
#include <netinet/in.h>

#include "ant_server/awaiter/socket_awaiter.hpp"
#include "ant_server/awaiter/timeput_awaiter.hpp"
#include "ant_server/handler/acceptor.hpp"
#include "ant_server/http/parser.hpp"
#include "ant_server/scheduler/executor.hpp"
#include "ant_server/scheduler/timer_keeper.hpp"
#include "ant_server/type.hpp"

static constexpr int kBufferSize = 16000;

HttpTask handle_http_client(Context& ctx, int client_fd);

class Server {
 public:
  static constexpr int kUringSize = 256;

  // Primary constructor with butil::EndPoint
  Server(Context& ctx, butil::EndPoint endpoint) : ctx_(ctx), endpoint_(endpoint) { Init(); }

  // Convenient constructor with port and optional IP (defaults to butil::IP_ANY)
  Server(Context& ctx, int port, butil::ip_t ip = butil::IP_ANY) : Server(ctx, butil::EndPoint(ip, port)) {}

  // Legacy constructor for backward compatibility
  Server(Context& ctx, int domain, int port, int service, int protocol, int backlog, u_long interface)
      : Server(ctx, butil::EndPoint(butil::int2ip(htonl(interface)), port)) {
    (void)domain;
    (void)service;
    (void)protocol;
    (void)backlog;
  }

  ~Server() {
    if (server_socket_ >= 0) {
      close(server_socket_);
    }
  }

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  int GetSocketFd() const noexcept { return server_socket_; }
  const butil::EndPoint& GetEndPoint() const noexcept { return endpoint_; }

 private:
  void Init() {
    server_socket_ = butil::tcp_listen(endpoint_);
    if (server_socket_ < 0) {
      perror("Failed to start listening with butil::tcp_listen...");
      exit(EXIT_FAILURE);
    }

    acceptor_ = std::make_unique<Acceptor>(ctx_, server_socket_, [this](int client_fd) {
      TimerKeeper& tk = ctx_.GetTimerKeeper();

      [](Context& ctx, TimerKeeper& timer_keeper, int fd) -> DetachedTask {
        co_await with_timeout(timer_keeper, std::chrono::seconds(5), [&]() { return handle_http_client(ctx, fd); });
      }(ctx_, tk, client_fd);
    });
    acceptor_->Start();
  }

  Context& ctx_;
  butil::EndPoint endpoint_;
  int server_socket_ {-1};
  std::unique_ptr<Acceptor> acceptor_;
};

inline HttpTask handle_http_client(Context& ctx, int client_fd) {
  butil::IOBuf read_buf;
  HttpParser parser;
  HttpRequest req;

  while (true) {
    ParseResult result = parser.parse(read_buf, req);

    if (result.status == PARSE_NEED_MORE_DATA) {
      int read_bytes = co_await ReadAwaiter {ctx, client_fd, read_buf};
      if (read_bytes <= 0) {
        if (read_bytes == -ECANCELED) {
          std::cout << "[Server] Client fd " << client_fd << " timed out (5s), safely closing connection." << std::endl;
        } else {
          std::cout << "[Server] Client fd " << client_fd << " disconnected." << std::endl;
        }
        co_await CloseAwaiter {ctx, client_fd};
        co_return;
      }
      continue;
    } else if (result.status == PARSE_SUCCESS) {
      read_buf.pop_front(result.consumed_bytes);

      bool keep_alive = req.keep_alive;
      std::string body = "Hello, C++20 io_uring Web Server with IOBuf & llhttp!";

      butil::IOBuf response_buf;
      std::string header_str = std::format(
          "HTTP/1.1 200 OK\r\n"
          "Server: MyAwesomeServer/1.0\r\n"
          "Content-Length: {}\r\n"
          "Content-Type: text/plain\r\n"
          "Connection: {}\r\n"
          "\r\n"
          "{}",
          body.size(), keep_alive ? "keep-alive" : "close", body);
      response_buf.append(header_str);

      co_await IOBufWriteAwaiter {ctx, client_fd, response_buf};

      if (keep_alive) {
        parser.reset();
        req = HttpRequest {};
        continue;
      } else {
        co_await CloseAwaiter {ctx, client_fd};
        co_return;
      }
    } else {
      co_await CloseAwaiter {ctx, client_fd};
      co_return;
    }
  }
}

#endif
