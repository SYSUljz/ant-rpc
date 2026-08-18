#pragma once

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <coroutine>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <arpa/inet.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include "absl/synchronization/notification.h"
#include "ant_server/awaiter/socket_awaiter.hpp"
#include "ant_server/context/context.hpp"
#include "ant_server/coroutine/task.hpp"
#include "ant_server/rpc/controller.hpp"
#include "ant_server/rpc/protocol.hpp"
#include "ant_server/rpc/slot_table.hpp"
#include "butil/endpoint.h"
#include "butil/iobuf.h"

namespace ant_server::rpc {

struct RpcChannelOptions {
  int connect_timeout_ms {1000};
  int64_t timeout_ms {5000};
  bool tcp_no_delay {true};
};

using ChannelOptions = RpcChannelOptions;

class RpcChannel;

// Modern C++20 Coroutine Awaiter for RPC Call
struct RpcCallAwaiter {
  RpcChannel& channel;
  std::string_view service_name;
  std::string_view method_name;
  RpcController* controller;
  const google::protobuf::Message* request;
  google::protobuf::Message* response;
  uint64_t correlation_id {0};

  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> handle) noexcept;
  void await_resume() noexcept {}
};

// High-Performance Multiplexing C++20 RPC Channel implementing google::protobuf::RpcChannel
class RpcChannel : public google::protobuf::RpcChannel {
 public:
  RpcChannel() : ctx_(nullptr) {}
  explicit RpcChannel(Context& ctx) : ctx_(&ctx) {}

  ~RpcChannel() override { Close(); }

  RpcChannel(const RpcChannel&) = delete;
  RpcChannel& operator=(const RpcChannel&) = delete;

  // Initialize TCP connection with IP and Port
  int Init(const std::string& server_ip, int port, const RpcChannelOptions* options = nullptr) {
    butil::ip_t ip_val;
    if (butil::str2ip(server_ip.c_str(), &ip_val) != 0) {
      return -1;
    }
    return Init(butil::EndPoint(ip_val, port), options);
  }

  int Init(const char* ip, int port, const RpcChannelOptions* options = nullptr) {
    return Init(std::string(ip), port, options);
  }

  // Initialize TCP connection with butil::EndPoint
  int Init(butil::EndPoint endpoint, const RpcChannelOptions* options = nullptr) {
    if (running_.load(std::memory_order_relaxed)) {
      Close();
    }
    if (options) {
      options_ = *options;
    }
    endpoint_ = endpoint;

    client_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd_ < 0) {
      return -1;
    }

    if (options_.tcp_no_delay) {
      int flag = 1;
      setsockopt(client_fd_, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    }

    struct sockaddr_in serv_addr {};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(endpoint_.port);
    serv_addr.sin_addr = endpoint_.ip;

    if (connect(client_fd_, reinterpret_cast<struct sockaddr*>(&serv_addr), sizeof(serv_addr)) < 0) {
      close(client_fd_);
      client_fd_ = -1;
      return -1;
    }

    running_.store(true, std::memory_order_release);

    // Launch background Receiver thread (bRPC InputMessenger style)
    reader_thread_ = std::thread([this]() {
      butil::IOBuf recv_buffer;
      char temp[4096];
      while (running_.load(std::memory_order_acquire)) {
        ssize_t n = recv(client_fd_, temp, sizeof(temp), 0);
        if (n <= 0) {
          break;
        }
        recv_buffer.append(temp, n);
        while (true) {
          FrameParseResult res = TryParseRpcFrame(recv_buffer);
          if (res.status == FrameParseStatus::NEED_MORE_DATA) {
            break;
          }
          if (res.status != FrameParseStatus::SUCCESS) {
            recv_buffer.clear();
            break;
          }
          recv_buffer.pop_front(res.total_frame_bytes);
          slot_table_.CompleteSlot(res.meta.correlation_id(), res.body_iobuf, res.meta, res.attachment_iobuf);
        }
      }
    });

    return 0;
  }

  void Close() {
    if (running_.exchange(false, std::memory_order_acq_rel)) {
      if (client_fd_ >= 0) {
        shutdown(client_fd_, SHUT_RDWR);
        close(client_fd_);
        client_fd_ = -1;
      }
      if (reader_thread_.joinable()) {
        reader_thread_.join();
      }
    }
  }

  int fd() const noexcept { return client_fd_; }
  SlotTable<65536>& slot_table() { return slot_table_; }

  // Standard Protobuf RpcChannel interface implementation
  void CallMethod(const google::protobuf::MethodDescriptor* method, google::protobuf::RpcController* controller,
                  const google::protobuf::Message* request, google::protobuf::Message* response,
                  google::protobuf::Closure* done) override {
    auto* cntl = static_cast<RpcController*>(controller);

    if (done != nullptr) {
      SendRpc(method->service()->full_name(), method->name(), cntl, request, response, nullptr, done);
    } else {
      absl::Notification notify;
      struct SyncClosure : public google::protobuf::Closure {
        absl::Notification& n;
        explicit SyncClosure(absl::Notification& notif) : n(notif) {}
        void Run() override { n.Notify(); }
      } sync_done(notify);

      SendRpc(method->service()->full_name(), method->name(), cntl, request, response, nullptr, &sync_done);

      int64_t timeout = (cntl && cntl->TimeoutMs() > 0) ? cntl->TimeoutMs() : options_.timeout_ms;
      if (!notify.WaitForNotificationWithTimeout(absl::Milliseconds(timeout))) {
        if (cntl) {
          cntl->SetFailed(RPC_ETIMEOUT, "RPC call timed out");
        }
      }
    }
  }

  // Raw IOBuf method call (useful for testing or direct binary RPC)
  void CallMethod(std::string_view service_name, std::string_view method_name, RpcController& cntl,
                  const butil::IOBuf& req_body, butil::IOBuf& resp_body) {
    absl::Notification notify;
    struct SyncClosure : public google::protobuf::Closure {
      absl::Notification& n;
      explicit SyncClosure(absl::Notification& notif) : n(notif) {}
      void Run() override { n.Notify(); }
    } sync_done(notify);

    SendRpc(service_name, method_name, &cntl, nullptr, nullptr, nullptr, &sync_done, &req_body, &resp_body);

    int64_t timeout = cntl.TimeoutMs() > 0 ? cntl.TimeoutMs() : options_.timeout_ms;
    if (!notify.WaitForNotificationWithTimeout(absl::Milliseconds(timeout))) {
      cntl.SetFailed(RPC_ETIMEOUT, "RPC call timed out");
    }
  }

  // C++20 Coroutine Async Call Awaiter generator
  RpcCallAwaiter CallAsync(const google::protobuf::MethodDescriptor* method, RpcController* controller,
                           const google::protobuf::Message* request, google::protobuf::Message* response) {
    return RpcCallAwaiter {*this, method->service()->full_name(), method->name(), controller, request, response};
  }

  RpcCallAwaiter CallAsync(std::string_view service_name, std::string_view method_name, RpcController* controller,
                           const google::protobuf::Message* request, google::protobuf::Message* response) {
    return RpcCallAwaiter {*this, service_name, method_name, controller, request, response};
  }

  // Modern C++20 Value-returning CallMethod template
  template <typename ResponseType>
  Task<ResponseType> Call(std::string_view service_name, std::string_view method_name,
                          const google::protobuf::Message& request, RpcController* controller = nullptr) {
    ResponseType resp;
    co_await CallAsync(service_name, method_name, controller, &request, &resp);
    co_return resp;
  }

  // Send encoded RPC frame into socket (Thread-safe frame sending)
  uint64_t SendRpc(std::string_view service_name, std::string_view method_name, RpcController* controller,
                   const google::protobuf::Message* request, google::protobuf::Message* response,
                   std::coroutine_handle<> handle, google::protobuf::Closure* done,
                   const butil::IOBuf* req_raw_body = nullptr, butil::IOBuf* resp_raw_body = nullptr) {
    if (controller) {
      controller->RecordStart();
    }

    // 1. Allocate slot
    uint64_t cid = slot_table_.AllocateSlot(handle, response, controller, done, resp_raw_body);
    if (cid == 0) {
      if (controller) {
        controller->SetFailed(RPC_EOVERLOAD, "RPC slot table capacity exhausted");
      }
      if (handle) {
        handle.resume();
      } else if (done) {
        done->Run();
      }
      return 0;
    }

    // 2. Build RpcMeta
    RpcMeta meta;
    meta.set_msg_type(RPC_REQUEST);
    meta.set_correlation_id(cid);
    meta.set_service_name(std::string(service_name));
    meta.set_method_name(std::string(method_name));
    if (controller) {
      meta.set_log_id(controller->LogId());
      meta.set_timeout_ms(controller->TimeoutMs());
      for (const auto& [k, v] : controller->Headers()) {
        (*meta.mutable_headers())[k] = v;
      }
    }

    // 3. Pack Frame
    butil::IOBuf send_buf;
    const butil::IOBuf* attachment = controller ? &controller->RequestAttachment() : nullptr;
    if (request) {
      PackRpcFrame(meta, request, attachment, send_buf);
    } else if (req_raw_body) {
      PackRpcFrame(meta, *req_raw_body, attachment, send_buf);
    } else {
      butil::IOBuf empty_body;
      PackRpcFrame(meta, empty_body, attachment, send_buf);
    }

    // 4. Send over TCP socket with write mutex (atomic frame boundary)
    {
      std::lock_guard<std::mutex> lock(write_mu_);
      if (client_fd_ < 0) {
        slot_table_.TimeoutSlot(cid, "Connection closed");
        return cid;
      }

      size_t blk_num = send_buf.backing_block_num();
      for (size_t i = 0; i < blk_num; ++i) {
        butil::StringPiece sp = send_buf.backing_block(i);
        size_t written = 0;
        while (written < sp.size()) {
          ssize_t sent = send(client_fd_, sp.data() + written, sp.size() - written, MSG_NOSIGNAL);
          if (sent < 0) {
            slot_table_.TimeoutSlot(cid, "Failed to send RPC request: " + std::string(strerror(errno)));
            return cid;
          }
          written += sent;
        }
      }
    }

    return cid;
  }

 private:
  // Background Receiver Coroutine for Context
  Task<void> ReceiverLoop() {
    butil::IOBuf recv_buffer;
    while (running_.load(std::memory_order_acquire)) {
      int bytes_read = co_await ReadAwaiter(*ctx_, client_fd_, recv_buffer);
      if (bytes_read <= 0) {
        break;
      }

      while (true) {
        FrameParseResult res = TryParseRpcFrame(recv_buffer);
        if (res.status == FrameParseStatus::NEED_MORE_DATA) {
          break;
        }
        if (res.status != FrameParseStatus::SUCCESS) {
          recv_buffer.clear();
          break;
        }

        recv_buffer.pop_front(res.total_frame_bytes);
        slot_table_.CompleteSlot(res.meta.correlation_id(), res.body_iobuf, res.meta, res.attachment_iobuf);
      }
    }
    co_return;
  }
  // comment : cache line aligna
  Context* ctx_ {nullptr};
  int client_fd_ {-1};
  butil::EndPoint endpoint_;
  RpcChannelOptions options_;
  std::atomic<bool> running_ {false};
  std::mutex write_mu_;
  std::thread reader_thread_;
  SlotTable<65536> slot_table_;
};

inline void RpcCallAwaiter::await_suspend(std::coroutine_handle<> handle) noexcept {
  correlation_id = channel.SendRpc(service_name, method_name, controller, request, response, handle, nullptr);
}

}  // namespace ant_server::rpc

namespace ant_rpc {
using namespace ant_server::rpc;
}
