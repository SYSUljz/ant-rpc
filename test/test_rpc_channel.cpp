#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <absl/synchronization/notification.h>
#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "ant_server/awaiter/timeput_awaiter.hpp"
#include "ant_server/context/context.hpp"
#include "ant_server/coroutine/task.hpp"
#include "ant_server/handler/acceptor.hpp"
#include "ant_server/rpc/channel.hpp"
#include "ant_server/rpc/controller.hpp"
#include "ant_server/rpc/protocol.hpp"
#include "ant_server/rpc/rpc_server.hpp"
#include "ant_server/rpc/service_registry.hpp"
#include "ant_server/scheduler/scheduler.hpp"
#include "echo.pb.h"

class EchoServiceImpl : public ant_rpc::EchoService {
 public:
  void Echo(google::protobuf::RpcController* controller, const ant_rpc::EchoRequest* request,
            ant_rpc::EchoResponse* response, google::protobuf::Closure* done) override {
    (void)controller;
    response->set_message("Echo: " + request->message());
    if (done) {
      done->Run();
    }
  }
};

// A protobuf-compatible controller that is intentionally not this project's
// RpcController. It verifies the compatibility boundary rejects unsafe casts.
class ForeignRpcController final : public google::protobuf::RpcController {
 public:
  void Reset() override { error_.clear(); }
  bool Failed() const override { return !error_.empty(); }
  std::string ErrorText() const override { return error_; }
  void StartCancel() override {}
  void SetFailed(const std::string& reason) override { error_ = reason; }
  bool IsCanceled() const override { return false; }
  void NotifyOnCancel(google::protobuf::Closure* callback) override { callback->Run(); }

 private:
  std::string error_;
};

class RawTerminalPeer {
 public:
  enum class Action { kReset, kMalformedResponse };

  explicit RawTerminalPeer(Action action) : action_(action) {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(listen_fd_, 0);
    int enabled = 1;
    EXPECT_EQ(setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)), 0);
    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    EXPECT_EQ(bind(listen_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
    EXPECT_EQ(listen(listen_fd_, 1), 0);
    socklen_t length = sizeof(address);
    EXPECT_EQ(getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&address), &length), 0);
    port_ = ntohs(address.sin_port);
    thread_ = std::thread([this] { ServeOnce(); });
  }

  ~RawTerminalPeer() {
    if (thread_.joinable()) {
      thread_.join();
    }
    if (listen_fd_ >= 0) {
      close(listen_fd_);
    }
  }

  int port() const noexcept { return port_; }

 private:
  void ServeOnce() {
    const int peer = accept(listen_fd_, nullptr, nullptr);
    if (peer < 0) {
      return;
    }
    char byte = 0;
    (void)recv(peer, &byte, sizeof(byte), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    if (action_ == Action::kMalformedResponse) {
      const std::array<char, 16> invalid_header {'N', 'O', 'P', 'E'};
      (void)send(peer, invalid_header.data(), invalid_header.size(), MSG_NOSIGNAL);
    } else {
      linger reset {1, 0};
      (void)setsockopt(peer, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset));
    }
    close(peer);
  }

  Action action_;
  int listen_fd_ {-1};
  int port_ {0};
  std::thread thread_;
};

struct CountdownClosure final : google::protobuf::Closure {
  CountdownClosure(std::atomic<int>& remaining, absl::Notification& notification)
      : remaining(remaining), notification(notification) {}
  void Run() override {
    if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      notification.Notify();
    }
  }
  std::atomic<int>& remaining;
  absl::Notification& notification;
};

void ExpectActiveCallsFailAfterPeerTerminalEvent(RawTerminalPeer::Action action) {
  constexpr int kCallCount = 4;
  RawTerminalPeer peer(action);
  Scheduler scheduler {1, 1};
  Context& context = scheduler.GetIOContext(0);
  scheduler.Start();

  ant_rpc::RpcChannel channel(context);
  ASSERT_EQ(channel.Init("127.0.0.1", peer.port()), 0);
  ant_rpc::EchoService_Stub stub(&channel);
  std::array<ant_rpc::EchoRequest, kCallCount> requests;
  std::array<ant_rpc::EchoResponse, kCallCount> responses;
  std::array<ant_rpc::RpcController, kCallCount> controllers;
  std::atomic<int> remaining {kCallCount};
  absl::Notification all_done;
  std::array<CountdownClosure, kCallCount> closures {
      CountdownClosure(remaining, all_done), CountdownClosure(remaining, all_done),
      CountdownClosure(remaining, all_done), CountdownClosure(remaining, all_done)};

  for (int index = 0; index < kCallCount; ++index) {
    requests[index].set_message("terminal-" + std::to_string(index));
    stub.Echo(&controllers[index], &requests[index], &responses[index], &closures[index]);
  }

  ASSERT_TRUE(all_done.WaitForNotificationWithTimeout(absl::Seconds(3)));
  for (const auto& controller : controllers) {
    EXPECT_TRUE(controller.Failed());
    EXPECT_EQ(controller.ErrorCode(), ant_rpc::RPC_ECONN_FAILED);
  }

  channel.Close();
  scheduler.Stop();
}

TEST(RpcChannelTerminalFailureTest, PeerResetFailsMultiplePendingCalls) {
  ExpectActiveCallsFailAfterPeerTerminalEvent(RawTerminalPeer::Action::kReset);
}

TEST(RpcChannelTerminalFailureTest, ProtocolErrorFailsMultipleActiveCalls) {
  ExpectActiveCallsFailAfterPeerTerminalEvent(RawTerminalPeer::Action::kMalformedResponse);
}

TEST(RpcChannelCancellationTest, StartCancelResolvesAnActiveSlot) {
  RawTerminalPeer peer(RawTerminalPeer::Action::kReset);
  Scheduler scheduler {1, 1};
  Context& context = scheduler.GetIOContext(0);
  scheduler.Start();

  ant_rpc::RpcChannel channel(context);
  ASSERT_EQ(channel.Init("127.0.0.1", peer.port()), 0);
  ant_rpc::EchoService_Stub stub(&channel);
  ant_rpc::EchoRequest request;
  request.set_message("cancel-me");
  ant_rpc::EchoResponse response;
  ant_rpc::RpcController controller;
  std::atomic<int> remaining {1};
  absl::Notification done;
  CountdownClosure closure(remaining, done);

  stub.Echo(&controller, &request, &response, &closure);
  controller.StartCancel();

  ASSERT_TRUE(done.WaitForNotificationWithTimeout(absl::Seconds(2)));
  EXPECT_TRUE(controller.IsCanceled());
  EXPECT_TRUE(controller.Failed());
  EXPECT_EQ(controller.ErrorCode(), ant_rpc::RPC_ECANCELED);
  EXPECT_EQ(remaining.load(), 0);

  channel.Close();
  scheduler.Stop();
}

class RpcChannelTest : public ::testing::Test {
 protected:
  int port_ {0};
  Scheduler server_scheduler_ {1, 1};
  Scheduler client_scheduler_ {1, 1};
  Context& server_ctx_ {server_scheduler_.GetIOContext(0)};
  Context& client_ctx_ {client_scheduler_.GetIOContext(0)};
  ant_rpc::ServiceRegistry registry_;
  EchoServiceImpl echo_service_;
  std::unique_ptr<Acceptor> acceptor_;
  int server_socket_ {-1};

  void SetUp() override {
    registry_.RegisterService(&echo_service_);
    server_socket_ = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(server_socket_, 0);

    int opt = 1;
    setsockopt(server_socket_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(0);
    addr.sin_addr.s_addr = INADDR_ANY;

    ASSERT_GE(bind(server_socket_, (struct sockaddr*)&addr, sizeof(addr)), 0);
    ASSERT_GE(listen(server_socket_, 128), 0);

    socklen_t addr_len = sizeof(addr);
    ASSERT_GE(getsockname(server_socket_, (struct sockaddr*)&addr, &addr_len), 0);
    port_ = ntohs(addr.sin_port);

    acceptor_ = std::make_unique<Acceptor>(server_ctx_, server_socket_, [this](int client_fd) {
      ant_rpc::handle_rpc_client(server_ctx_, client_fd, registry_);
    });
    acceptor_->Start();

    server_scheduler_.Start();
    client_scheduler_.Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void TearDown() override {
    server_scheduler_.Stop();
    client_scheduler_.Stop();
    if (server_socket_ >= 0) {
      close(server_socket_);
    }
  }
};

TEST_F(RpcChannelTest, ProtobufStubSyncCall) {
  ant_rpc::RpcChannel channel(client_ctx_);
  ASSERT_EQ(channel.Init("127.0.0.1", port_), 0);

  ant_rpc::EchoService_Stub stub(&channel);

  ant_rpc::EchoRequest req;
  req.set_message("Hello Protobuf Stub!");

  ant_rpc::EchoResponse resp;
  ant_rpc::RpcController cntl;

  // Standard Protobuf synchronous blocking call via Stub
  stub.Echo(&cntl, &req, &resp, nullptr);

  ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
  EXPECT_EQ(resp.message(), "Echo: Hello Protobuf Stub!");
  EXPECT_GT(cntl.latency_us(), 0);

  channel.Close();
}

TEST_F(RpcChannelTest, DefaultChannelUsesProcessRuntime) {
  ASSERT_TRUE(ant_rpc::InitRuntime({.worker_threads = 1, .io_threads = 1}));

  ant_rpc::RpcChannel channel;
  ASSERT_EQ(channel.Init("127.0.0.1:" + std::to_string(port_)), 0);

  ant_rpc::EchoService_Stub stub(&channel);
  ant_rpc::EchoRequest req;
  req.set_message("global-runtime");
  ant_rpc::EchoResponse resp;
  ant_rpc::RpcController cntl;
  stub.Echo(&cntl, &req, &resp, nullptr);

  EXPECT_FALSE(cntl.Failed()) << cntl.ErrorText();
  EXPECT_EQ(resp.message(), "Echo: global-runtime");

  channel.Close();
  EXPECT_TRUE(ant_rpc::ShutdownRuntime());
}

TEST_F(RpcChannelTest, AsyncCallbackCall) {
  ant_rpc::RpcChannel channel(client_ctx_);
  ASSERT_EQ(channel.Init("127.0.0.1", port_), 0);

  ant_rpc::EchoService_Stub stub(&channel);

  ant_rpc::EchoRequest req;
  req.set_message("Async Callback Test");

  ant_rpc::EchoResponse resp;
  ant_rpc::RpcController cntl;

  absl::Notification done_notification;
  struct TestClosure : public google::protobuf::Closure {
    absl::Notification& notif;
    explicit TestClosure(absl::Notification& n) : notif(n) {}
    void Run() override { notif.Notify(); }
  } done_closure(done_notification);

  stub.Echo(&cntl, &req, &resp, &done_closure);

  ASSERT_TRUE(done_notification.WaitForNotificationWithTimeout(absl::Seconds(3)));
  ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
  EXPECT_EQ(resp.message(), "Echo: Async Callback Test");

  channel.Close();
}

TEST_F(RpcChannelTest, RejectsForeignProtobufController) {
  ant_rpc::RpcChannel channel(client_ctx_);
  ASSERT_EQ(channel.Init("127.0.0.1", port_), 0);
  ant_rpc::EchoService_Stub stub(&channel);

  ForeignRpcController controller;
  ant_rpc::EchoRequest request;
  ant_rpc::EchoResponse response;
  absl::Notification done_notification;
  struct TestClosure final : google::protobuf::Closure {
    explicit TestClosure(absl::Notification& notification) : notification(notification) {}
    void Run() override { notification.Notify(); }
    absl::Notification& notification;
  } done(done_notification);

  stub.Echo(&controller, &request, &response, &done);

  EXPECT_TRUE(done_notification.WaitForNotificationWithTimeout(absl::Seconds(1)));
  EXPECT_TRUE(controller.Failed());
  EXPECT_NE(controller.ErrorText().find("requires ant_server::rpc::RpcController"), std::string::npos);
  channel.Close();
}

TEST_F(RpcChannelTest, CoroutineCallMethodTemplate) {
  ant_rpc::RpcChannel channel(client_ctx_);
  ASSERT_EQ(channel.Init("127.0.0.1", port_), 0);

  absl::Notification coroutine_done;

  [](ant_rpc::RpcChannel& ch, int port, absl::Notification& notif) -> DetachedTask {
    ant_rpc::EchoRequest req;
    req.set_message("Modern C++20 Coroutine Call");

    ant_rpc::RpcController cntl;
    auto resp = co_await ch.Call<ant_rpc::EchoResponse>("ant_rpc.EchoService", "Echo", req, &cntl);

    EXPECT_FALSE(cntl.Failed());
    EXPECT_EQ(resp.message(), "Echo: Modern C++20 Coroutine Call");
    notif.Notify();
  }(channel, port_, coroutine_done);

  ASSERT_TRUE(coroutine_done.WaitForNotificationWithTimeout(absl::Seconds(3)));
  channel.Close();
}

TEST_F(RpcChannelTest, HighConcurrencyMultiplexingOnSingleTcpConnection) {
  ant_rpc::RpcChannel channel(client_ctx_);
  ASSERT_EQ(channel.Init("127.0.0.1", port_), 0);

  constexpr int TOTAL_CALLS = 100;
  std::atomic<int> completed_calls {0};
  absl::Notification all_done;

  for (int i = 0; i < TOTAL_CALLS; ++i) {
    [](ant_rpc::RpcChannel& ch, int idx, std::atomic<int>& count, absl::Notification& notif) -> DetachedTask {
      ant_rpc::EchoRequest req;
      req.set_message("Multiplex Ping " + std::to_string(idx));

      ant_rpc::RpcController cntl;
      auto resp = co_await ch.Call<ant_rpc::EchoResponse>("ant_rpc.EchoService", "Echo", req, &cntl);

      EXPECT_FALSE(cntl.Failed()) << cntl.ErrorText();
      EXPECT_EQ(resp.message(), "Echo: Multiplex Ping " + std::to_string(idx));

      if (count.fetch_add(1) + 1 == TOTAL_CALLS) {
        notif.Notify();
      }
    }(channel, i, completed_calls, all_done);
  }

  ASSERT_TRUE(all_done.WaitForNotificationWithTimeout(absl::Seconds(5)));
  EXPECT_EQ(completed_calls.load(), TOTAL_CALLS);

  channel.Close();
}

TEST_F(RpcChannelTest, SharedChannelAcceptsCallsFromMultipleExternalThreads) {
  ant_rpc::RpcChannel channel(client_ctx_);
  ASSERT_EQ(channel.Init("127.0.0.1", port_), 0);

  constexpr int kThreadCount = 4;
  constexpr int kCallsPerThread = 20;
  std::atomic<int> successful_calls {0};
  std::atomic<bool> failed {false};
  std::vector<std::thread> callers;
  callers.reserve(kThreadCount);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    callers.emplace_back([&channel, &successful_calls, &failed, thread_id] {
      ant_rpc::EchoService_Stub stub(&channel);
      for (int call_id = 0; call_id < kCallsPerThread; ++call_id) {
        ant_rpc::RpcController controller;
        ant_rpc::EchoRequest request;
        ant_rpc::EchoResponse response;
        const std::string payload = "external-" + std::to_string(thread_id) + "-" + std::to_string(call_id);
        request.set_message(payload);
        stub.Echo(&controller, &request, &response, nullptr);
        if (controller.Failed() || response.message() != "Echo: " + payload) {
          failed.store(true, std::memory_order_release);
          return;
        }
        successful_calls.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (auto& caller : callers) {
    caller.join();
  }

  EXPECT_FALSE(failed.load(std::memory_order_acquire));
  EXPECT_EQ(successful_calls.load(std::memory_order_relaxed), kThreadCount * kCallsPerThread);
  channel.Close();
}
