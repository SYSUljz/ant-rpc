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

#include "ant_rpc/awaiter/timeput_awaiter.hpp"
#include "ant_rpc/context/context.hpp"
#include "ant_rpc/coroutine/task.hpp"
#include "ant_rpc/handler/acceptor.hpp"
#include "ant_rpc/rpc/channel.hpp"
#include "ant_rpc/rpc/controller.hpp"
#include "ant_rpc/rpc/protocol.hpp"
#include "ant_rpc/rpc/rpc_server.hpp"
#include "ant_rpc/rpc/service_registry.hpp"
#include "ant_rpc/scheduler/scheduler.hpp"
#include "echo.pb.h"

using namespace ant_rpc;
using namespace ant_rpc::rpc;

class EchoServiceImpl : public EchoService {
 public:
  void Echo(google::protobuf::RpcController* controller, const EchoRequest* request, EchoResponse* response,
            google::protobuf::Closure* done) override {
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
  enum class Action { kReset, kMalformedResponse, kHoldOpen };

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
    } else if (action_ == Action::kReset) {
      linger reset {1, 0};
      (void)setsockopt(peer, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset));
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
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

  RpcChannel channel(context);
  ASSERT_EQ(channel.Init("127.0.0.1", peer.port()), 0);
  EchoService_Stub stub(&channel);
  std::array<EchoRequest, kCallCount> requests;
  std::array<EchoResponse, kCallCount> responses;
  std::array<RpcController, kCallCount> controllers;
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
    EXPECT_EQ(controller.ErrorCode(), RPC_ECONN_FAILED);
  }
  EXPECT_EQ(channel.metrics().calls_started.Value(), kCallCount);
  EXPECT_EQ(channel.metrics().calls_completed.Value(), kCallCount);
  EXPECT_EQ(channel.metrics().calls_succeeded.Value(), 0);
  EXPECT_EQ(channel.metrics().call_errors.Value(), kCallCount);
  EXPECT_EQ(channel.metrics().active_in_flight.Value(), 0);

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

  RpcChannel channel(context);
  ASSERT_EQ(channel.Init("127.0.0.1", peer.port()), 0);
  EchoService_Stub stub(&channel);
  EchoRequest request;
  request.set_message("cancel-me");
  EchoResponse response;
  RpcController controller;
  std::atomic<int> remaining {1};
  absl::Notification done;
  CountdownClosure closure(remaining, done);

  stub.Echo(&controller, &request, &response, &closure);
  controller.StartCancel();

  ASSERT_TRUE(done.WaitForNotificationWithTimeout(absl::Seconds(2)));
  EXPECT_TRUE(controller.IsCanceled());
  EXPECT_TRUE(controller.Failed());
  EXPECT_EQ(controller.ErrorCode(), RPC_ECANCELED);
  EXPECT_EQ(remaining.load(), 0);
  EXPECT_EQ(channel.metrics().calls_completed.Value(), 1);
  EXPECT_EQ(channel.metrics().call_errors.Value(), 1);
  EXPECT_EQ(channel.metrics().calls_canceled.Value(), 1);
  EXPECT_EQ(channel.metrics().active_in_flight.Value(), 0);

  channel.Close();
  scheduler.Stop();
}

TEST(RpcChannelDeadlineTest, CallbackCallTimesOutWithoutPeerResponse) {
  RawTerminalPeer peer(RawTerminalPeer::Action::kHoldOpen);
  Scheduler scheduler {1, 1};
  Context& context = scheduler.GetIOContext(0);
  scheduler.Start();

  RpcChannel channel(context);
  RpcChannelOptions options;
  options.default_rpc_timeout = std::chrono::milliseconds(30);
  ASSERT_EQ(channel.Init("127.0.0.1", peer.port(), &options), 0);
  EchoService_Stub stub(&channel);
  EchoRequest request;
  request.set_message("deadline-callback");
  EchoResponse response;
  RpcController controller;
  std::atomic<int> remaining {1};
  absl::Notification done;
  CountdownClosure closure(remaining, done);

  stub.Echo(&controller, &request, &response, &closure);

  ASSERT_TRUE(done.WaitForNotificationWithTimeout(absl::Seconds(2)));
  EXPECT_TRUE(controller.Failed());
  EXPECT_EQ(controller.ErrorCode(), RPC_ETIMEOUT);
  EXPECT_EQ(remaining.load(), 0);
  EXPECT_EQ(channel.metrics().calls_completed.Value(), 1);
  EXPECT_EQ(channel.metrics().call_errors.Value(), 1);
  EXPECT_EQ(channel.metrics().calls_timed_out.Value(), 1);
  EXPECT_EQ(channel.metrics().active_in_flight.Value(), 0);

  channel.Close();
  scheduler.Stop();
}

TEST(RpcChannelDeadlineTest, CoroutineCallTimesOutWithoutPeerResponse) {
  RawTerminalPeer peer(RawTerminalPeer::Action::kHoldOpen);
  Scheduler scheduler {1, 1};
  Context& context = scheduler.GetIOContext(0);
  scheduler.Start();

  RpcChannel channel(context);
  RpcChannelOptions options;
  options.default_rpc_timeout = std::chrono::milliseconds(30);
  ASSERT_EQ(channel.Init("127.0.0.1", peer.port(), &options), 0);
  absl::Notification done;
  std::atomic<int> observed_error {RPC_SUCCESS};

  [](RpcChannel& channel, std::atomic<int>& observed_error, absl::Notification& done) -> DetachedTask {
    EchoRequest request;
    request.set_message("deadline-coroutine");
    EchoResponse response;
    RpcController controller;
    co_await channel.CallAsync("ant_rpc.EchoService", "Echo", &controller, &request, &response);
    observed_error.store(controller.ErrorCode(), std::memory_order_release);
    done.Notify();
  }(channel, observed_error, done);

  ASSERT_TRUE(done.WaitForNotificationWithTimeout(absl::Seconds(2)));
  EXPECT_EQ(observed_error.load(std::memory_order_acquire), RPC_ETIMEOUT);

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
  ServiceRegistry registry_;
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

    acceptor_ = std::make_unique<Acceptor>(
        server_ctx_, server_socket_, [this](int client_fd) { handle_rpc_client(server_ctx_, client_fd, registry_); });
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

TEST_F(RpcChannelTest, StartCancelAndWireResponseRaceCompletesExactlyOnce) {
  RpcChannel channel(client_ctx_);
  ASSERT_EQ(channel.Init("127.0.0.1", port_), 0);
  EchoService_Stub stub(&channel);
  constexpr int kIterations = 128;
  for (int iteration = 0; iteration < kIterations; ++iteration) {
    EchoRequest request;
    request.set_message("race-cancel-response");
    EchoResponse response;
    RpcController controller;
    std::atomic<int> remaining {1};
    absl::Notification done;
    CountdownClosure closure(remaining, done);

    stub.Echo(&controller, &request, &response, &closure);
    // CallMethod has returned only after it installed the controller's active
    // slot. The cancel thread now races the normal response arriving through
    // the IO driver, without reading controller internals concurrently.
    std::thread cancel_thread([&] { controller.StartCancel(); });

    const bool completed = done.WaitForNotificationWithTimeout(absl::Seconds(3));
    cancel_thread.join();
    ASSERT_TRUE(completed);

    EXPECT_TRUE(controller.IsCanceled());
    EXPECT_EQ(remaining.load(), 0) << "the RPC completion callback must run once";
    if (controller.Failed()) {
      EXPECT_EQ(controller.ErrorCode(), RPC_ECANCELED);
    } else {
      EXPECT_EQ(response.message(), "Echo: race-cancel-response");
    }
  }

  channel.Close();
}

TEST_F(RpcChannelTest, ProtobufStubSyncCall) {
  RpcChannel channel(client_ctx_);
  ASSERT_EQ(channel.Init("127.0.0.1", port_), 0);

  EchoService_Stub stub(&channel);

  EchoRequest req;
  req.set_message("Hello Protobuf Stub!");

  EchoResponse resp;
  RpcController cntl;

  // Standard Protobuf synchronous blocking call via Stub
  stub.Echo(&cntl, &req, &resp, nullptr);

  ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
  EXPECT_EQ(resp.message(), "Echo: Hello Protobuf Stub!");
  EXPECT_GT(cntl.latency_us(), 0);
  EXPECT_EQ(channel.metrics().calls_started.Value(), 1);
  EXPECT_EQ(channel.metrics().calls_completed.Value(), 1);
  EXPECT_EQ(channel.metrics().calls_succeeded.Value(), 1);
  EXPECT_EQ(channel.metrics().call_errors.Value(), 0);
  EXPECT_EQ(channel.metrics().active_in_flight.Value(), 0);
  EXPECT_EQ(channel.metrics().outbound_bytes.Value(), 0);
  EXPECT_EQ(channel.metrics().call_latency.Snapshot().count, 1);

  channel.Close();
}

TEST(RpcChannelMetricsTest, CallRejectedBeforeSlotAllocationIsRecordedInline) {
  RpcChannel channel;
  EchoService_Stub stub(&channel);
  EchoRequest request;
  EchoResponse response;
  RpcController controller;

  stub.Echo(&controller, &request, &response, nullptr);

  EXPECT_TRUE(controller.Failed());
  EXPECT_EQ(controller.ErrorCode(), RPC_ECONN_FAILED);
  EXPECT_EQ(channel.metrics().calls_started.Value(), 1);
  EXPECT_EQ(channel.metrics().calls_completed.Value(), 1);
  EXPECT_EQ(channel.metrics().calls_succeeded.Value(), 0);
  EXPECT_EQ(channel.metrics().call_errors.Value(), 1);
  EXPECT_EQ(channel.metrics().active_in_flight.Value(), 0);
}

TEST_F(RpcChannelTest, DefaultChannelUsesProcessRuntime) {
  ASSERT_TRUE(InitRuntime({.worker_threads = 1, .io_threads = 1}));

  RpcChannel channel;
  ASSERT_EQ(channel.Init("127.0.0.1:" + std::to_string(port_)), 0);

  EchoService_Stub stub(&channel);
  EchoRequest req;
  req.set_message("global-runtime");
  EchoResponse resp;
  RpcController cntl;
  stub.Echo(&cntl, &req, &resp, nullptr);

  EXPECT_FALSE(cntl.Failed()) << cntl.ErrorText();
  EXPECT_EQ(resp.message(), "Echo: global-runtime");

  channel.Close();
  EXPECT_TRUE(ShutdownRuntime());
}

TEST_F(RpcChannelTest, AsyncCallbackCall) {
  RpcChannel channel(client_ctx_);
  ASSERT_EQ(channel.Init("127.0.0.1", port_), 0);

  EchoService_Stub stub(&channel);

  EchoRequest req;
  req.set_message("Async Callback Test");

  EchoResponse resp;
  RpcController cntl;

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
  RpcChannel channel(client_ctx_);
  ASSERT_EQ(channel.Init("127.0.0.1", port_), 0);
  EchoService_Stub stub(&channel);

  ForeignRpcController controller;
  EchoRequest request;
  EchoResponse response;
  absl::Notification done_notification;
  struct TestClosure final : google::protobuf::Closure {
    explicit TestClosure(absl::Notification& notification) : notification(notification) {}
    void Run() override { notification.Notify(); }
    absl::Notification& notification;
  } done(done_notification);

  stub.Echo(&controller, &request, &response, &done);

  EXPECT_TRUE(done_notification.WaitForNotificationWithTimeout(absl::Seconds(1)));
  EXPECT_TRUE(controller.Failed());
  EXPECT_NE(controller.ErrorText().find("requires ant_rpc::rpc::RpcController"), std::string::npos);
  channel.Close();
}

TEST_F(RpcChannelTest, CoroutineCallMethodTemplate) {
  RpcChannel channel(client_ctx_);
  ASSERT_EQ(channel.Init("127.0.0.1", port_), 0);

  absl::Notification coroutine_done;

  [](RpcChannel& ch, int port, absl::Notification& notif) -> DetachedTask {
    EchoRequest req;
    req.set_message("Modern C++20 Coroutine Call");

    RpcController cntl;
    auto resp = co_await ch.Call<EchoResponse>("ant_rpc.EchoService", "Echo", req, &cntl);

    EXPECT_FALSE(cntl.Failed());
    EXPECT_EQ(resp.message(), "Echo: Modern C++20 Coroutine Call");
    notif.Notify();
  }(channel, port_, coroutine_done);

  ASSERT_TRUE(coroutine_done.WaitForNotificationWithTimeout(absl::Seconds(3)));
  channel.Close();
}

TEST_F(RpcChannelTest, HighConcurrencyMultiplexingOnSingleTcpConnection) {
  RpcChannel channel(client_ctx_);
  ASSERT_EQ(channel.Init("127.0.0.1", port_), 0);

  constexpr int TOTAL_CALLS = 100;
  std::atomic<int> completed_calls {0};
  absl::Notification all_done;

  for (int i = 0; i < TOTAL_CALLS; ++i) {
    [](RpcChannel& ch, int idx, std::atomic<int>& count, absl::Notification& notif) -> DetachedTask {
      EchoRequest req;
      req.set_message("Multiplex Ping " + std::to_string(idx));

      RpcController cntl;
      auto resp = co_await ch.Call<EchoResponse>("ant_rpc.EchoService", "Echo", req, &cntl);

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
  RpcChannel channel(client_ctx_);
  ASSERT_EQ(channel.Init("127.0.0.1", port_), 0);

  constexpr int kThreadCount = 4;
  constexpr int kCallsPerThread = 20;
  std::atomic<int> successful_calls {0};
  std::atomic<bool> failed {false};
  std::vector<std::thread> callers;
  callers.reserve(kThreadCount);

  for (int thread_id = 0; thread_id < kThreadCount; ++thread_id) {
    callers.emplace_back([&channel, &successful_calls, &failed, thread_id] {
      EchoService_Stub stub(&channel);
      for (int call_id = 0; call_id < kCallsPerThread; ++call_id) {
        RpcController controller;
        EchoRequest request;
        EchoResponse response;
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
