#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <absl/synchronization/notification.h>
#include <gtest/gtest.h>

#include "ant_server/context/context.hpp"
#include "ant_server/rpc/channel.hpp"
#include "ant_server/rpc/controller.hpp"
#include "ant_server/rpc/error_code.hpp"
#include "ant_server/rpc/rpc_server.hpp"
#include "ant_server/scheduler/scheduler.hpp"
#include "echo.pb.h"

using namespace ant_server::rpc;

namespace {

// 1. Standard Protobuf EchoService implementation
class EchoServiceImpl : public ant_rpc::EchoService {
 public:
  void Echo(google::protobuf::RpcController* controller, const ant_rpc::EchoRequest* request,
            ant_rpc::EchoResponse* response, google::protobuf::Closure* done) override {
    auto* cntl = static_cast<RpcController*>(controller);
    if (request->message() == "trigger_error") {
      if (cntl) {
        cntl->SetFailed(RPC_EINTERNAL, "Custom business failure");
      }
    } else {
      response->set_message("Echo: " + request->message());
      if (cntl && !cntl->RequestAttachment().empty()) {
        cntl->ResponseAttachment() = cntl->RequestAttachment();
      }
    }
    if (done) {
      done->Run();
    }
  }
};

class BlockingEchoService final : public ant_rpc::EchoService {
 public:
  void Echo(google::protobuf::RpcController*, const ant_rpc::EchoRequest* request, ant_rpc::EchoResponse* response,
            google::protobuf::Closure* done) override {
    entered.Notify();
    release.WaitForNotification();
    response->set_message("Echo: " + request->message());
    if (done) {
      done->Run();
    }
  }

  absl::Notification entered;
  absl::Notification release;
};

class OutOfOrderEchoService final : public ant_rpc::EchoService {
 public:
  void Echo(google::protobuf::RpcController*, const ant_rpc::EchoRequest* request, ant_rpc::EchoResponse* response,
            google::protobuf::Closure* done) override {
    if (request->message() == "slow") {
      slow_started.Notify();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    response->set_message("Echo: " + request->message());
    if (done) {
      done->Run();
    }
  }

  absl::Notification slow_started;
};

struct NotifyClosure final : google::protobuf::Closure {
  explicit NotifyClosure(absl::Notification& notification) : notification(notification) {}
  void Run() override { notification.Notify(); }
  absl::Notification& notification;
};

struct OrderedNotifyClosure final : google::protobuf::Closure {
  OrderedNotifyClosure(absl::Notification& notification, std::atomic<int>& next_order, std::atomic<int>& order)
      : notification(notification), next_order(next_order), order(order) {}
  void Run() override {
    order.store(next_order.fetch_add(1, std::memory_order_acq_rel) + 1, std::memory_order_release);
    notification.Notify();
  }
  absl::Notification& notification;
  std::atomic<int>& next_order;
  std::atomic<int>& order;
};

TEST(RpcOptionsTest, RejectsInvalidChannelAndServerConfigurations) {
  RpcChannelOptions channel_options;
  EXPECT_TRUE(channel_options.IsValid());
  channel_options.connect_timeout = std::chrono::milliseconds {0};
  EXPECT_FALSE(channel_options.IsValid());
  channel_options = {};
  channel_options.default_rpc_timeout = std::chrono::milliseconds {-1};
  EXPECT_FALSE(channel_options.IsValid());
  channel_options = {};
  channel_options.max_frame_bytes = kRpcHeaderBytes - 1;
  EXPECT_FALSE(channel_options.IsValid());
  channel_options = {};
  channel_options.max_in_flight = 0;
  EXPECT_FALSE(channel_options.IsValid());
  channel_options = {};
  channel_options.max_in_flight = 65537;
  EXPECT_FALSE(channel_options.IsValid());
  channel_options = {};
  channel_options.max_outbound_bytes = kRpcHeaderBytes - 1;
  EXPECT_FALSE(channel_options.IsValid());

  RpcServerOptions server_options;
  EXPECT_TRUE(server_options.IsValid());
  server_options.max_connections = 0;
  EXPECT_FALSE(server_options.IsValid());
  server_options = {};
  server_options.max_in_flight = 0;
  EXPECT_FALSE(server_options.IsValid());
  server_options = {};
  server_options.max_frame_bytes = kRpcHeaderBytes - 1;
  EXPECT_FALSE(server_options.IsValid());
  server_options = {};
  server_options.graceful_stop_timeout = std::chrono::milliseconds {-1};
  EXPECT_FALSE(server_options.IsValid());
  server_options = {};
  server_options.idle_timeout = std::chrono::milliseconds {-1};
  EXPECT_FALSE(server_options.IsValid());
}

}  // namespace

// Test Suite for RpcServer with dynamic ephemeral port binding
class RpcServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Port 0 tells OS to assign an available ephemeral port
    server_ = std::make_unique<RpcServer>(server_context_, 0);

    // Register services (bRPC style)
    echo_service_ = std::make_shared<EchoServiceImpl>();
    ASSERT_TRUE(server_->AddService(echo_service_));
    ASSERT_TRUE(server_->Start());

    scheduler_.Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    port_ = server_->GetEndPoint().port;
    channel_.Init("127.0.0.1", port_);
  }

  void TearDown() override {
    channel_.Close();
    server_->Stop();
    EXPECT_TRUE(server_->Join());
    scheduler_.Stop();
  }

  int port_ {0};
  Scheduler scheduler_ {1, 2};
  Context& server_context_ {scheduler_.GetIOContext(0)};
  Context& client_context_ {scheduler_.GetIOContext(1)};
  std::shared_ptr<EchoServiceImpl> echo_service_;
  std::unique_ptr<RpcServer> server_;
  RpcChannel channel_ {client_context_};
};

// 1. Basic Echo RPC Call via Protobuf Stub
TEST_F(RpcServerTest, BasicEchoCall) {
  ant_rpc::EchoService_Stub stub(&channel_);
  RpcController cntl;
  ant_rpc::EchoRequest req;
  ant_rpc::EchoResponse resp;

  req.set_message("Hello AntServer RPC!");
  stub.Echo(&cntl, &req, &resp, nullptr);

  EXPECT_FALSE(cntl.Failed());
  EXPECT_EQ(cntl.ErrorCode(), RPC_SUCCESS);
  EXPECT_EQ(resp.message(), "Echo: Hello AntServer RPC!");
}

// 2. Custom Business Failure Propagation
TEST_F(RpcServerTest, CustomBusinessError) {
  ant_rpc::EchoService_Stub stub(&channel_);
  RpcController cntl;
  ant_rpc::EchoRequest req;
  ant_rpc::EchoResponse resp;

  req.set_message("trigger_error");
  stub.Echo(&cntl, &req, &resp, nullptr);

  EXPECT_TRUE(cntl.Failed());
  EXPECT_EQ(cntl.ErrorCode(), RPC_EINTERNAL);
  EXPECT_EQ(cntl.ErrorText(), "Custom business failure");
}

// 3. Service Not Found (RPC_ENOSERVICE)
TEST_F(RpcServerTest, ServiceNotFound) {
  RpcController cntl;
  butil::IOBuf req;
  butil::IOBuf resp;

  channel_.CallMethod("NonExistentService", "Echo", cntl, req, resp);

  EXPECT_TRUE(cntl.Failed());
  EXPECT_EQ(cntl.ErrorCode(), RPC_ENOSERVICE);
}

// 4. Method Not Found (RPC_ENOMETHOD)
TEST_F(RpcServerTest, MethodNotFound) {
  RpcController cntl;
  butil::IOBuf req;
  butil::IOBuf resp;

  channel_.CallMethod("ant_rpc.EchoService", "NonExistentMethod", cntl, req, resp);

  EXPECT_TRUE(cntl.Failed());
  EXPECT_EQ(cntl.ErrorCode(), RPC_ENOMETHOD);
}

// 5. Headers and Attachments Transmission
TEST_F(RpcServerTest, HeadersAndAttachments) {
  ant_rpc::EchoService_Stub stub(&channel_);
  RpcController cntl;
  cntl.SetHeader("auth_token", "secret123");
  cntl.RequestAttachment().append("extra_payload_binary");

  ant_rpc::EchoRequest req;
  ant_rpc::EchoResponse resp;
  req.set_message("HelloWithMeta");

  stub.Echo(&cntl, &req, &resp, nullptr);

  EXPECT_FALSE(cntl.Failed());
  EXPECT_EQ(cntl.ErrorCode(), RPC_SUCCESS);
  EXPECT_EQ(resp.message(), "Echo: HelloWithMeta");
  EXPECT_EQ(cntl.ResponseAttachment().to_string(), "extra_payload_binary");
}

// 6. Concurrent Multi-Threaded RPC Requests through RpcServer
TEST_F(RpcServerTest, ConcurrentRequests) {
  constexpr size_t kNumThreads = 4;
  constexpr size_t kCallsPerThread = 25;
  std::vector<std::thread> threads;
  std::atomic<size_t> success_count {0};

  for (size_t t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([this, &success_count, t]() {
      RpcChannel ch(client_context_);
      ch.Init("127.0.0.1", port_);
      ant_rpc::EchoService_Stub stub(&ch);

      for (size_t i = 0; i < kCallsPerThread; ++i) {
        RpcController cntl;
        ant_rpc::EchoRequest req;
        ant_rpc::EchoResponse resp;

        std::string payload = "msg_" + std::to_string(t) + "_" + std::to_string(i);
        req.set_message(payload);
        stub.Echo(&cntl, &req, &resp, nullptr);

        if (!cntl.Failed() && resp.message() == "Echo: " + payload) {
          success_count.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  for (auto& th : threads) {
    if (th.joinable()) {
      th.join();
    }
  }

  EXPECT_EQ(success_count.load(), kNumThreads * kCallsPerThread);
}

TEST(RpcServerLifecycleTest, ConstructionDoesNotListenAndLifecycleIsExplicit) {
  Scheduler scheduler {1, 1};
  Context& context = scheduler.GetIOContext(0);
  RpcServer server(context, 0);
  auto service = std::make_shared<EchoServiceImpl>();

  EXPECT_EQ(server.status(), RpcServer::Status::kCreated);
  EXPECT_EQ(server.GetSocketFd(), -1);
  EXPECT_TRUE(server.AddService(service));
  EXPECT_TRUE(server.Start());
  EXPECT_EQ(server.status(), RpcServer::Status::kRunning);
  EXPECT_GT(server.GetSocketFd(), -1);
  EXPECT_FALSE(server.Start());
  EXPECT_FALSE(server.AddService(std::make_shared<EchoServiceImpl>()));

  scheduler.Start();
  server.Stop();
  EXPECT_EQ(server.status(), RpcServer::Status::kStopping);
  EXPECT_TRUE(server.Join());
  EXPECT_EQ(server.status(), RpcServer::Status::kStopped);
  EXPECT_TRUE(server.Join());
  scheduler.Stop();
}

TEST(RpcServerOptionsTest, MaxInFlightReturnsOverloadWithoutBlockingReceiveLoop) {
  RpcServerOptions options;
  options.max_in_flight = 1;
  // One worker intentionally blocks in the first service. A second worker
  // keeps client completion independent from that server-side business work.
  Scheduler scheduler {2, 2};
  Context& server_context = scheduler.GetIOContext(0);
  Context& client_context = scheduler.GetIOContext(1);
  RpcServer server(server_context, 0, butil::IP_ANY, options);
  auto service = std::make_shared<BlockingEchoService>();
  ASSERT_TRUE(server.AddService(service));
  ASSERT_TRUE(server.Start());
  scheduler.Start();

  RpcChannel channel(client_context);
  ASSERT_EQ(channel.Init("127.0.0.1", server.GetEndPoint().port), 0);
  ant_rpc::EchoService_Stub stub(&channel);

  ant_rpc::EchoRequest first_request;
  first_request.set_message("first");
  ant_rpc::EchoResponse first_response;
  RpcController first_controller;
  absl::Notification first_done;
  NotifyClosure first_closure(first_done);
  stub.Echo(&first_controller, &first_request, &first_response, &first_closure);
  ASSERT_TRUE(service->entered.WaitForNotificationWithTimeout(absl::Seconds(2)));

  ant_rpc::EchoRequest second_request;
  second_request.set_message("second");
  ant_rpc::EchoResponse second_response;
  RpcController second_controller;
  stub.Echo(&second_controller, &second_request, &second_response, nullptr);
  EXPECT_TRUE(second_controller.Failed());
  EXPECT_EQ(second_controller.ErrorCode(), RPC_EOVERLOAD);

  service->release.Notify();
  ASSERT_TRUE(first_done.WaitForNotificationWithTimeout(absl::Seconds(2)));
  EXPECT_FALSE(first_controller.Failed()) << first_controller.ErrorText();
  EXPECT_EQ(first_response.message(), "Echo: first");

  channel.Close();
  server.Stop();
  EXPECT_TRUE(server.Join());
  scheduler.Stop();
}

TEST(RpcServerConnectionTest, SameConnectionResponsesFollowWorkerCompletionOrder) {
  Scheduler scheduler {2, 2};
  Context& server_context = scheduler.GetIOContext(0);
  Context& client_context = scheduler.GetIOContext(1);
  RpcServer server(server_context, 0);
  auto service = std::make_shared<OutOfOrderEchoService>();
  ASSERT_TRUE(server.AddService(service));
  ASSERT_TRUE(server.Start());
  scheduler.Start();

  RpcChannel channel(client_context);
  ASSERT_EQ(channel.Init("127.0.0.1", server.GetEndPoint().port), 0);
  ant_rpc::EchoService_Stub stub(&channel);

  RpcController slow_controller;
  ant_rpc::EchoRequest slow_request;
  ant_rpc::EchoResponse slow_response;
  slow_request.set_message("slow");
  RpcController fast_controller;
  ant_rpc::EchoRequest fast_request;
  ant_rpc::EchoResponse fast_response;
  fast_request.set_message("fast");

  absl::Notification slow_done;
  absl::Notification fast_done;
  std::atomic<int> next_order {0};
  std::atomic<int> slow_order {0};
  std::atomic<int> fast_order {0};
  OrderedNotifyClosure slow_closure(slow_done, next_order, slow_order);
  OrderedNotifyClosure fast_closure(fast_done, next_order, fast_order);

  stub.Echo(&slow_controller, &slow_request, &slow_response, &slow_closure);
  ASSERT_TRUE(service->slow_started.WaitForNotificationWithTimeout(absl::Seconds(2)));

  // ReceiveLoop has already handed A to a worker. It must keep reading this
  // same TCP connection so B can execute and respond while A sleeps.
  stub.Echo(&fast_controller, &fast_request, &fast_response, &fast_closure);
  ASSERT_TRUE(fast_done.WaitForNotificationWithTimeout(absl::Seconds(2)));
  EXPECT_FALSE(fast_controller.Failed()) << fast_controller.ErrorText();
  EXPECT_EQ(fast_response.message(), "Echo: fast");
  EXPECT_EQ(fast_order.load(std::memory_order_acquire), 1);

  ASSERT_TRUE(slow_done.WaitForNotificationWithTimeout(absl::Seconds(2)));
  EXPECT_FALSE(slow_controller.Failed()) << slow_controller.ErrorText();
  EXPECT_EQ(slow_response.message(), "Echo: slow");
  EXPECT_EQ(slow_order.load(std::memory_order_acquire), 2);

  channel.Close();
  server.Stop();
  EXPECT_TRUE(server.Join());
  scheduler.Stop();
}
