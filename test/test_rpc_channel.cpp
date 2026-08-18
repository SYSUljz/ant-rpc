#include <unistd.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

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

class RpcChannelTest : public ::testing::Test {
 protected:
  int port_ {0};
  Context server_ctx_ {256};
  Context client_ctx_ {256};
  ant_rpc::ServiceRegistry registry_;
  EchoServiceImpl echo_service_;
  std::unique_ptr<Acceptor> acceptor_;
  int server_socket_ {-1};
  std::thread server_thread_;
  std::thread client_thread_;

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

    server_thread_ = std::thread([this]() { server_ctx_.Start(); });
    client_thread_ = std::thread([this]() { client_ctx_.Start(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void TearDown() override {
    server_ctx_.Stop();
    client_ctx_.Stop();
    if (server_thread_.joinable()) server_thread_.join();
    if (client_thread_.joinable()) client_thread_.join();
    if (server_socket_ >= 0) close(server_socket_);
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
