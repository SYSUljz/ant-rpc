#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

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

}  // namespace

// Test Suite for RpcServer with dynamic ephemeral port binding
class RpcServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Port 0 tells OS to assign an available ephemeral port
    server_ = std::make_unique<RpcServer>(server_context_, 0);

    // Register services (bRPC style)
    echo_service_ = std::make_shared<EchoServiceImpl>();
    server_->AddService(echo_service_);

    scheduler_.Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    port_ = server_->GetEndPoint().port;
    channel_.Init("127.0.0.1", port_);
  }

  void TearDown() override {
    channel_.Close();
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
