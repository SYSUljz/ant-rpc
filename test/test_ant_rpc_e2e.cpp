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

#include "ant_rpc/awaiter/timeput_awaiter.hpp"
#include "ant_rpc/context/context.hpp"
#include "ant_rpc/handler/acceptor.hpp"
#include "ant_rpc/rpc/protocol.hpp"
#include "ant_rpc/rpc/rpc_server.hpp"
#include "ant_rpc/rpc/service_registry.hpp"
#include "ant_rpc/scheduler/scheduler.hpp"
#include "butil/iobuf.h"
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

class AntRpcE2ETest : public ::testing::Test {
 protected:
  static constexpr int PORT = 9012;
  Scheduler scheduler_ {1, 1};
  Context& context_ {scheduler_.GetIOContext(0)};
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
    addr.sin_port = htons(PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    ASSERT_GE(bind(server_socket_, (struct sockaddr*)&addr, sizeof(addr)), 0);
    ASSERT_GE(listen(server_socket_, 128), 0);

    acceptor_ = std::make_unique<Acceptor>(
        context_, server_socket_, [this](int client_fd) { handle_rpc_client(context_, client_fd, registry_); });
    acceptor_->Start();

    scheduler_.Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void TearDown() override {
    scheduler_.Stop();
    if (server_socket_ >= 0) {
      close(server_socket_);
    }
  }

  int ConnectClient() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(fd, 0);

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    int ret = connect(fd, (struct sockaddr*)&addr, sizeof(addr));
    EXPECT_EQ(ret, 0);
    return fd;
  }
};

TEST_F(AntRpcE2ETest, SingleRpcCallOverTcp) {
  int client_fd = ConnectClient();
  ASSERT_GE(client_fd, 0);

  // 1. Pack RPC Request
  EchoRequest req;
  req.set_message("Hello Ant RPC!");

  butil::IOBuf send_buf;
  PackRpcFrame(/*msg_type=Request*/ 0, /*correlation_id*/ 1001, "ant_rpc.EchoService", "Echo", req, send_buf);

  // 2. Send over TCP
  std::string flat_bytes = send_buf.to_string();
  ssize_t sent = write(client_fd, flat_bytes.data(), flat_bytes.size());
  ASSERT_EQ(sent, static_cast<ssize_t>(flat_bytes.size()));

  // 3. Receive RPC Response Frame
  butil::IOBuf recv_buf;
  char temp[1024];
  ssize_t n = read(client_fd, temp, sizeof(temp));
  ASSERT_GT(n, 0);
  recv_buf.append(temp, n);

  FrameParseResult res = TryParseRpcFrame(recv_buf);
  ASSERT_EQ(res.status, FrameParseStatus::SUCCESS);
  EXPECT_EQ(res.meta.msg_type(), RPC_RESPONSE);
  EXPECT_EQ(res.meta.correlation_id(), 1001);
  EXPECT_EQ(res.meta.service_name(), "ant_rpc.EchoService");
  EXPECT_EQ(res.meta.method_name(), "Echo");

  // 4. Parse response protobuf message
  EchoResponse resp;
  butil::IOBufAsZeroCopyInputStream zc_in(res.body_iobuf);
  ASSERT_TRUE(resp.ParseFromZeroCopyStream(&zc_in));
  EXPECT_EQ(resp.message(), "Echo: Hello Ant RPC!");

  close(client_fd);
}

TEST_F(AntRpcE2ETest, MultiplexedContinuousCallsOnSingleTcpConnection) {
  int client_fd = ConnectClient();
  ASSERT_GE(client_fd, 0);

  for (uint64_t i = 1; i <= 20; ++i) {
    EchoRequest req;
    req.set_message("Ping " + std::to_string(i));

    butil::IOBuf send_buf;
    PackRpcFrame(0, i, "ant_rpc.EchoService", "Echo", req, send_buf);
    std::string flat_bytes = send_buf.to_string();
    ASSERT_EQ(write(client_fd, flat_bytes.data(), flat_bytes.size()), static_cast<ssize_t>(flat_bytes.size()));

    butil::IOBuf recv_buf;
    char temp[1024];
    ssize_t n = read(client_fd, temp, sizeof(temp));
    ASSERT_GT(n, 0);
    recv_buf.append(temp, n);

    FrameParseResult res = TryParseRpcFrame(recv_buf);
    ASSERT_EQ(res.status, FrameParseStatus::SUCCESS);
    EXPECT_EQ(res.meta.correlation_id(), i);

    EchoResponse resp;
    butil::IOBufAsZeroCopyInputStream zc_in(res.body_iobuf);
    ASSERT_TRUE(resp.ParseFromZeroCopyStream(&zc_in));
    EXPECT_EQ(resp.message(), "Echo: Ping " + std::to_string(i));
  }

  close(client_fd);
}

TEST_F(AntRpcE2ETest, ChunkedAndFragmentedByteStreaming) {
  int client_fd = ConnectClient();
  ASSERT_GE(client_fd, 0);

  EchoRequest req;
  req.set_message("Fragmented Chunked RPC Payload Test");

  butil::IOBuf send_buf;
  PackRpcFrame(0, 9999, "ant_rpc.EchoService", "Echo", req, send_buf);
  std::string flat_bytes = send_buf.to_string();

  // Send byte-by-byte with small delays to simulate heavy network fragmentation
  for (char byte : flat_bytes) {
    ASSERT_EQ(write(client_fd, &byte, 1), 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Receive response
  butil::IOBuf recv_buf;
  char temp[1024];
  while (true) {
    ssize_t n = read(client_fd, temp, sizeof(temp));
    if (n > 0) {
      recv_buf.append(temp, n);
      auto res = TryParseRpcFrame(recv_buf);
      if (res.status == FrameParseStatus::SUCCESS) {
        EXPECT_EQ(res.meta.correlation_id(), 9999);
        EchoResponse resp;
        butil::IOBufAsZeroCopyInputStream zc_in(res.body_iobuf);
        ASSERT_TRUE(resp.ParseFromZeroCopyStream(&zc_in));
        EXPECT_EQ(resp.message(), "Echo: Fragmented Chunked RPC Payload Test");
        break;
      }
    }
  }

  close(client_fd);
}
