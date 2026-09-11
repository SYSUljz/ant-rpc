#include <poll.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <absl/synchronization/notification.h>
#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

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

class QueueSaturatingEchoService final : public ant_rpc::EchoService {
 public:
  void Echo(google::protobuf::RpcController*, const ant_rpc::EchoRequest* request, ant_rpc::EchoResponse* response,
            google::protobuf::Closure* done) override {
    if (!first_call_started.exchange(true, std::memory_order_acq_rel)) {
      entered.Notify();
    }
    release.WaitForNotification();
    response->set_message("Echo: " + request->message());
    if (done) {
      done->Run();
    }
  }

  std::atomic<bool> first_call_started {false};
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

class AsyncDoneEchoService final : public ant_rpc::EchoService {
 public:
  ~AsyncDoneEchoService() override { Join(); }

  void Echo(google::protobuf::RpcController*, const ant_rpc::EchoRequest* request, ant_rpc::EchoResponse* response,
            google::protobuf::Closure* done) override {
    const std::string message = request->message();
    std::lock_guard<std::mutex> lock(mu_);
    worker_ = std::thread([message, response, done] {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      response->set_message("Async: " + message);
      done->Run();
      // A duplicate completion before the IO owner has removed the call must
      // be harmless: InboundCallState's CAS admits only the first one.
      done->Run();
    });
  }

  void Join() {
    std::lock_guard<std::mutex> lock(mu_);
    if (worker_.joinable()) {
      worker_.join();
    }
  }

 private:
  std::mutex mu_;
  std::thread worker_;
};

class DeferredDoneEchoService final : public ant_rpc::EchoService {
 public:
  ~DeferredDoneEchoService() override { Join(); }

  void Echo(google::protobuf::RpcController*, const ant_rpc::EchoRequest* request, ant_rpc::EchoResponse* response,
            google::protobuf::Closure* done) override {
    const std::string message = request->message();
    std::lock_guard<std::mutex> lock(mu_);
    worker_ = std::thread([this, message, response, done] {
      entered.Notify();
      release.WaitForNotification();
      response->set_message("Deferred: " + message);
      done->Run();
    });
  }

  void Join() {
    std::lock_guard<std::mutex> lock(mu_);
    if (worker_.joinable()) {
      worker_.join();
    }
  }

  absl::Notification entered;
  absl::Notification release;

 private:
  std::mutex mu_;
  std::thread worker_;
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

struct CountingNotifyClosure final : google::protobuf::Closure {
  CountingNotifyClosure(absl::Notification& notification, std::atomic<int>& count)
      : notification(notification), count(count) {}
  void Run() override {
    count.fetch_add(1, std::memory_order_acq_rel);
    notification.Notify();
  }
  absl::Notification& notification;
  std::atomic<int>& count;
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
  server_options.io_contexts = 0;
  EXPECT_FALSE(server_options.IsValid());
  server_options = {};
  server_options.max_connections = 0;
  EXPECT_FALSE(server_options.IsValid());
  server_options = {};
  server_options.max_in_flight = 0;
  EXPECT_FALSE(server_options.IsValid());
  server_options = {};
  server_options.max_in_flight_per_connection = 0;
  EXPECT_FALSE(server_options.IsValid());
  server_options = {};
  server_options.max_in_flight_per_connection = server_options.max_in_flight + 1;
  EXPECT_FALSE(server_options.IsValid());
  server_options = {};
  server_options.max_pending_worker_tasks = 0;
  EXPECT_FALSE(server_options.IsValid());
  server_options = {};
  server_options.max_frame_bytes = kRpcHeaderBytes - 1;
  EXPECT_FALSE(server_options.IsValid());
  server_options = {};
  server_options.max_outbound_bytes_per_connection = kRpcHeaderBytes - 1;
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

  // These are framework metrics: EchoServiceImpl does not perform any manual
  // instrumentation. HandleRequest and FinalizeInboundCall own the updates.
  const auto& metrics = server_->metrics();
  EXPECT_EQ(metrics.requests_received.Value(), 1);
  EXPECT_EQ(metrics.calls_started.Value(), 1);
  EXPECT_EQ(metrics.calls_completed.Value(), 1);
  EXPECT_EQ(metrics.call_errors.Value(), 0);
  EXPECT_EQ(metrics.responses_enqueued.Value(), 1);
  EXPECT_EQ(metrics.active_in_flight.Value(), 0);
  EXPECT_EQ(metrics.request_latency.Snapshot().count, 1);
  const auto method_metrics = metrics.MethodSnapshot();
  ASSERT_EQ(method_metrics.size(), 1);
  EXPECT_EQ(method_metrics[0]->service_name, "ant_rpc.EchoService");
  EXPECT_EQ(method_metrics[0]->method_name, "Echo");
  EXPECT_EQ(method_metrics[0]->calls_started.Value(), 1);
  EXPECT_EQ(method_metrics[0]->calls_completed.Value(), 1);
  EXPECT_EQ(method_metrics[0]->call_errors.Value(), 0);
  EXPECT_EQ(method_metrics[0]->active_in_flight.Value(), 0);
  EXPECT_EQ(method_metrics[0]->call_latency.Snapshot().count, 1);
}

// A fresh TCP connection per request, with bounded IO even if the admin loop
// regresses. The response is deliberately checked over the HTTP wire.
std::string FetchAdmin(const AdminServer& admin, std::string_view path) {
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    ADD_FAILURE() << "socket errno=" << errno;
    return {};
  }
  timeval timeout {2, 0};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
  sockaddr_in address {};
  address.sin_family = AF_INET;
  address.sin_port = htons(admin.GetEndPoint().port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  int connected = connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));
  if (connected != 0 && (errno == EINTR || errno == EINPROGRESS)) {
    pollfd writable {fd, POLLOUT, 0};
    int polled;
    do {
      polled = poll(&writable, 1, 2000);
    } while (polled < 0 && errno == EINTR);
    int error = 0;
    socklen_t size = sizeof(error);
    if (polled > 0 && getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &size) == 0 && error == 0) {
      connected = 0;
    }
  }
  if (connected != 0) {
    ADD_FAILURE() << "connect failed errno=" << errno;
    close(fd);
    return {};
  }
  const std::string request = "GET " + std::string(path) + " HTTP/1.1\r\nHost: localhost\r\n\r\n";
  if (send(fd, request.data(), request.size(), MSG_NOSIGNAL) != static_cast<ssize_t>(request.size())) {
    ADD_FAILURE() << "send errno=" << errno;
    close(fd);
    return {};
  }
  std::string response;
  char buffer[2048];
  ssize_t bytes;
  while ((bytes = recv(fd, buffer, sizeof(buffer), 0)) > 0) {
    response.append(buffer, static_cast<std::size_t>(bytes));
  }
  if (bytes < 0) {
    ADD_FAILURE() << "recv errno=" << errno;
  }
  close(fd);
  return response;
}

TEST(AdminServerTest, HealthAndStatusFollowLifecycleAndUnregisterOnDestruction) {
  Scheduler scheduler {1, 1};
  Context& context = scheduler.GetIOContext(0);
  AdminServer admin(context);
  ASSERT_TRUE(admin.Start());
  scheduler.Start();
  EXPECT_TRUE(FetchAdmin(admin, "/health").starts_with("HTTP/1.1 503"));
  EXPECT_NE(FetchAdmin(admin, "/status").find("{\"ready\":false,\"servers\":[]}"), std::string::npos);

  auto server = std::make_unique<RpcServer>(context, 0);
  ASSERT_TRUE(admin.AddServer("echo\"\\\n", *server));
  EXPECT_TRUE(FetchAdmin(admin, "/health").starts_with("HTTP/1.1 503"));
  EXPECT_NE(FetchAdmin(admin, "/status").find("\"state\":\"created\""), std::string::npos);
  ASSERT_TRUE(server->Start());
  EXPECT_TRUE(FetchAdmin(admin, "/health").starts_with("HTTP/1.1 200"));
  const auto status = FetchAdmin(admin, "/status/");
  EXPECT_TRUE(status.starts_with("HTTP/1.1 200"));
  EXPECT_NE(status.find("Content-Type: application/json"), std::string::npos);
  EXPECT_NE(status.find("\"ready\":true"), std::string::npos);
  EXPECT_NE(status.find("\"name\":\"echo\\\"\\\\\\u000a\""), std::string::npos);
  EXPECT_NE(status.find("\"state\":\"running\""), std::string::npos);
  EXPECT_NE(status.find("\"active_connections\":0"), std::string::npos);
  EXPECT_NE(status.find("\"io_command_backlog\":0"), std::string::npos);

  server->Stop();
  EXPECT_TRUE(FetchAdmin(admin, "/health").starts_with("HTTP/1.1 503"));
  EXPECT_NE(FetchAdmin(admin, "/status").find("\"state\":\"stopping\""), std::string::npos);
  EXPECT_TRUE(server->Join());
  EXPECT_NE(FetchAdmin(admin, "/status").find("\"state\":\"stopped\""), std::string::npos);
  server.reset();
  EXPECT_TRUE(FetchAdmin(admin, "/health").starts_with("HTTP/1.1 503"));
  EXPECT_NE(FetchAdmin(admin, "/status").find("\"servers\":[]"), std::string::npos);
  EXPECT_EQ(FetchAdmin(admin, "/metrics").find("{server="), std::string::npos);
  auto replacement = std::make_unique<RpcServer>(context, 0);
  EXPECT_TRUE(admin.AddServer("echo\"\\\n", *replacement));
  replacement.reset();
  EXPECT_TRUE(FetchAdmin(admin, "/missing").starts_with("HTTP/1.1 404"));
  admin.Stop();
  scheduler.Stop();
}

TEST(AdminServerTest, ConcurrentQueriesAndDestructionRemoveAllAliasesAndObservers) {
  Scheduler scheduler {1, 1};
  auto first = std::make_shared<detail::AdminServerState>(scheduler.GetIOContext(0));
  auto second = std::make_shared<detail::AdminServerState>(scheduler.GetIOContext(0));
  auto server = std::make_unique<RpcServer>(scheduler.GetIOContext(0), 0);
  ASSERT_TRUE(first->AddSource("one", *server));
  ASSERT_TRUE(first->AddSource("alias", *server));
  ASSERT_TRUE(second->AddSource("two", *server));
  std::atomic<bool> stop {false};
  absl::Notification entered;
  std::thread reader([&] {
    entered.Notify();
    while (!stop.load(std::memory_order_acquire)) {
      (void)first->StatusText();
      (void)first->Healthy();
      (void)first->MetricsText();
      (void)second->StatusText();
    }
  });
  entered.WaitForNotification();
  server.reset();
  EXPECT_EQ(first->StatusText(), "{\"ready\":false,\"servers\":[]}\n");
  EXPECT_EQ(second->StatusText(), "{\"ready\":false,\"servers\":[]}\n");
  EXPECT_EQ(first->MetricsText().find("{server="), std::string::npos);
  stop.store(true, std::memory_order_release);
  reader.join();
  // Reverse lifetime order: expired observers do not retain or dereference admin state.
  auto another = std::make_unique<RpcServer>(scheduler.GetIOContext(0), 0);
  ASSERT_TRUE(first->AddSource("one", *another));
  first.reset();
  another.reset();
}

TEST(AdminServerTest, HealthRequiresAllRegisteredServersRunning) {
  Scheduler scheduler {1, 1};
  Context& context = scheduler.GetIOContext(0);
  RpcServer running(context, 0);
  ASSERT_TRUE(running.Start());
  // Binding the same endpoint produces a real startup failure.
  RpcServer failed(context, running.GetEndPoint());
  EXPECT_FALSE(failed.Start());
  ASSERT_EQ(failed.status(), RpcServer::Status::kFailed);
  AdminServer admin(context);
  ASSERT_TRUE(admin.AddServer("running", running));
  ASSERT_TRUE(admin.AddServer("failed", failed));
  ASSERT_TRUE(admin.Start());
  scheduler.Start();
  EXPECT_TRUE(FetchAdmin(admin, "/health").starts_with("HTTP/1.1 503"));
  const auto status = FetchAdmin(admin, "/status");
  EXPECT_TRUE(status.starts_with("HTTP/1.1 200"));
  EXPECT_NE(status.find("\"state\":\"failed\""), std::string::npos);
  EXPECT_NE(status.find("\"state\":\"running\""), std::string::npos);
  admin.Stop();
  running.Stop();
  EXPECT_TRUE(running.Join());
  failed.Stop();
  EXPECT_TRUE(failed.Join());
  scheduler.Stop();
}

TEST(AdminServerTest, ServesRpcMetricsOnTheSameContext) {
  Scheduler scheduler {1, 2};
  Context& rpc_context = scheduler.GetIOContext(0);
  Context& client_context = scheduler.GetIOContext(1);
  RpcServer rpc_server(rpc_context, 0);
  auto service = std::make_shared<EchoServiceImpl>();
  ASSERT_TRUE(rpc_server.AddService(service));
  ASSERT_TRUE(rpc_server.Start());

  // The admin listener is a separate protocol endpoint, but deliberately
  // shares the RPC listener's Context and thus its IO thread.
  AdminServer admin_server(rpc_context);
  ASSERT_TRUE(admin_server.AddServer("echo", rpc_server));
  auto business_requests = std::make_shared<ant_server::metrics::Counter>();
  ASSERT_TRUE(admin_server.Registry().Register("business_requests_total", business_requests));
  business_requests->Add(3);
  ASSERT_TRUE(admin_server.Start());
  scheduler.Start();

  RpcChannel channel(client_context);
  ASSERT_EQ(channel.Init("127.0.0.1", rpc_server.GetEndPoint().port), 0);
  ant_rpc::EchoService_Stub stub(&channel);
  RpcController controller;
  ant_rpc::EchoRequest request;
  ant_rpc::EchoResponse response;
  request.set_message("metrics");
  stub.Echo(&controller, &request, &response, nullptr);
  ASSERT_FALSE(controller.Failed()) << controller.ErrorText();

  channel.Close();
  bool peer_close_recorded = false;
  for (int attempt = 0; attempt != 200; ++attempt) {
    if (rpc_server.metrics().closed_connections.Value() == 1) {
      peer_close_recorded = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(peer_close_recorded);

  const int metrics_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(metrics_fd, 0);
  struct sockaddr_in address {};
  address.sin_family = AF_INET;
  address.sin_port = htons(admin_server.GetEndPoint().port);
  ASSERT_EQ(inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
  ASSERT_EQ(connect(metrics_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  constexpr std::string_view kRequest = "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n";
  ASSERT_EQ(send(metrics_fd, kRequest.data(), kRequest.size(), 0), static_cast<ssize_t>(kRequest.size()));

  std::string wire_response;
  char buffer[1024];
  while (true) {
    const ssize_t bytes = recv(metrics_fd, buffer, sizeof(buffer), 0);
    if (bytes <= 0) {
      break;
    }
    wire_response.append(buffer, static_cast<std::size_t>(bytes));
  }
  close(metrics_fd);

  EXPECT_NE(wire_response.find("HTTP/1.1 200 OK"), std::string::npos);
  EXPECT_NE(wire_response.find("ant_custom_business_requests_total 3\n"), std::string::npos);
  EXPECT_NE(wire_response.find("ant_rpc_server_requests_received_total{server=\"echo\"} 1"), std::string::npos);
  EXPECT_NE(wire_response.find("ant_rpc_server_active_in_flight{server=\"echo\"} 0"), std::string::npos);
  EXPECT_NE(wire_response.find("ant_rpc_server_io_command_backlog{server=\"echo\"} 0"), std::string::npos);
  EXPECT_NE(wire_response.find("ant_rpc_server_connection_closes_total{server=\"echo\",reason=\"peer_eof\"} 1"),
            std::string::npos);
  EXPECT_NE(wire_response.find("ant_rpc_server_method_calls_completed_total{server=\"echo\",service=\"ant_rpc."
                               "EchoService\",method=\"Echo\"} 1"),
            std::string::npos);

  admin_server.Stop();
  rpc_server.Stop();
  EXPECT_TRUE(rpc_server.Join());
  scheduler.Stop();
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
  const auto method_metrics = server_->metrics().MethodSnapshot();
  ASSERT_EQ(method_metrics.size(), 1);
  EXPECT_EQ(method_metrics[0]->calls_started.Value(), 1);
  EXPECT_EQ(method_metrics[0]->calls_completed.Value(), 1);
  EXPECT_EQ(method_metrics[0]->call_errors.Value(), 1);
  EXPECT_EQ(method_metrics[0]->active_in_flight.Value(), 0);
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

  // A method lookup failure is an RPC-level response, not a transport
  // failure. The same multiplexed connection must remain usable.
  ant_rpc::EchoService_Stub stub(&channel_);
  RpcController echo_controller;
  ant_rpc::EchoRequest echo_request;
  ant_rpc::EchoResponse echo_response;
  echo_request.set_message("after-method-error");
  stub.Echo(&echo_controller, &echo_request, &echo_response, nullptr);
  EXPECT_FALSE(echo_controller.Failed()) << echo_controller.ErrorText();
  EXPECT_EQ(echo_response.message(), "Echo: after-method-error");

  // Arbitrary method names from the wire must not create unbounded metric
  // labels. Only the successfully resolved Echo descriptor is registered.
  const auto method_metrics = server_->metrics().MethodSnapshot();
  ASSERT_EQ(method_metrics.size(), 1);
  EXPECT_EQ(method_metrics[0]->method_name, "Echo");
  EXPECT_EQ(method_metrics[0]->calls_completed.Value(), 1);
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

TEST(RpcServerOptionsTest, MaxConnectionsRejectsAdditionalConnectionWithoutDisturbingExistingOne) {
  RpcServerOptions options;
  options.max_connections = 1;
  Scheduler scheduler {2, 2};
  Context& server_context = scheduler.GetIOContext(0);
  Context& client_context = scheduler.GetIOContext(1);
  RpcServer server(server_context, 0, butil::IP_ANY, options);
  auto service = std::make_shared<EchoServiceImpl>();
  ASSERT_TRUE(server.AddService(service));
  ASSERT_TRUE(server.Start());
  scheduler.Start();

  RpcChannel admitted_channel(client_context);
  ASSERT_EQ(admitted_channel.Init("127.0.0.1", server.GetEndPoint().port), 0);
  bool first_connection_tracked = false;
  for (int attempt = 0; attempt != 200; ++attempt) {
    if (server.metrics().active_connections.Value() == 1) {
      first_connection_tracked = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(first_connection_tracked);

  // Use a raw socket for the rejected peer: TCP connect may complete before
  // the server accepts and closes the over-limit fd, so successful connect()
  // does not mean the RPC connection was admitted.
  const int rejected_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(rejected_fd, 0);
  sockaddr_in address {};
  address.sin_family = AF_INET;
  address.sin_port = htons(server.GetEndPoint().port);
  ASSERT_EQ(inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
  ASSERT_EQ(connect(rejected_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

  bool rejection_recorded = false;
  for (int attempt = 0; attempt != 200; ++attempt) {
    if (server.metrics().rejected_connections.Value() == 1) {
      rejection_recorded = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(rejection_recorded);
  pollfd rejected_poll {rejected_fd, POLLIN, 0};
  ASSERT_GT(poll(&rejected_poll, 1, 2000), 0);
  char byte = 0;
  EXPECT_LE(recv(rejected_fd, &byte, sizeof(byte), 0), 0);
  close(rejected_fd);

  // Rejecting the second connection must not evict or poison the admitted
  // connection.
  ant_rpc::EchoService_Stub admitted_stub(&admitted_channel);
  RpcController admitted_controller;
  ant_rpc::EchoRequest admitted_request;
  ant_rpc::EchoResponse admitted_response;
  admitted_request.set_message("still-admitted");
  admitted_stub.Echo(&admitted_controller, &admitted_request, &admitted_response, nullptr);
  EXPECT_FALSE(admitted_controller.Failed()) << admitted_controller.ErrorText();
  EXPECT_EQ(admitted_response.message(), "Echo: still-admitted");
  EXPECT_EQ(server.metrics().accepted_connections.Value(), 1);
  EXPECT_EQ(server.metrics().active_connections.Value(), 1);

  admitted_channel.Close();
  server.Stop();
  EXPECT_TRUE(server.Join());
  scheduler.Stop();
}

TEST(RpcServerOptionsTest, GlobalMaxInFlightRejectsCallFromAnotherConnection) {
  RpcServerOptions options;
  options.max_in_flight = 1;
  options.max_in_flight_per_connection = 1;
  Scheduler scheduler {2, 2};
  Context& server_context = scheduler.GetIOContext(0);
  Context& client_context = scheduler.GetIOContext(1);
  RpcServer server(server_context, 0, butil::IP_ANY, options);
  auto service = std::make_shared<BlockingEchoService>();
  ASSERT_TRUE(server.AddService(service));
  ASSERT_TRUE(server.Start());
  scheduler.Start();

  RpcChannel first_channel(client_context);
  RpcChannel second_channel(client_context);
  ASSERT_EQ(first_channel.Init("127.0.0.1", server.GetEndPoint().port), 0);
  ASSERT_EQ(second_channel.Init("127.0.0.1", server.GetEndPoint().port), 0);
  ant_rpc::EchoService_Stub first_stub(&first_channel);
  ant_rpc::EchoService_Stub second_stub(&second_channel);

  RpcController first_controller;
  ant_rpc::EchoRequest first_request;
  ant_rpc::EchoResponse first_response;
  first_request.set_message("occupies-global-slot");
  absl::Notification first_done;
  NotifyClosure first_closure(first_done);
  first_stub.Echo(&first_controller, &first_request, &first_response, &first_closure);
  ASSERT_TRUE(service->entered.WaitForNotificationWithTimeout(absl::Seconds(2)));
  ASSERT_EQ(server.metrics().active_in_flight.Value(), 1);

  RpcController second_controller;
  ant_rpc::EchoRequest second_request;
  ant_rpc::EchoResponse second_response;
  second_request.set_message("different-connection");
  second_stub.Echo(&second_controller, &second_request, &second_response, nullptr);
  EXPECT_TRUE(second_controller.Failed());
  EXPECT_EQ(second_controller.ErrorCode(), RPC_EOVERLOAD);

  service->release.Notify();
  ASSERT_TRUE(first_done.WaitForNotificationWithTimeout(absl::Seconds(2)));
  EXPECT_FALSE(first_controller.Failed()) << first_controller.ErrorText();
  EXPECT_EQ(first_response.message(), "Echo: occupies-global-slot");
  EXPECT_EQ(server.metrics().requests_received.Value(), 2);
  EXPECT_EQ(server.metrics().calls_started.Value(), 1);
  EXPECT_EQ(server.metrics().calls_completed.Value(), 1);
  EXPECT_EQ(server.metrics().requests_rejected_overload.Value(), 1);
  EXPECT_EQ(server.metrics().active_in_flight.Value(), 0);

  first_channel.Close();
  second_channel.Close();
  server.Stop();
  EXPECT_TRUE(server.Join());
  scheduler.Stop();
}

TEST(RpcChannelOptionsTest, FrameLargerThanClientOutboundBudgetFailsInline) {
  Scheduler scheduler {1, 2};
  Context& server_context = scheduler.GetIOContext(0);
  Context& client_context = scheduler.GetIOContext(1);
  RpcServer server(server_context, 0);
  auto service = std::make_shared<EchoServiceImpl>();
  ASSERT_TRUE(server.AddService(service));
  ASSERT_TRUE(server.Start());
  scheduler.Start();

  RpcChannelOptions channel_options;
  channel_options.max_outbound_bytes = kRpcHeaderBytes;
  RpcChannel channel(client_context);
  ASSERT_EQ(channel.Init("127.0.0.1", server.GetEndPoint().port, &channel_options), 0);
  ant_rpc::EchoService_Stub stub(&channel);

  RpcController controller;
  ant_rpc::EchoRequest request;
  ant_rpc::EchoResponse response;
  request.set_message("larger-than-header-only-budget");
  stub.Echo(&controller, &request, &response, nullptr);

  EXPECT_TRUE(controller.Failed());
  EXPECT_EQ(controller.ErrorCode(), RPC_EOVERLOAD);
  EXPECT_NE(controller.ErrorText().find("max_outbound_bytes"), std::string::npos);
  EXPECT_EQ(channel.slot_table().active_slot_count(), 0U);
  EXPECT_EQ(server.metrics().requests_received.Value(), 0);

  channel.Close();
  server.Stop();
  EXPECT_TRUE(server.Join());
  scheduler.Stop();
}

TEST(RpcServerOptionsTest, MaxInFlightPerConnectionReturnsOverloadWithoutBlockingReceiveLoop) {
  RpcServerOptions options;
  options.max_in_flight = 2;
  options.max_in_flight_per_connection = 1;
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

  const auto& metrics = server.metrics();
  EXPECT_EQ(metrics.requests_received.Value(), 2);
  EXPECT_EQ(metrics.calls_started.Value(), 1);
  EXPECT_EQ(metrics.calls_completed.Value(), 1);
  EXPECT_EQ(metrics.requests_rejected_overload.Value(), 1);
  EXPECT_EQ(metrics.responses_enqueued.Value(), 2);
  EXPECT_EQ(metrics.call_errors.Value(), 0);

  channel.Close();
  server.Stop();
  EXPECT_TRUE(server.Join());
  scheduler.Stop();
}

TEST(RpcServerOptionsTest, PendingWorkerLimitReturnsOverloadAndBoundsQueue) {
  RpcServerOptions options;
  options.max_in_flight = 3;
  options.max_in_flight_per_connection = 3;
  options.max_pending_worker_tasks = 1;
  // The first service call occupies the only worker. The second is allowed to
  // wait in its queue; the third must produce an overload response instead
  // of growing that queue without bound.
  Scheduler scheduler {1, 2};
  Context& server_context = scheduler.GetIOContext(0);
  Context& client_context = scheduler.GetIOContext(1);
  RpcServer server(server_context, 0, butil::IP_ANY, options);
  auto service = std::make_shared<QueueSaturatingEchoService>();
  ASSERT_TRUE(server.AddService(service));
  ASSERT_TRUE(server.Start());
  scheduler.Start();

  RpcChannel first_channel(client_context);
  RpcChannel second_channel(client_context);
  RpcChannel third_channel(client_context);
  ASSERT_EQ(first_channel.Init("127.0.0.1", server.GetEndPoint().port), 0);
  ASSERT_EQ(second_channel.Init("127.0.0.1", server.GetEndPoint().port), 0);
  ASSERT_EQ(third_channel.Init("127.0.0.1", server.GetEndPoint().port), 0);
  ant_rpc::EchoService_Stub first_stub(&first_channel);
  ant_rpc::EchoService_Stub second_stub(&second_channel);
  ant_rpc::EchoService_Stub third_stub(&third_channel);

  ant_rpc::EchoRequest first_request;
  first_request.set_message("first");
  ant_rpc::EchoResponse first_response;
  RpcController first_controller;
  absl::Notification first_done;
  NotifyClosure first_closure(first_done);
  first_stub.Echo(&first_controller, &first_request, &first_response, &first_closure);
  ASSERT_TRUE(service->entered.WaitForNotificationWithTimeout(absl::Seconds(2)));

  ant_rpc::EchoRequest second_request;
  second_request.set_message("second");
  ant_rpc::EchoResponse second_response;
  RpcController second_controller;
  absl::Notification second_done;
  NotifyClosure second_closure(second_done);
  second_stub.Echo(&second_controller, &second_request, &second_response, &second_closure);

  bool second_is_queued = false;
  for (int attempt = 0; attempt != 200; ++attempt) {
    if (server.metrics().pending_worker_tasks.Value() == 1) {
      second_is_queued = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_TRUE(second_is_queued);

  ant_rpc::EchoRequest third_request;
  third_request.set_message("third");
  ant_rpc::EchoResponse third_response;
  RpcController third_controller;
  absl::Notification third_done;
  NotifyClosure third_closure(third_done);
  third_stub.Echo(&third_controller, &third_request, &third_response, &third_closure);

  // Client continuations share this test's sole worker with the intentionally
  // blocked service, so the callback itself cannot run until release. The
  // server must nevertheless reject and queue its wire response immediately.
  bool overload_response_enqueued = false;
  for (int attempt = 0; attempt != 200; ++attempt) {
    if (server.metrics().requests_rejected_overload.Value() == 1 && server.metrics().responses_enqueued.Value() == 1) {
      overload_response_enqueued = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_TRUE(overload_response_enqueued);
  EXPECT_EQ(server.metrics().requests_received.Value(), 3);
  EXPECT_EQ(server.metrics().requests_rejected_overload.Value(), 1);

  service->release.Notify();
  ASSERT_TRUE(first_done.WaitForNotificationWithTimeout(absl::Seconds(2)));
  ASSERT_TRUE(second_done.WaitForNotificationWithTimeout(absl::Seconds(2)));
  ASSERT_TRUE(third_done.WaitForNotificationWithTimeout(absl::Seconds(2)));
  EXPECT_FALSE(first_controller.Failed()) << first_controller.ErrorText();
  EXPECT_FALSE(second_controller.Failed()) << second_controller.ErrorText();
  EXPECT_TRUE(third_controller.Failed());
  EXPECT_EQ(third_controller.ErrorCode(), RPC_EOVERLOAD);
  EXPECT_EQ(first_response.message(), "Echo: first");
  EXPECT_EQ(second_response.message(), "Echo: second");
  EXPECT_EQ(server.metrics().pending_worker_tasks.Value(), 0);
  EXPECT_EQ(server.metrics().calls_started.Value(), 2);
  EXPECT_EQ(server.metrics().calls_completed.Value(), 2);
  EXPECT_EQ(server.metrics().requests_rejected_overload.Value(), 1);

  first_channel.Close();
  second_channel.Close();
  third_channel.Close();
  server.Stop();
  EXPECT_TRUE(server.Join());
  scheduler.Stop();
}

TEST(RpcServerOptionsTest, OversizedResponseForConnectionOutboundLimitClosesConnection) {
  RpcServerOptions options;
  options.max_outbound_bytes_per_connection = kRpcHeaderBytes;
  Scheduler scheduler {1, 2};
  Context& server_context = scheduler.GetIOContext(0);
  Context& client_context = scheduler.GetIOContext(1);
  RpcServer server(server_context, 0, butil::IP_ANY, options);
  auto service = std::make_shared<EchoServiceImpl>();
  ASSERT_TRUE(server.AddService(service));
  ASSERT_TRUE(server.Start());
  scheduler.Start();

  RpcChannel channel(client_context);
  ASSERT_EQ(channel.Init("127.0.0.1", server.GetEndPoint().port), 0);
  ant_rpc::EchoService_Stub stub(&channel);
  RpcController controller;
  ant_rpc::EchoRequest request;
  ant_rpc::EchoResponse response;
  request.set_message(std::string(1024, 'x'));
  stub.Echo(&controller, &request, &response, nullptr);

  // The service can finish, but its complete response cannot be placed in the
  // per-connection outbound budget. The server closes instead of retaining an
  // unbounded frame or silently dropping the response.
  EXPECT_TRUE(controller.Failed());
  const auto& metrics = server.metrics();
  EXPECT_EQ(metrics.calls_started.Value(), 1);
  EXPECT_EQ(metrics.calls_completed.Value(), 1);
  EXPECT_EQ(metrics.call_errors.Value(), 1);
  EXPECT_EQ(metrics.responses_enqueued.Value(), 0);
  EXPECT_EQ(metrics.outbound_bytes.Value(), 0);

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

TEST(RpcServerServiceTest, AsyncDoneFromAnotherThreadCompletesExactlyOnce) {
  Scheduler scheduler {2, 2};
  Context& server_context = scheduler.GetIOContext(0);
  Context& client_context = scheduler.GetIOContext(1);
  RpcServer server(server_context, 0);
  auto service = std::make_shared<AsyncDoneEchoService>();
  ASSERT_TRUE(server.AddService(service));
  ASSERT_TRUE(server.Start());
  scheduler.Start();

  RpcChannel channel(client_context);
  ASSERT_EQ(channel.Init("127.0.0.1", server.GetEndPoint().port), 0);
  ant_rpc::EchoService_Stub stub(&channel);
  RpcController controller;
  ant_rpc::EchoRequest request;
  ant_rpc::EchoResponse response;
  request.set_message("later");
  absl::Notification done;
  std::atomic<int> completion_count {0};
  CountingNotifyClosure closure(done, completion_count);

  stub.Echo(&controller, &request, &response, &closure);
  ASSERT_TRUE(done.WaitForNotificationWithTimeout(absl::Seconds(2)));
  service->Join();
  EXPECT_FALSE(controller.Failed()) << controller.ErrorText();
  EXPECT_EQ(response.message(), "Async: later");
  EXPECT_EQ(completion_count.load(std::memory_order_acquire), 1);

  channel.Close();
  server.Stop();
  EXPECT_TRUE(server.Join());
  scheduler.Stop();
}

TEST(RpcServerServiceTest, LateDoneAfterClientDisconnectIsSafelyDiscarded) {
  Scheduler scheduler {2, 2};
  Context& server_context = scheduler.GetIOContext(0);
  Context& client_context = scheduler.GetIOContext(1);
  RpcServer server(server_context, 0);
  auto service = std::make_shared<DeferredDoneEchoService>();
  ASSERT_TRUE(server.AddService(service));
  ASSERT_TRUE(server.Start());
  scheduler.Start();

  RpcChannel channel(client_context);
  ASSERT_EQ(channel.Init("127.0.0.1", server.GetEndPoint().port), 0);
  ant_rpc::EchoService_Stub stub(&channel);
  RpcController controller;
  ant_rpc::EchoRequest request;
  ant_rpc::EchoResponse response;
  request.set_message("abandoned");
  absl::Notification callback_done;
  NotifyClosure closure(callback_done);
  stub.Echo(&controller, &request, &response, &closure);
  ASSERT_TRUE(service->entered.WaitForNotificationWithTimeout(absl::Seconds(2)));

  // Closing the peer must not destroy the state that the service still uses
  // through response and done. The eventual response is simply not written.
  channel.Close();
  ASSERT_TRUE(callback_done.WaitForNotificationWithTimeout(absl::Seconds(2)));
  service->release.Notify();
  service->Join();

  server.Stop();
  EXPECT_TRUE(server.Join());
  scheduler.Stop();
}

TEST(RpcServerLifecycleTest, StopClosesConnectionsButWaitsForRunningService) {
  RpcServerOptions options;
  options.graceful_stop_timeout = std::chrono::milliseconds(20);
  Scheduler scheduler {2, 2};
  Context& server_context = scheduler.GetIOContext(0);
  Context& client_context = scheduler.GetIOContext(1);
  RpcServer server(server_context, 0, butil::IP_ANY, options);
  auto service = std::make_shared<DeferredDoneEchoService>();
  ASSERT_TRUE(server.AddService(service));
  ASSERT_TRUE(server.Start());
  scheduler.Start();

  RpcChannel channel(client_context);
  ASSERT_EQ(channel.Init("127.0.0.1", server.GetEndPoint().port), 0);
  ant_rpc::EchoService_Stub stub(&channel);
  RpcController controller;
  ant_rpc::EchoRequest request;
  ant_rpc::EchoResponse response;
  request.set_message("finish-after-stop");
  absl::Notification callback_done;
  NotifyClosure closure(callback_done);
  stub.Echo(&controller, &request, &response, &closure);
  ASSERT_TRUE(service->entered.WaitForNotificationWithTimeout(absl::Seconds(2)));

  server.Stop();
  EXPECT_EQ(server.status(), RpcServer::Status::kStopping);

  // The graceful deadline closes the transport and unblocks the client, but
  // must not terminate user code. The service is still waiting on release.
  ASSERT_TRUE(callback_done.WaitForNotificationWithTimeout(absl::Seconds(2)));
  EXPECT_TRUE(controller.Failed());

  service->release.Notify();
  service->Join();
  EXPECT_TRUE(server.Join());
  EXPECT_EQ(server.metrics().closed_connections.Value(), 1);
  EXPECT_EQ(server.metrics().ConnectionCloses(ConnectionCloseReason::kServerStop), 1);
  EXPECT_EQ(server.metrics().io_command_backlog.Value(), 0);
  channel.Close();
  scheduler.Stop();
}
