#include <memory>
#include <string>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <gtest/gtest.h>

#include "butil/iobuf.h"
#include "echo.pb.h"

// Implementation of EchoService
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

TEST(ProtobufTest, BasicSerializationAndDeserialization) {
  ant_rpc::EchoRequest req;
  req.set_message("Hello Ant RPC!");

  std::string serialized;
  ASSERT_TRUE(req.SerializeToString(&serialized));

  ant_rpc::EchoRequest parsed_req;
  ASSERT_TRUE(parsed_req.ParseFromString(serialized));
  EXPECT_EQ(parsed_req.message(), "Hello Ant RPC!");
}

TEST(ProtobufTest, ServiceReflectionAndDynamicInvocation) {
  EchoServiceImpl service;

  // 1. Get ServiceDescriptor via reflection
  const google::protobuf::ServiceDescriptor* service_desc = service.GetDescriptor();
  ASSERT_NE(service_desc, nullptr);
  EXPECT_EQ(service_desc->full_name(), "ant_rpc.EchoService");

  // 2. Find Method by name
  const google::protobuf::MethodDescriptor* method_desc = service_desc->FindMethodByName("Echo");
  ASSERT_NE(method_desc, nullptr);
  EXPECT_EQ(method_desc->name(), "Echo");

  // 3. Dynamically instantiate Request and Response prototypes
  std::unique_ptr<google::protobuf::Message> req_msg(service.GetRequestPrototype(method_desc).New());
  std::unique_ptr<google::protobuf::Message> resp_msg(service.GetResponsePrototype(method_desc).New());

  // 4. Fill in dynamic request data
  auto* req = dynamic_cast<ant_rpc::EchoRequest*>(req_msg.get());
  ASSERT_NE(req, nullptr);
  req->set_message("Reflection call test");

  // 5. Invoke via CallMethod (as an RPC framework dispatcher would do)
  service.CallMethod(method_desc, nullptr, req_msg.get(), resp_msg.get(), nullptr);

  // 6. Verify dynamic response
  auto* resp = dynamic_cast<ant_rpc::EchoResponse*>(resp_msg.get());
  ASSERT_NE(resp, nullptr);
  EXPECT_EQ(resp->message(), "Echo: Reflection call test");
}
