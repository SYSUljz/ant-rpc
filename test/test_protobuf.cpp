#include <array>
#include <limits>
#include <memory>
#include <string>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <gtest/gtest.h>

#include "ant_server/rpc/protocol.hpp"
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

TEST(ProtobufTest, FrameParserRejectsConfiguredMaxFrameBytes) {
  ant_rpc::EchoRequest request;
  request.set_message("frame-limit");
  butil::IOBuf frame;
  ant_rpc::PackRpcFrame(/*msg_type=*/0, /*correlation_id=*/7, "ant_rpc.EchoService", "Echo", request, frame);

  ASSERT_GT(frame.size(), sizeof(ant_server::rpc::RpcHeader));
  const auto result = ant_server::rpc::TryParseRpcFrame(frame, frame.size() - 1);
  EXPECT_EQ(result.status, ant_server::rpc::FrameParseStatus::ERROR_FRAME_TOO_LARGE);
}

TEST(ProtobufTest, WireHeaderUsesNetworkByteOrderAndVersionOne) {
  ant_rpc::EchoRequest request;
  request.set_message("wire-order");
  butil::IOBuf frame;
  ASSERT_TRUE(ant_server::rpc::PackRpcFrame(/*msg_type=*/0, /*correlation_id=*/7, "ant_rpc.EchoService", "Echo",
                                            request, frame));

  std::array<unsigned char, ant_server::rpc::kRpcHeaderBytes> header {};
  frame.copy_to(header.data(), header.size(), 0);
  EXPECT_EQ(header[0], 'A');
  EXPECT_EQ(header[1], 'N');
  EXPECT_EQ(header[2], 'T');
  EXPECT_EQ(header[3], 'R');
  EXPECT_EQ(header[12], ant_server::rpc::kRpcWireVersion);
  EXPECT_EQ(header[13], 0);
  EXPECT_EQ(header[14], 0);
  EXPECT_EQ(header[15], 0);

  const auto parsed = ant_server::rpc::TryParseRpcFrame(frame);
  ASSERT_EQ(parsed.status, ant_server::rpc::FrameParseStatus::SUCCESS);
  EXPECT_EQ(parsed.header.flags, ant_server::rpc::MakeWireFlags());
  ant_rpc::EchoRequest decoded;
  butil::IOBufAsZeroCopyInputStream body_stream(parsed.body_iobuf);
  ASSERT_TRUE(decoded.ParseFromZeroCopyStream(&body_stream));
  EXPECT_EQ(decoded.message(), "wire-order");
}

TEST(ProtobufTest, RejectsInvalidMagicBeforeReadingDeclaredPayload) {
  ant_rpc::EchoRequest request;
  request.set_message("bad-magic");
  butil::IOBuf frame;
  ASSERT_TRUE(ant_server::rpc::PackRpcFrame(/*msg_type=*/0, /*correlation_id=*/7, "ant_rpc.EchoService", "Echo",
                                            request, frame));
  std::string bytes = frame.to_string();
  bytes[0] = 'X';
  butil::IOBuf invalid;
  invalid.append(bytes.data(), bytes.size());

  EXPECT_EQ(ant_server::rpc::TryParseRpcFrame(invalid).status, ant_server::rpc::FrameParseStatus::ERROR_MAGIC_MISMATCH);
}

TEST(ProtobufTest, FrameLengthAndAttachmentBoundaryRoundTrip) {
  ant_server::rpc::RpcMeta meta;
  meta.set_msg_type(ant_server::rpc::RPC_REQUEST);
  meta.set_correlation_id(99);
  butil::IOBuf body;
  body.append("body", 4);
  butil::IOBuf attachment;
  attachment.append("attachment", 10);
  butil::IOBuf frame;
  ASSERT_TRUE(ant_server::rpc::PackRpcFrame(meta, body, &attachment, frame));

  const auto parsed = ant_server::rpc::TryParseRpcFrame(frame);
  ASSERT_EQ(parsed.status, ant_server::rpc::FrameParseStatus::SUCCESS);
  EXPECT_EQ(parsed.header.body_len, body.size() + attachment.size());
  EXPECT_EQ(parsed.meta.attachment_size(), attachment.size());
  EXPECT_EQ(parsed.total_frame_bytes, frame.size());
  EXPECT_EQ(parsed.body_iobuf.to_string(), "body");
  EXPECT_EQ(parsed.attachment_iobuf.to_string(), "attachment");
}

TEST(ProtobufTest, RejectsUnsupportedWireVersionAndFeatureFlags) {
  ant_rpc::EchoRequest request;
  request.set_message("invalid-flags");
  butil::IOBuf valid;
  ASSERT_TRUE(ant_server::rpc::PackRpcFrame(/*msg_type=*/0, /*correlation_id=*/7, "ant_rpc.EchoService", "Echo",
                                            request, valid));
  std::string bytes = valid.to_string();

  bytes[12] = 2;  // Version is the most-significant byte of flags.
  butil::IOBuf unsupported_version;
  unsupported_version.append(bytes.data(), bytes.size());
  EXPECT_EQ(ant_server::rpc::TryParseRpcFrame(unsupported_version).status,
            ant_server::rpc::FrameParseStatus::ERROR_UNSUPPORTED_VERSION);

  bytes[12] = ant_server::rpc::kRpcWireVersion;
  bytes[15] = 1;  // Lowest feature bit is unknown in V1.
  butil::IOBuf unsupported_flag;
  unsupported_flag.append(bytes.data(), bytes.size());
  EXPECT_EQ(ant_server::rpc::TryParseRpcFrame(unsupported_flag).status,
            ant_server::rpc::FrameParseStatus::ERROR_UNSUPPORTED_FLAGS);
}

TEST(ProtobufTest, RejectsAttachmentPastBodyAndDoesNotPackPastLimit) {
  ant_server::rpc::RpcMeta meta;
  meta.set_msg_type(ant_server::rpc::RPC_REQUEST);
  meta.set_correlation_id(7);
  meta.set_attachment_size(100);
  std::string serialized_meta;
  ASSERT_TRUE(meta.SerializeToString(&serialized_meta));

  std::array<unsigned char, ant_server::rpc::kRpcHeaderBytes> header {
      'A', 'N', 'T', 'R', 0, 0, 0, 1, 0, 0, 0, static_cast<unsigned char>(serialized_meta.size()), 1, 0, 0, 0};
  butil::IOBuf malformed;
  malformed.append(header.data(), header.size());
  malformed.append(serialized_meta.data(), serialized_meta.size());
  malformed.append("x", 1);
  EXPECT_EQ(ant_server::rpc::TryParseRpcFrame(malformed).status,
            ant_server::rpc::FrameParseStatus::ERROR_INVALID_LENGTH);

  ant_rpc::EchoRequest request;
  request.set_message("will-not-fit");
  butil::IOBuf output;
  ASSERT_FALSE(ant_server::rpc::PackRpcFrame(/*msg_type=*/0, /*correlation_id=*/7, "ant_rpc.EchoService", "Echo",
                                             request, output, ant_server::rpc::kRpcHeaderBytes));
  EXPECT_TRUE(output.empty());
}

TEST(ProtobufTest, RejectsOversizedDeclaredLengthsWithoutWaitingForPayload) {
  std::array<unsigned char, ant_server::rpc::kRpcHeaderBytes> header {};
  header[0] = 'A';
  header[1] = 'N';
  header[2] = 'T';
  header[3] = 'R';
  // body_len and meta_len are both UINT32_MAX in big-endian form.
  for (int index = 4; index < 12; ++index) {
    header[index] = 0xFF;
  }
  header[12] = ant_server::rpc::kRpcWireVersion;
  butil::IOBuf malicious;
  malicious.append(header.data(), header.size());

  const auto parsed = ant_server::rpc::TryParseRpcFrame(malicious);
  EXPECT_EQ(parsed.status, ant_server::rpc::FrameParseStatus::ERROR_FRAME_TOO_LARGE);

  // The packer also rejects a frame before it emits a header when its complete
  // wire length cannot fit under the configured limit.
  ant_server::rpc::RpcMeta meta;
  meta.set_msg_type(ant_server::rpc::RPC_REQUEST);
  meta.set_correlation_id(1);
  butil::IOBuf body;
  body.append("0123456789", 10);
  butil::IOBuf output;
  EXPECT_FALSE(ant_server::rpc::PackRpcFrame(meta, body, nullptr, output, 20));
  EXPECT_TRUE(output.empty());
}

TEST(ProtobufTest, HandlesPartialAndBackToBackFrames) {
  ant_rpc::EchoRequest first_request;
  first_request.set_message("first");
  ant_rpc::EchoRequest second_request;
  second_request.set_message("second");
  butil::IOBuf first;
  butil::IOBuf second;
  ASSERT_TRUE(ant_server::rpc::PackRpcFrame(/*msg_type=*/0, /*correlation_id=*/1, "ant_rpc.EchoService", "Echo",
                                            first_request, first));
  ASSERT_TRUE(ant_server::rpc::PackRpcFrame(/*msg_type=*/0, /*correlation_id=*/2, "ant_rpc.EchoService", "Echo",
                                            second_request, second));

  const std::string first_bytes = first.to_string();
  const std::size_t split = first_bytes.size() / 2;
  butil::IOBuf receive_buffer;
  receive_buffer.append(first_bytes.data(), split);
  const auto partial = ant_server::rpc::TryParseRpcFrame(receive_buffer);
  EXPECT_EQ(partial.status, ant_server::rpc::FrameParseStatus::NEED_MORE_DATA);
  EXPECT_EQ(partial.total_frame_bytes, first.size());

  receive_buffer.append(first_bytes.data() + split, first_bytes.size() - split);
  receive_buffer.append(second);
  const auto parsed_first = ant_server::rpc::TryParseRpcFrame(receive_buffer);
  ASSERT_EQ(parsed_first.status, ant_server::rpc::FrameParseStatus::SUCCESS);
  EXPECT_EQ(parsed_first.meta.correlation_id(), 1);
  receive_buffer.pop_front(parsed_first.total_frame_bytes);

  const auto parsed_second = ant_server::rpc::TryParseRpcFrame(receive_buffer);
  ASSERT_EQ(parsed_second.status, ant_server::rpc::FrameParseStatus::SUCCESS);
  EXPECT_EQ(parsed_second.meta.correlation_id(), 2);
  EXPECT_EQ(parsed_second.total_frame_bytes, receive_buffer.size());
}
