#include <stop_token>
#include <string>

#include <gtest/gtest.h>

#include "ant_server/rpc/controller.hpp"
#include "ant_server/rpc/error_code.hpp"

using namespace ant_server::rpc;

// 1. Test Default State of RpcController
TEST(RpcControllerTest, DefaultState) {
  RpcController cntl;

  EXPECT_FALSE(cntl.Failed());
  EXPECT_EQ(cntl.ErrorCode(), RPC_SUCCESS);
  EXPECT_TRUE(cntl.ErrorText().empty());
  EXPECT_EQ(cntl.CorrelationId(), 0);
  EXPECT_EQ(cntl.LogId(), 0);
  EXPECT_EQ(cntl.TimeoutMs(), -1);
  EXPECT_TRUE(cntl.RequestAttachment().empty());
  EXPECT_TRUE(cntl.ResponseAttachment().empty());
  EXPECT_TRUE(cntl.Headers().empty());
  EXPECT_FALSE(cntl.IsCanceled());
}

// 2. Test Error Setting and Error Code String conversion
TEST(RpcControllerTest, ErrorHandling) {
  RpcController cntl;

  cntl.SetFailed(RPC_ENOMETHOD, "Method Echo not found in Service");
  EXPECT_TRUE(cntl.Failed());
  EXPECT_EQ(cntl.ErrorCode(), RPC_ENOMETHOD);
  EXPECT_EQ(cntl.ErrorText(), "Method Echo not found in Service");

  EXPECT_EQ(RpcErrorToString(RPC_SUCCESS), "RPC_SUCCESS");
  EXPECT_EQ(RpcErrorToString(RPC_ENOSERVICE), "RPC_ENOSERVICE");
  EXPECT_EQ(RpcErrorToString(RPC_ENOMETHOD), "RPC_ENOMETHOD");
  EXPECT_EQ(RpcErrorToString(RPC_ETIMEOUT), "RPC_ETIMEOUT");
  EXPECT_EQ(RpcErrorToString(9999), "RPC_EUNKNOWN");
}

// 3. Test IDs, Endpoints, and Metadata Headers
TEST(RpcControllerTest, MetadataAndEndpoints) {
  RpcController cntl;

  cntl.SetCorrelationId(123456789ULL);
  cntl.SetLogId(987654321ULL);
  EXPECT_EQ(cntl.CorrelationId(), 123456789ULL);
  EXPECT_EQ(cntl.LogId(), 987654321ULL);

  butil::EndPoint ep(butil::IP_ANY, 8012);
  cntl.SetRemoteSide(ep);
  EXPECT_EQ(cntl.RemoteSide().port, 8012);

  // Headers / KV metadata
  cntl.SetHeader("authorization", "Bearer my_secret_token");
  cntl.SetHeader("trace_id", "trace-xyz-100");

  EXPECT_EQ(cntl.Headers().size(), 2);
  auto auth = cntl.GetHeader("authorization");
  ASSERT_TRUE(auth.has_value());
  EXPECT_EQ(*auth, "Bearer my_secret_token");

  auto trace = cntl.GetHeader("trace_id");
  ASSERT_TRUE(trace.has_value());
  EXPECT_EQ(*trace, "trace-xyz-100");

  EXPECT_FALSE(cntl.GetHeader("non_existent_key").has_value());
}

// 4. Test Zero-Copy IOBuf Attachments
TEST(RpcControllerTest, IOBufAttachments) {
  RpcController cntl;

  cntl.RequestAttachment().append("Hello Attachment Data");
  EXPECT_EQ(cntl.RequestAttachment().size(), 21);

  cntl.ResponseAttachment().append("Response Binary Data 123");
  EXPECT_EQ(cntl.ResponseAttachment().size(), 24);
}

// 5. Test C++20 StopToken Integration
TEST(RpcControllerTest, StopTokenCancellation) {
  RpcController cntl;
  std::stop_source source;

  cntl.SetStopToken(source.get_token());
  EXPECT_FALSE(cntl.IsCanceled());

  // Trigger stop via stop_source
  source.request_stop();
  EXPECT_TRUE(cntl.IsCanceled());
}

// 6. Test Reset Functionality for Controller Reusability
TEST(RpcControllerTest, ResetCleansAllState) {
  RpcController cntl;

  cntl.SetFailed(RPC_EINTERNAL, "Database query timed out");
  cntl.SetCorrelationId(101);
  cntl.SetLogId(202);
  cntl.SetTimeoutMs(5000);
  cntl.SetHeader("k", "v");
  cntl.RequestAttachment().append("payload");
  cntl.ResponseAttachment().append("resp");

  std::stop_source source;
  cntl.SetStopToken(source.get_token());

  // Call Reset()
  cntl.Reset();

  EXPECT_FALSE(cntl.Failed());
  EXPECT_EQ(cntl.ErrorCode(), RPC_SUCCESS);
  EXPECT_TRUE(cntl.ErrorText().empty());
  EXPECT_EQ(cntl.CorrelationId(), 0);
  EXPECT_EQ(cntl.LogId(), 0);
  EXPECT_EQ(cntl.TimeoutMs(), -1);
  EXPECT_TRUE(cntl.Headers().empty());
  EXPECT_TRUE(cntl.RequestAttachment().empty());
  EXPECT_TRUE(cntl.ResponseAttachment().empty());
  EXPECT_FALSE(cntl.IsCanceled());
}
