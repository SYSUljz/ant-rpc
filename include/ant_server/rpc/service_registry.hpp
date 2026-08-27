#pragma once

#include <memory>
#include <string>
#include <string_view>

#include <absl/container/flat_hash_map.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include "ant_server/rpc/controller.hpp"
#include "ant_server/rpc/error_code.hpp"
#include "ant_server/rpc/protocol.hpp"
#include "butil/iobuf.h"

namespace ant_server::rpc {

class ServiceRegistry {
 public:
  ServiceRegistry() = default;

  // Register a Protobuf RPC Service implementation
  bool RegisterService(google::protobuf::Service* service) {
    if (!service) {
      return false;
    }
    const auto* desc = service->GetDescriptor();
    if (!desc) {
      return false;
    }

    return services_.try_emplace(desc->full_name(), service).second;
  }

  // Find a service by full name (Zero-allocation lookup with string_view)
  google::protobuf::Service* FindService(std::string_view service_name) const {
    auto it = services_.find(service_name);
    if (it != services_.end()) {
      return it->second;
    }
    return nullptr;
  }

  // Dispatch and execute an RPC request
  bool Dispatch(FrameParseResult& req_frame, butil::IOBuf& resp_out_buf) {
    const std::string& service_name = req_frame.meta.service_name();
    const std::string& method_name = req_frame.meta.method_name();
    uint64_t correlation_id = req_frame.meta.correlation_id();

    RpcMeta resp_meta;
    resp_meta.set_msg_type(RPC_RESPONSE);
    resp_meta.set_correlation_id(correlation_id);
    resp_meta.set_service_name(service_name);
    resp_meta.set_method_name(method_name);

    auto* service = FindService(service_name);
    if (!service) {
      resp_meta.set_error_code(RPC_ENOSERVICE);
      resp_meta.set_error_text("Service not found: " + service_name);
      return PackRpcFrame(resp_meta, nullptr, nullptr, resp_out_buf);
    }

    const auto* service_desc = service->GetDescriptor();
    const auto* method_desc = service_desc->FindMethodByName(method_name);
    if (!method_desc) {
      resp_meta.set_error_code(RPC_ENOMETHOD);
      resp_meta.set_error_text("Method not found: " + method_name);
      return PackRpcFrame(resp_meta, nullptr, nullptr, resp_out_buf);
    }

    // 1. Dynamically create Request and Response prototypes
    std::unique_ptr<google::protobuf::Message> req_msg(service->GetRequestPrototype(method_desc).New());
    std::unique_ptr<google::protobuf::Message> resp_msg(service->GetResponsePrototype(method_desc).New());

    // 2. Zero-Copy deserialize from request body IOBuf
    if (!req_frame.body_iobuf.empty()) {
      butil::IOBufAsZeroCopyInputStream zc_in(req_frame.body_iobuf);
      if (!req_msg->ParseFromZeroCopyStream(&zc_in)) {
        resp_meta.set_error_code(RPC_EINVALID_DATA);
        resp_meta.set_error_text("Failed to parse request protobuf");
        return PackRpcFrame(resp_meta, nullptr, nullptr, resp_out_buf);
      }
    }

    // 3. Initialize Controller & transfer request headers (Zero-copy)
    RpcController cntl;
    cntl.SetCorrelationId(correlation_id);
    cntl.SetLogId(req_frame.meta.log_id());
    cntl.SetTimeoutMs(req_frame.meta.timeout_ms());
    if (!req_frame.meta.headers().empty()) {
      cntl.MutableRequestHeaders().swap(*req_frame.meta.mutable_headers());
    }
    if (!req_frame.attachment_iobuf.empty()) {
      cntl.RequestAttachment() = std::move(req_frame.attachment_iobuf);
    }

    // 4. Invoke user RPC service implementation
    service->CallMethod(method_desc, &cntl, req_msg.get(), resp_msg.get(), nullptr);

    // 5. Populate response metadata from Controller
    resp_meta.set_error_code(cntl.ErrorCode());
    resp_meta.set_error_text(cntl.ErrorText());
    if (!cntl.ResponseHeaders().empty()) {
      resp_meta.mutable_headers()->swap(cntl.MutableResponseHeaders());
    }

    // 6. Pack response frame
    const butil::IOBuf* resp_attach = !cntl.ResponseAttachment().empty() ? &cntl.ResponseAttachment() : nullptr;
    return PackRpcFrame(resp_meta, resp_msg.get(), resp_attach, resp_out_buf);
  }

 private:
  absl::flat_hash_map<std::string, google::protobuf::Service*> services_;
};

}  // namespace ant_server::rpc

namespace ant_rpc {
using namespace ant_server::rpc;
}
