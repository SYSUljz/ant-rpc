#pragma once

#include <memory>
#include <string>
#include <string_view>

#include <absl/container/flat_hash_map.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

namespace ant_server::rpc {

class ServiceRegistry {
 public:
  struct MethodLookup {
    google::protobuf::Service* service {nullptr};
    const google::protobuf::MethodDescriptor* method {nullptr};
  };
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

  // Resolve descriptors only. Request/response/controller lifetime belongs to
  // ServerConnection::InboundCallState, not to this registry.
  [[nodiscard]] MethodLookup FindMethod(std::string_view service_name, std::string_view method_name) const {
    auto* service = FindService(service_name);
    if (!service) {
      return {};
    }
    const auto* descriptor = service->GetDescriptor();
    return {service, descriptor ? descriptor->FindMethodByName(method_name) : nullptr};
  }

 private:
  absl::flat_hash_map<std::string, google::protobuf::Service*> services_;
};

}  // namespace ant_server::rpc

namespace ant_rpc {
using namespace ant_server::rpc;
}
