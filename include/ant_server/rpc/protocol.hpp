#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <google/protobuf/message.h>

#include "ant_server/rpc/error_code.hpp"
#include "butil/iobuf.h"
#include "rpc_meta.pb.h"

namespace ant_server::rpc {

// ============================================================================
// RPC Wire Protocol Specification (bRPC-inspired baidu_std standard format):
//
// +--------------------+--------------------+--------------------+--------------------+
// |  Magic (4 Bytes)   |  Body Len (4B)     |  Meta Len (4B)     |  Flags (4B)        |
// |  "ANTR" (0x52544E41|  ProtoBody+Attach  |  RpcMeta PB Size   |  Reserved/Flags    |
// +--------------------+--------------------+--------------------+--------------------+
// |  Meta: Protobuf RpcMeta (correlation_id, service_name, method_name, error, etc.)   |
// +------------------------------------------------------------------------------------+
// |  Body: Protobuf Business Message (Request / Response)                              |
// +------------------------------------------------------------------------------------+
// |  Attachment (Optional): Zero-Copy Raw Binary Payload (butil::IOBuf)                |
// +------------------------------------------------------------------------------------+
// ============================================================================

#pragma pack(push, 1)
struct RpcHeader {
  static constexpr uint32_t MAGIC = 0x52544E41;  // "ANTR" in little-endian

  uint32_t magic {MAGIC};
  uint32_t body_len {0};  // Total length of Protobuf serialized body + attachment
  uint32_t meta_len {0};  // Length of Protobuf serialized RpcMeta
  uint32_t flags {0};     // Reserved / Flags
};
#pragma pack(pop)

static_assert(sizeof(RpcHeader) == 16, "RpcHeader must be exactly 16 bytes");

enum class FrameParseStatus {
  SUCCESS,
  NEED_MORE_DATA,
  ERROR_MAGIC_MISMATCH,
  ERROR_CORRUPTED_FRAME
};

struct FrameParseResult {
  FrameParseStatus status {FrameParseStatus::NEED_MORE_DATA};
  size_t total_frame_bytes {0};
  RpcHeader header {};
  RpcMeta meta {};
  butil::IOBuf body_iobuf;        // Protobuf business body
  butil::IOBuf attachment_iobuf;  // Zero-copy raw binary payload
};

// Try parsing one complete RPC frame from IOBuf
inline FrameParseResult TryParseRpcFrame(const butil::IOBuf& buf) {
  if (buf.size() < sizeof(RpcHeader)) {
    return {FrameParseStatus::NEED_MORE_DATA, 0, {}, {}, {}, {}};
  }

  RpcHeader header;
  buf.copy_to(&header, sizeof(RpcHeader), 0);

  if (header.magic != RpcHeader::MAGIC) {
    return {FrameParseStatus::ERROR_MAGIC_MISMATCH, 0, {}, {}, {}, {}};
  }

  size_t total_needed = sizeof(RpcHeader) + header.meta_len + header.body_len;
  if (buf.size() < total_needed) {
    return {FrameParseStatus::NEED_MORE_DATA, total_needed, header, {}, {}, {}};
  }

  // 1. Zero-copy extract and deserialize RpcMeta
  RpcMeta meta;
  if (header.meta_len > 0) {
    butil::IOBuf meta_buf;
    buf.append_to(&meta_buf, header.meta_len, sizeof(RpcHeader));
    butil::IOBufAsZeroCopyInputStream zc_meta(meta_buf);
    if (!meta.ParseFromZeroCopyStream(&zc_meta)) {
      return {FrameParseStatus::ERROR_CORRUPTED_FRAME, total_needed, header, {}, {}, {}};
    }
  }

  uint32_t attachment_size = meta.attachment_size();
  if (header.body_len < attachment_size) {
    return {FrameParseStatus::ERROR_CORRUPTED_FRAME, total_needed, header, {}, {}, {}};
  }

  uint32_t proto_body_len = header.body_len - attachment_size;
  size_t body_offset = sizeof(RpcHeader) + header.meta_len;

  // 2. Extract Protobuf body (Zero-Copy slice)
  butil::IOBuf body_iobuf;
  if (proto_body_len > 0) {
    buf.append_to(&body_iobuf, proto_body_len, body_offset);
  }

  // 3. Extract Attachment (Zero-Copy slice)
  butil::IOBuf attachment_iobuf;
  if (attachment_size > 0) {
    buf.append_to(&attachment_iobuf, attachment_size, body_offset + proto_body_len);
  }

  return {FrameParseStatus::SUCCESS, total_needed, header, std::move(meta),
          std::move(body_iobuf), std::move(attachment_iobuf)};
}

// Pack an RPC frame into target IOBuf
inline void PackRpcFrame(RpcMeta& meta, const google::protobuf::Message* message,
                         const butil::IOBuf* attachment, butil::IOBuf& out_buf) {
  uint32_t attachment_size = attachment ? static_cast<uint32_t>(attachment->size()) : 0;
  meta.set_attachment_size(attachment_size);

  butil::IOBuf meta_buf;
  {
    butil::IOBufAsZeroCopyOutputStream zc_meta(&meta_buf);
    meta.SerializeToZeroCopyStream(&zc_meta);
  }

  butil::IOBuf body_buf;
  if (message) {
    butil::IOBufAsZeroCopyOutputStream zc_body(&body_buf);
    message->SerializeToZeroCopyStream(&zc_body);
  }

  RpcHeader header;
  header.magic = RpcHeader::MAGIC;
  header.meta_len = static_cast<uint32_t>(meta_buf.size());
  header.body_len = static_cast<uint32_t>(body_buf.size()) + attachment_size;
  header.flags = 0;

  out_buf.append(&header, sizeof(RpcHeader));
  out_buf.append(meta_buf);
  if (!body_buf.empty()) {
    out_buf.append(body_buf);
  }
  if (attachment && !attachment->empty()) {
    out_buf.append(*attachment);
  }
}

// Overload for raw IOBuf body payload
inline void PackRpcFrame(RpcMeta& meta, const butil::IOBuf& raw_body,
                         const butil::IOBuf* attachment, butil::IOBuf& out_buf) {
  uint32_t attachment_size = attachment ? static_cast<uint32_t>(attachment->size()) : 0;
  meta.set_attachment_size(attachment_size);

  butil::IOBuf meta_buf;
  {
    butil::IOBufAsZeroCopyOutputStream zc_meta(&meta_buf);
    meta.SerializeToZeroCopyStream(&zc_meta);
  }

  RpcHeader header;
  header.magic = RpcHeader::MAGIC;
  header.meta_len = static_cast<uint32_t>(meta_buf.size());
  header.body_len = static_cast<uint32_t>(raw_body.size()) + attachment_size;
  header.flags = 0;

  out_buf.append(&header, sizeof(RpcHeader));
  out_buf.append(meta_buf);
  if (!raw_body.empty()) {
    out_buf.append(raw_body);
  }
  if (attachment && !attachment->empty()) {
    out_buf.append(*attachment);
  }
}

// Convenience wrapper for basic RPC request / response messages
inline void PackRpcFrame(uint8_t msg_type, uint64_t correlation_id, std::string_view service_name,
                         std::string_view method_name, const google::protobuf::Message& message,
                         butil::IOBuf& out_buf) {
  RpcMeta meta;
  meta.set_msg_type(static_cast<RpcMessageType>(msg_type));
  meta.set_correlation_id(correlation_id);
  if (!service_name.empty()) {
    meta.set_service_name(std::string(service_name));
  }
  if (!method_name.empty()) {
    meta.set_method_name(std::string(method_name));
  }
  PackRpcFrame(meta, &message, nullptr, out_buf);
}

}  // namespace ant_server::rpc

namespace ant_rpc {
  using namespace ant_server::rpc;
}
