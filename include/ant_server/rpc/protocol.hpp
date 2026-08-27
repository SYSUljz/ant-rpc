#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <google/protobuf/message.h>

#include "butil/iobuf.h"
#include "rpc_meta.pb.h"

namespace ant_server::rpc {

// RPC wire format, version 1.
//
// Every multi-byte integer is encoded in network byte order (big-endian).
// The 16-byte header is followed by RpcMeta, protobuf/raw body, and an opaque
// attachment. body_len covers body + attachment; RpcMeta::attachment_size is
// their boundary and must not exceed body_len.
//
// flags layout: [ protocol version: 8 ][ feature flags: 24 ]. Version 1 has
// no feature flags. Unknown versions or features are rejected, never ignored.
struct RpcHeader {
  static constexpr uint32_t kMagic = 0x414E5452U;  // ASCII "ANTR"

  uint32_t magic {kMagic};
  uint32_t body_len {0};
  uint32_t meta_len {0};
  uint32_t flags {0};
};

inline constexpr std::size_t kRpcHeaderBytes = 16;
inline constexpr std::size_t kDefaultMaxRpcFrameBytes = 16U * 1024U * 1024U;
inline constexpr uint8_t kRpcWireVersion = 1;
inline constexpr uint32_t kWireVersionShift = 24;
inline constexpr uint32_t kWireFeatureMask = 0x00FFFFFFU;
inline constexpr uint32_t kKnownWireFeatures = 0;

[[nodiscard]] constexpr uint32_t MakeWireFlags(uint8_t version = kRpcWireVersion, uint32_t features = 0) noexcept {
  return (static_cast<uint32_t>(version) << kWireVersionShift) | (features & kWireFeatureMask);
}
[[nodiscard]] constexpr uint8_t WireVersion(uint32_t flags) noexcept {
  return static_cast<uint8_t>(flags >> kWireVersionShift);
}
[[nodiscard]] constexpr uint32_t WireFeatures(uint32_t flags) noexcept { return flags & kWireFeatureMask; }

enum class FrameParseStatus {
  SUCCESS,
  NEED_MORE_DATA,
  ERROR_MAGIC_MISMATCH,
  ERROR_UNSUPPORTED_VERSION,
  ERROR_UNSUPPORTED_FLAGS,
  ERROR_FRAME_TOO_LARGE,
  ERROR_INVALID_LENGTH,
  ERROR_CORRUPTED_FRAME,
};

struct FrameParseResult {
  FrameParseStatus status {FrameParseStatus::NEED_MORE_DATA};
  std::size_t total_frame_bytes {0};
  RpcHeader header {};
  RpcMeta meta {};
  butil::IOBuf body_iobuf;
  butil::IOBuf attachment_iobuf;
};

namespace detail {

[[nodiscard]] inline uint32_t LoadUint32BE(const std::array<std::uint8_t, kRpcHeaderBytes>& bytes,
                                           std::size_t offset) noexcept {
  return (static_cast<uint32_t>(bytes[offset]) << 24U) | (static_cast<uint32_t>(bytes[offset + 1]) << 16U) |
         (static_cast<uint32_t>(bytes[offset + 2]) << 8U) | static_cast<uint32_t>(bytes[offset + 3]);
}
inline void StoreUint32BE(std::array<std::uint8_t, kRpcHeaderBytes>& bytes, std::size_t offset,
                          uint32_t value) noexcept {
  bytes[offset] = static_cast<std::uint8_t>(value >> 24U);
  bytes[offset + 1] = static_cast<std::uint8_t>(value >> 16U);
  bytes[offset + 2] = static_cast<std::uint8_t>(value >> 8U);
  bytes[offset + 3] = static_cast<std::uint8_t>(value);
}
inline void AppendWireHeader(const RpcHeader& header, butil::IOBuf& out) {
  std::array<std::uint8_t, kRpcHeaderBytes> bytes {};
  StoreUint32BE(bytes, 0, header.magic);
  StoreUint32BE(bytes, 4, header.body_len);
  StoreUint32BE(bytes, 8, header.meta_len);
  StoreUint32BE(bytes, 12, header.flags);
  out.append(bytes.data(), bytes.size());
}
[[nodiscard]] inline bool FitsUint32(std::size_t value) noexcept {
  return value <= std::numeric_limits<uint32_t>::max();
}

inline bool PackSerializedFrame(RpcMeta& meta, const butil::IOBuf& body, const butil::IOBuf* attachment,
                                butil::IOBuf& out, std::size_t max_frame_bytes) {
  const std::size_t attachment_size = attachment ? attachment->size() : 0;
  if (!FitsUint32(attachment_size) || !FitsUint32(body.size()) ||
      attachment_size > std::numeric_limits<uint32_t>::max() - body.size()) {
    return false;
  }
  meta.set_attachment_size(static_cast<uint32_t>(attachment_size));

  butil::IOBuf meta_buf;
  {
    // The stream's destructor returns unused bytes in its current IOBuf block.
    // It must run before meta_buf.size() is used to calculate or append a frame.
    butil::IOBufAsZeroCopyOutputStream zc_meta(&meta_buf);
    if (!meta.SerializeToZeroCopyStream(&zc_meta)) {
      return false;
    }
  }
  if (meta_buf.empty() || !FitsUint32(meta_buf.size())) {
    return false;
  }

  const std::size_t payload_size = meta_buf.size() + body.size() + attachment_size;
  if (payload_size > max_frame_bytes || kRpcHeaderBytes > max_frame_bytes - payload_size) {
    return false;
  }

  RpcHeader header;
  header.meta_len = static_cast<uint32_t>(meta_buf.size());
  header.body_len = static_cast<uint32_t>(body.size() + attachment_size);
  header.flags = MakeWireFlags();
  AppendWireHeader(header, out);
  out.append(meta_buf);
  if (!body.empty()) {
    out.append(body);
  }
  if (attachment_size != 0) {
    out.append(*attachment);
  }
  return true;
}

}  // namespace detail

// Parse one frame without consuming `buf`. max_frame_bytes covers every byte
// on the wire: header + meta + body + attachment.
inline FrameParseResult TryParseRpcFrame(const butil::IOBuf& buf,
                                         std::size_t max_frame_bytes = kDefaultMaxRpcFrameBytes) {
  if (buf.size() < kRpcHeaderBytes) {
    return {FrameParseStatus::NEED_MORE_DATA, 0, {}, {}, {}, {}};
  }

  std::array<std::uint8_t, kRpcHeaderBytes> header_bytes {};
  buf.copy_to(header_bytes.data(), header_bytes.size(), 0);
  RpcHeader header;
  header.magic = detail::LoadUint32BE(header_bytes, 0);
  header.body_len = detail::LoadUint32BE(header_bytes, 4);
  header.meta_len = detail::LoadUint32BE(header_bytes, 8);
  header.flags = detail::LoadUint32BE(header_bytes, 12);

  if (header.magic != RpcHeader::kMagic) {
    return {FrameParseStatus::ERROR_MAGIC_MISMATCH, 0, {}, {}, {}, {}};
  }
  if (WireVersion(header.flags) != kRpcWireVersion) {
    return {FrameParseStatus::ERROR_UNSUPPORTED_VERSION, 0, header, {}, {}, {}};
  }
  if ((WireFeatures(header.flags) & ~kKnownWireFeatures) != 0) {
    return {FrameParseStatus::ERROR_UNSUPPORTED_FLAGS, 0, header, {}, {}, {}};
  }
  if (header.meta_len == 0) {
    return {FrameParseStatus::ERROR_INVALID_LENGTH, 0, header, {}, {}, {}};
  }

  const std::size_t meta_size = header.meta_len;
  if (meta_size > std::numeric_limits<std::size_t>::max() - header.body_len) {
    return {FrameParseStatus::ERROR_INVALID_LENGTH, 0, header, {}, {}, {}};
  }
  const std::size_t payload_size = meta_size + header.body_len;
  if (payload_size > max_frame_bytes || kRpcHeaderBytes > max_frame_bytes - payload_size) {
    return {FrameParseStatus::ERROR_FRAME_TOO_LARGE, 0, header, {}, {}, {}};
  }
  const std::size_t total_needed = kRpcHeaderBytes + payload_size;
  if (buf.size() < total_needed) {
    return {FrameParseStatus::NEED_MORE_DATA, total_needed, header, {}, {}, {}};
  }

  RpcMeta meta;
  butil::IOBuf meta_buf;
  buf.append_to(&meta_buf, header.meta_len, kRpcHeaderBytes);
  butil::IOBufAsZeroCopyInputStream zc_meta(meta_buf);
  if (!meta.ParseFromZeroCopyStream(&zc_meta)) {
    return {FrameParseStatus::ERROR_CORRUPTED_FRAME, total_needed, header, {}, {}, {}};
  }

  const uint32_t attachment_size = meta.attachment_size();
  if (attachment_size > header.body_len) {
    return {FrameParseStatus::ERROR_INVALID_LENGTH, total_needed, header, {}, {}, {}};
  }
  const uint32_t proto_body_len = header.body_len - attachment_size;
  const std::size_t body_offset = kRpcHeaderBytes + header.meta_len;

  butil::IOBuf body_iobuf;
  if (proto_body_len != 0) {
    buf.append_to(&body_iobuf, proto_body_len, body_offset);
  }
  butil::IOBuf attachment_iobuf;
  if (attachment_size != 0) {
    buf.append_to(&attachment_iobuf, attachment_size, body_offset + proto_body_len);
  }
  return {FrameParseStatus::SUCCESS, total_needed,          header,
          std::move(meta),           std::move(body_iobuf), std::move(attachment_iobuf)};
}

// On failure append nothing. The limit matches TryParseRpcFrame's definition.
inline bool PackRpcFrame(RpcMeta& meta, const google::protobuf::Message* message, const butil::IOBuf* attachment,
                         butil::IOBuf& out_buf, std::size_t max_frame_bytes = kDefaultMaxRpcFrameBytes) {
  butil::IOBuf body;
  if (message) {
    {
      // As above, finalize the zero-copy stream before body is framed.
      butil::IOBufAsZeroCopyOutputStream zc_body(&body);
      if (!message->SerializeToZeroCopyStream(&zc_body)) {
        return false;
      }
    }
  }
  return detail::PackSerializedFrame(meta, body, attachment, out_buf, max_frame_bytes);
}

inline bool PackRpcFrame(RpcMeta& meta, const butil::IOBuf& raw_body, const butil::IOBuf* attachment,
                         butil::IOBuf& out_buf, std::size_t max_frame_bytes = kDefaultMaxRpcFrameBytes) {
  return detail::PackSerializedFrame(meta, raw_body, attachment, out_buf, max_frame_bytes);
}

inline bool PackRpcFrame(uint8_t msg_type, uint64_t correlation_id, std::string_view service_name,
                         std::string_view method_name, const google::protobuf::Message& message, butil::IOBuf& out_buf,
                         std::size_t max_frame_bytes = kDefaultMaxRpcFrameBytes) {
  RpcMeta meta;
  meta.set_msg_type(static_cast<RpcMessageType>(msg_type));
  meta.set_correlation_id(correlation_id);
  if (!service_name.empty()) {
    meta.set_service_name(std::string(service_name));
  }
  if (!method_name.empty()) {
    meta.set_method_name(std::string(method_name));
  }
  return PackRpcFrame(meta, &message, nullptr, out_buf, max_frame_bytes);
}

}  // namespace ant_server::rpc

namespace ant_rpc {
using namespace ant_server::rpc;
}
