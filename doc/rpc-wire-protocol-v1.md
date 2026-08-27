# Ant RPC Wire Protocol V1

This document is the compatibility contract between an Ant RPC client and
server. It is intentionally independent of C++ object layout and host CPU
endianness.

## Frame layout

Every frame has a 16-byte fixed header, followed by three ordered regions:

```text
+--------------------+--------------------+--------------------+--------------------+
| magic: "ANTR"      | body_len           | meta_len           | flags              |
| 4 bytes            | uint32, big-endian | uint32, big-endian | uint32, big-endian |
+--------------------+--------------------+--------------------+--------------------+
| RpcMeta protobuf, exactly meta_len bytes                                        |
+----------------------------------------------------------------------------------+
| protobuf/raw body, body_len - RpcMeta.attachment_size() bytes                   |
+----------------------------------------------------------------------------------+
| attachment, RpcMeta.attachment_size() bytes                                     |
+----------------------------------------------------------------------------------+
```

All header integers use network byte order (big-endian). `body_len` is the
sum of the body and attachment, while `meta_len` covers only the serialized
`RpcMeta`. The attachment is always the final region. A receiver rejects a
frame if `meta_len == 0` or `attachment_size > body_len`.

The complete frame size is:

```text
16 + meta_len + body_len
```

`max_frame_bytes` applies to that complete value, including the fixed header.
The parser validates it immediately after reading the header, before copying
or deserializing the declared payload. The packer has the same rule and leaves
its output buffer untouched when the frame would exceed the limit or a length
cannot fit in `uint32_t`.

## Flags and compatibility

`flags` is split into `[version: 8][features: 24]`.

- V1 writes version `1` and feature bits `0` (`0x01000000`).
- An unknown version is a protocol error; it is not treated as V1.
- An unknown feature bit is a protocol error.
- Adding a backward-compatible optional feature requires allocating one of
  the low 24 feature bits, documenting its receiver behavior, and updating
  both ends before it is emitted.

This explicit rejection prevents accidental interoperation when a future
frame changes semantics.
