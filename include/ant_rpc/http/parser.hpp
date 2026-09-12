#pragma once

#include <string_view>

#include <absl/container/flat_hash_map.h>

#include "butil/iobuf.h"
#include "llhttp.h"

enum ParseStatus { PARSE_SUCCESS, PARSE_NEED_MORE_DATA, PARSE_ERROR };

struct ParseResult {
  ParseStatus status;
  size_t consumed_bytes {0};
};

struct HttpRequest {
  std::string_view method;
  std::string_view url;
  std::string_view version {"HTTP/1.1"};
  absl::flat_hash_map<std::string_view, std::string_view> headers;
  butil::IOBuf body;
  bool keep_alive {false};
  bool complete {false};
};

class HttpParser {
 public:
  HttpParser() {
    llhttp_settings_init(&settings_);

    settings_.on_url = [](llhttp_t* b, const char* at, size_t length) -> int {
      auto* ctx = static_cast<ParserContext*>(b->data);
      ctx->req->url = std::string_view(at, length);
      return 0;
    };

    settings_.on_header_field = [](llhttp_t* b, const char* at, size_t length) -> int {
      auto* ctx = static_cast<ParserContext*>(b->data);
      if (!ctx->current_value.empty()) {
        ctx->req->headers[ctx->current_field] = ctx->current_value;
        ctx->current_field = {};
        ctx->current_value = {};
      }
      ctx->current_field = std::string_view(at, length);
      return 0;
    };

    settings_.on_header_value = [](llhttp_t* b, const char* at, size_t length) -> int {
      auto* ctx = static_cast<ParserContext*>(b->data);
      ctx->current_value = std::string_view(at, length);
      return 0;
    };

    settings_.on_headers_complete = [](llhttp_t* b) -> int {
      auto* ctx = static_cast<ParserContext*>(b->data);
      if (!ctx->current_field.empty()) {
        ctx->req->headers[ctx->current_field] = ctx->current_value;
        ctx->current_field = {};
        ctx->current_value = {};
      }
      return 0;
    };

    settings_.on_body = [](llhttp_t* b, const char* at, size_t length) -> int {
      auto* ctx = static_cast<ParserContext*>(b->data);
      ctx->req->body.append(at, length);
      return 0;
    };

    settings_.on_message_complete = [](llhttp_t* b) -> int {
      auto* ctx = static_cast<ParserContext*>(b->data);
      ctx->req->complete = true;
      return 0;
    };

    llhttp_init(&parser_, HTTP_REQUEST, &settings_);
  }

  void reset() { llhttp_reset(&parser_); }

  // Iteratively parse backing physical blocks of butil::IOBuf (zero-copy)
  ParseResult parse(const butil::IOBuf& buf, HttpRequest& req) {
    reset();
    req = HttpRequest {};
    ParserContext ctx {&req, {}, {}};
    parser_.data = &ctx;

    size_t total_consumed = 0;
    size_t block_count = buf.backing_block_num();

    for (size_t i = 0; i < block_count; ++i) {
      butil::StringPiece blk = buf.backing_block(i);
      if (blk.empty()) {
        continue;
      }

      enum llhttp_errno err = llhttp_execute(&parser_, blk.data(), blk.size());

      if (err == HPE_OK) {
        total_consumed += blk.size();
      } else if (err == HPE_PAUSED || err == HPE_PAUSED_UPGRADE) {
        const char* pos = llhttp_get_error_pos(&parser_);
        if (pos >= blk.data() && pos <= blk.data() + blk.size()) {
          total_consumed += (pos - blk.data());
        } else {
          total_consumed += blk.size();
        }
        llhttp_resume(&parser_);
        break;
      } else {
        return {PARSE_ERROR, 0};
      }

      if (req.complete) {
        break;
      }
    }

    if (req.complete) {
      req.method = llhttp_method_name(static_cast<llhttp_method_t>(llhttp_get_method(&parser_)));
      req.keep_alive = llhttp_should_keep_alive(&parser_);
      return {PARSE_SUCCESS, total_consumed};
    }

    return {PARSE_NEED_MORE_DATA, total_consumed};
  }

 private:
  struct ParserContext {
    HttpRequest* req;
    std::string_view current_field;
    std::string_view current_value;
  };

  llhttp_t parser_;
  llhttp_settings_t settings_;
};

inline ParseResult try_parse_http(HttpParser& parser, const butil::IOBuf& buf, HttpRequest& req) {
  return parser.parse(buf, req);
}
