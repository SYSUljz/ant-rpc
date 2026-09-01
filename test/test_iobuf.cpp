#include <atomic>
#include <barrier>
#include <cstdlib>
#include <thread>

#include <gtest/gtest.h>

#include "ant_server/http/parser.hpp"
#include "butil/iobuf.h"

TEST(IOBufTest, BasicAppendAndCut) {
  butil::IOBuf buf;
  buf.append("Hello, ");
  buf.append("butil::IOBuf!");

  EXPECT_EQ(buf.length(), 20);
  EXPECT_EQ(buf.to_string(), "Hello, butil::IOBuf!");

  butil::IOBuf head;
  buf.cutn(&head, 5);

  EXPECT_EQ(head.to_string(), "Hello");
  EXPECT_EQ(buf.to_string(), ", butil::IOBuf!");
  EXPECT_EQ(buf.length(), 15);
}

// butil documents IOBuf as thread-compatible: separate IOBuf objects may
// share a Block and be destroyed concurrently. Keep this small reproducer
// separate from RPC so TSAN reports can distinguish an IOBuf implementation
// issue from an IO-to-worker handoff bug.
TEST(IOBufTest, SharedUserDataBlockCanBeReleasedFromDifferentThreads) {
  std::atomic<int> deleter_calls {0};
  auto* payload = static_cast<char*>(std::malloc(64));
  ASSERT_NE(payload, nullptr);

  butil::IOBuf source;
  ASSERT_EQ(source.append_user_data(payload, 64,
                                    [&deleter_calls](void* data) {
                                      std::free(data);
                                      deleter_calls.fetch_add(1, std::memory_order_relaxed);
                                    }),
            0);
  butil::IOBuf first(source);
  butil::IOBuf second(source);
  source.clear();

  std::barrier start {3};
  std::thread first_thread([&] {
    start.arrive_and_wait();
    first.clear();
  });
  std::thread second_thread([&] {
    start.arrive_and_wait();
    second.clear();
  });
  start.arrive_and_wait();
  first_thread.join();
  second_thread.join();

  EXPECT_EQ(deleter_calls.load(std::memory_order_acquire), 1);
}

TEST(HttpParserTest, ParseIOBufWithLlhttp) {
  butil::IOBuf buf;
  buf.append("GET /api/v1/resource HTTP/1.1\r\n");
  buf.append("Host: localhost:8080\r\n");
  buf.append("User-Agent: llhttp-tester\r\n");
  buf.append("Content-Length: 11\r\n\r\n");
  buf.append("Hello World");

  HttpParser parser;
  HttpRequest req;
  ParseResult res = parser.parse(buf, req);

  EXPECT_EQ(res.status, PARSE_SUCCESS);
  EXPECT_EQ(req.method, "GET");
  EXPECT_EQ(req.url, "/api/v1/resource");
  EXPECT_EQ(req.headers["Host"], "localhost:8080");
  EXPECT_EQ(req.headers["User-Agent"], "llhttp-tester");
  EXPECT_EQ(req.body.to_string(), "Hello World");
  EXPECT_TRUE(req.keep_alive);
  EXPECT_TRUE(req.complete);
}
