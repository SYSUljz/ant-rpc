#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "ant_server/logging/logging.hpp"
#include "butil/logging.h"

TEST(LoggingTest, FiltersBeforeFormattingAndWritesEscapedFieldsToStderr) {
  namespace log = ant_server::logging;
  log::Initialize({.min_severity = absl::LogSeverityAtLeast::kError});
  bool formatted = false;
  log::Write(log::Event::kServerStarted, [&](auto&) { formatted = true; });
  EXPECT_FALSE(formatted);
  testing::internal::CaptureStderr();
  log::Write(log::Event::kServerStartFailed,
             [](auto& out) { out << " reason=" << ant_server::logging::Quote("bad\n\"input\\"); });
  const auto text = testing::internal::GetCapturedStderr();
  EXPECT_NE(text.find("event=server_start_failed"), std::string::npos);
  EXPECT_NE(text.find("reason=\"bad\\x0a\\\"input\\\\\""), std::string::npos);
  log::Initialize();
}

TEST(LoggingTest, RateLimiterHasOneConcurrentWinnerAndReportsSuppression) {
  ant_server::logging::RateLimiter limiter;
  std::atomic<int> winners {0};
  std::vector<std::thread> threads;
  for (int i = 0; i < 16; ++i) {
    threads.emplace_back([&] {
      if (limiter.Allow(100, 1000)) {
        ++winners;
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(winners.load(), 1);
  EXPECT_EQ(limiter.TakeSuppressed(), 15);
  EXPECT_EQ(limiter.TakeSuppressed(), 0);
  EXPECT_FALSE(limiter.Allow(1099, 1000));
  EXPECT_TRUE(limiter.Allow(1100, 1000));
}

TEST(LoggingTest, SuppressedWarningSkipsFieldsAndIncrementsCount) {
  namespace log = ant_server::logging;
  log::Initialize();
  int formatted = 0;
  testing::internal::CaptureStderr();
  log::Write(log::Event::kOverload, [&](auto&) { ++formatted; });
  log::Write(log::Event::kOverload, [&](auto&) { ++formatted; });
  const auto output = testing::internal::GetCapturedStderr();
  EXPECT_EQ(formatted, 1);
  EXPECT_NE(output.find("test_logging.cpp"), std::string::npos);
  EXPECT_NE(output.find("event=overload"), std::string::npos);
  EXPECT_EQ(log::limiters[static_cast<std::size_t>(log::Event::kOverload)].TakeSuppressed(), 1);
}
