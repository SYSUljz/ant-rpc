#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "ant_server/metrics/latency_recorder.hpp"
#include "ant_server/metrics/metric.hpp"
#include "ant_server/rpc/core/client_metrics.hpp"

namespace {

TEST(MetricsTest, CounterAccumulatesAcrossThreads) {
  ant_server::metrics::Counter counter;
  constexpr int kThreadCount = 8;
  constexpr int kIncrementsPerThread = 10'000;

  std::vector<std::thread> threads;
  threads.reserve(kThreadCount);
  for (int thread = 0; thread < kThreadCount; ++thread) {
    threads.emplace_back([&counter] {
      for (int increment = 0; increment < kIncrementsPerThread; ++increment) {
        counter.Add();
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(counter.Value(), kThreadCount * kIncrementsPerThread);
}

TEST(MetricsTest, GaugeTracksSignedChanges) {
  ant_server::metrics::Gauge gauge;
  gauge.Set(10);
  gauge.Add(-4);
  gauge.Add(7);

  EXPECT_EQ(gauge.Value(), 13);
}

TEST(MetricsTest, LatencyRecorderSummarizesSamples) {
  ant_server::metrics::LatencyRecorder recorder;
  EXPECT_TRUE(recorder.Snapshot().empty());

  recorder.Record(std::chrono::microseconds(30));
  recorder.Record(std::chrono::microseconds(10));
  recorder.RecordMicroseconds(60);
  recorder.Record(std::chrono::microseconds(-1));

  const auto snapshot = recorder.Snapshot();
  EXPECT_FALSE(snapshot.empty());
  EXPECT_EQ(snapshot.count, 3);
  EXPECT_EQ(snapshot.total_microseconds, 100);
  EXPECT_EQ(snapshot.min_microseconds, 10);
  EXPECT_EQ(snapshot.max_microseconds, 60);
  EXPECT_EQ(snapshot.average_microseconds, 33);
}

TEST(MetricsTest, LatencyRecorderRecordsConcurrently) {
  ant_server::metrics::LatencyRecorder recorder;
  constexpr int kThreadCount = 4;
  constexpr int kRecordsPerThread = 5'000;

  std::vector<std::thread> threads;
  threads.reserve(kThreadCount);
  for (int thread = 0; thread < kThreadCount; ++thread) {
    threads.emplace_back([&recorder, thread] {
      for (int record = 0; record < kRecordsPerThread; ++record) {
        recorder.RecordMicroseconds(static_cast<uint64_t>(thread + 1));
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  const auto snapshot = recorder.Snapshot();
  EXPECT_EQ(snapshot.count, kThreadCount * kRecordsPerThread);
  EXPECT_EQ(snapshot.total_microseconds, 10ULL * kRecordsPerThread);
  EXPECT_EQ(snapshot.min_microseconds, 1);
  EXPECT_EQ(snapshot.max_microseconds, 4);
  EXPECT_EQ(snapshot.average_microseconds, 2);
}

TEST(MetricsTest, ClientMetricsClassifiesUniqueTerminalOutcomes) {
  ant_server::rpc::ClientMetrics metrics;
  metrics.calls_started.Add(4);
  metrics.RecordCompletion(ant_server::rpc::RPC_SUCCESS, 10);
  metrics.RecordCompletion(ant_server::rpc::RPC_EINTERNAL, 20);
  metrics.RecordCompletion(ant_server::rpc::RPC_ETIMEOUT, 30);
  metrics.RecordCompletion(ant_server::rpc::RPC_ECANCELED, 40);

  EXPECT_EQ(metrics.calls_started.Value(), 4);
  EXPECT_EQ(metrics.calls_completed.Value(), 4);
  EXPECT_EQ(metrics.calls_succeeded.Value(), 1);
  EXPECT_EQ(metrics.call_errors.Value(), 3);
  EXPECT_EQ(metrics.calls_timed_out.Value(), 1);
  EXPECT_EQ(metrics.calls_canceled.Value(), 1);
  EXPECT_EQ(metrics.call_latency.Snapshot().count, 4);
  EXPECT_EQ(metrics.call_latency.Snapshot().total_microseconds, 100);
}

}  // namespace
