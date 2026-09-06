#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "ant_server/metrics/latency_recorder.hpp"
#include "ant_server/metrics/metric.hpp"
#include "ant_server/metrics/metric_registry.hpp"
#include "ant_server/rpc/core/client_metrics.hpp"

namespace {

TEST(MetricRegistryTest, ExportsTypedSamplesAndEscapesLabels) {
  using namespace ant_server::metrics;
  MetricRegistry registry;
  auto count = std::make_shared<Counter>();
  auto gauge = std::make_shared<Gauge>();
  auto latency = std::make_shared<LatencyRecorder>();
  count->Add(7);
  gauge->Set(-2);
  latency->RecordMicroseconds(12);
  ASSERT_TRUE(registry.Register("requests_total", count, {{"service", "a\"b\\c\nd"}}));
  ASSERT_TRUE(registry.Register("pending", gauge));
  ASSERT_TRUE(registry.Register("latency_us", latency));
  const auto text = registry.PrometheusText();
  EXPECT_NE(text.find("requests_total{service=\"a\\\"b\\\\c\\nd\"} 7\n"), std::string::npos);
  EXPECT_NE(text.find("# TYPE pending gauge\npending -2\n"), std::string::npos);
  EXPECT_NE(text.find("latency_us_count 1\nlatency_us_sum 12\nlatency_us_min 12\nlatency_us_max 12\n"),
            std::string::npos);
  EXPECT_EQ(registry.Snapshot().size(), 3);
}

TEST(MetricRegistryTest, RejectsInvalidDuplicateAndConflictingSeries) {
  using namespace ant_server::metrics;
  MetricRegistry registry;
  auto count = std::make_shared<Counter>();
  EXPECT_FALSE(registry.Register("bad-name", count));
  EXPECT_FALSE(registry.Register("", count));
  EXPECT_FALSE(registry.Register("valid", std::shared_ptr<Counter>()));
  EXPECT_FALSE(registry.Register("valid", count, {{"__reserved", "x"}}));
  ASSERT_TRUE(registry.Register("requests", count, {{"service", "a"}}));
  EXPECT_FALSE(registry.Register("requests", count, {{"service", "a"}}));
  EXPECT_TRUE(registry.Register("requests", count, {{"service", "b"}}));
  EXPECT_FALSE(registry.Register("requests", std::make_shared<Gauge>(), {{"service", "c"}}));
  EXPECT_FALSE(registry.Register("requests", count, {{"method", "c"}}));
  ASSERT_TRUE(registry.Register("latency", std::make_shared<LatencyRecorder>()));
  EXPECT_FALSE(registry.Register("latency_count", count));
  ASSERT_TRUE(registry.Register("other_sum", count));
  EXPECT_FALSE(registry.Register("other", std::make_shared<LatencyRecorder>()));
}

TEST(MetricRegistryTest, RetainsOwnerUntilUnregisteredAndAllowsReregistration) {
  using namespace ant_server::metrics;
  struct Owner {
    Counter count;
  };
  MetricRegistry registry("app_");
  auto owner = std::make_shared<Owner>();
  std::weak_ptr<Owner> weak = owner;
  ASSERT_TRUE(registry.Register("requests", std::shared_ptr<const Counter>(owner, &owner->count)));
  owner.reset();
  EXPECT_FALSE(weak.expired());
  EXPECT_EQ(registry.Snapshot().front().name, "app_requests");
  ASSERT_TRUE(registry.Unregister("requests"));
  EXPECT_TRUE(weak.expired());
  EXPECT_FALSE(registry.Unregister("requests"));
  EXPECT_TRUE(registry.Snapshot().empty());
  EXPECT_TRUE(registry.Register("requests", std::make_shared<Gauge>()));
}

TEST(MetricRegistryTest, ConcurrentRegistrationCollectionAndUpdates) {
  using namespace ant_server::metrics;
  MetricRegistry registry;
  auto counter = std::make_shared<Counter>();
  ASSERT_TRUE(registry.Register("permanent", counter));
  std::thread updater([&] {
    for (int i = 0; i < 2000; ++i) {
      counter->Add();
    }
  });
  std::thread registrar([&] {
    for (int i = 0; i < 1000; ++i) {
      EXPECT_TRUE(registry.Register("temporary", std::make_shared<Gauge>()));
      EXPECT_TRUE(registry.Unregister("temporary"));
    }
  });
  for (int i = 0; i < 1000; ++i) {
    const auto samples = registry.Snapshot();
    EXPECT_GE(samples.size(), 1);
    EXPECT_LE(samples.size(), 2);
    EXPECT_FALSE(registry.PrometheusText().empty());
  }
  updater.join();
  registrar.join();
  EXPECT_EQ(std::get<uint64_t>(registry.Snapshot().front().value), 2000);
}

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
