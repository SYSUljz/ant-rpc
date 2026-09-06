#pragma once

#include <algorithm>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ant_server/metrics/latency_recorder.hpp"
#include "ant_server/metrics/metric.hpp"

namespace ant_server::metrics {

using MetricLabels = std::map<std::string, std::string>;

struct MetricSnapshot {
  std::string name;
  MetricLabels labels;
  std::variant<uint64_t, int64_t, LatencySnapshot> value;
};

// An explicitly owned registry. Registration retains the metric (aliasing
// shared_ptrs may retain a containing metrics object). Unregister removes it
// from future snapshots; an already started snapshot may still include it.
// Values are weakly consistent. Updates never acquire the registry mutex.
class MetricRegistry {
 public:
  // A fixed prefix permits an exporter to reserve a namespace for this registry.
  explicit MetricRegistry(std::string prefix = {}) : prefix_(std::move(prefix)) {}

  template <typename T>
    requires(std::is_same_v<std::remove_const_t<T>, Counter> || std::is_same_v<std::remove_const_t<T>, Gauge> ||
             std::is_same_v<std::remove_const_t<T>, LatencyRecorder>)
  bool Register(std::string name, std::shared_ptr<T> metric, MetricLabels labels = {}) {
    if (!ValidName(name)) {
      return false;
    }
    name = prefix_ + name;
    if (!metric || !ValidName(name)) {
      return false;
    }
    for (const auto& [key, value] : labels) {
      (void)value;
      if (!ValidName(key) || key.starts_with("__")) {
        return false;
      }
    }
    Handle handle = std::shared_ptr<const std::remove_const_t<T>>(std::move(metric));
    absl::MutexLock lock(&mutex_);
    // Reserve summary suffixes as well, preventing ambiguous exported samples.
    for (const auto& [existing_name, entries] : families_) {
      if (existing_name == name) {
        if (entries.front().metric.index() != handle.index() || !SameLabelKeys(entries.front().labels, labels)) {
          return false;
        }
        for (const auto& entry : entries) {
          if (entry.labels == labels) {
            return false;
          }
        }
      } else if (Conflicts(existing_name, entries.front().metric.index(), name, handle.index())) {
        return false;
      }
    }
    families_[name].push_back({std::move(labels), std::move(handle)});
    return true;
  }

  bool Unregister(std::string_view name, const MetricLabels& labels = {}) {
    // Keep removed ownership outside the lock, including custom deleters.
    std::vector<Entry> removed;
    {
      absl::MutexLock lock(&mutex_);
      auto it = families_.find(prefix_ + std::string(name));
      if (it == families_.end()) {
        return false;
      }
      auto& entries = it->second;
      auto entry = std::find_if(entries.begin(), entries.end(), [&](const Entry& e) { return e.labels == labels; });
      if (entry == entries.end()) {
        return false;
      }
      removed.push_back(std::move(*entry));
      entries.erase(entry);
      if (entries.empty()) {
        families_.erase(it);
      }
    }
    return true;
  }

  [[nodiscard]] std::vector<MetricSnapshot> Snapshot() const {
    std::vector<std::pair<std::string, Entry>> handles;
    {
      absl::MutexLock lock(&mutex_);
      for (const auto& [name, entries] : families_) {
        for (const auto& entry : entries) {
          handles.emplace_back(name, entry);
        }
      }
    }
    std::vector<MetricSnapshot> result;
    result.reserve(handles.size());
    for (const auto& [name, entry] : handles) {
      MetricSnapshot sample {name, entry.labels, uint64_t {0}};
      std::visit(
          [&](const auto& metric) {
            if constexpr (std::is_same_v<typename std::decay_t<decltype(metric)>::element_type,
                                         const LatencyRecorder>) {
              sample.value = metric->Snapshot();
            } else {
              sample.value = metric->Value();
            }
          },
          entry.metric);
      result.push_back(std::move(sample));
    }
    std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
      return a.name < b.name || (a.name == b.name && a.labels < b.labels);
    });
    return result;
  }

  [[nodiscard]] std::string PrometheusText() const {
    std::ostringstream output;
    std::string previous;
    for (const auto& sample : Snapshot()) {
      if (previous != sample.name) {
        output << "# TYPE " << sample.name << ' '
               << (sample.value.index() == 0   ? "counter"
                   : sample.value.index() == 1 ? "gauge"
                                               : "summary")
               << '\n';
        if (sample.value.index() == 2) {
          output << "# TYPE " << sample.name << "_min gauge\n# TYPE " << sample.name << "_max gauge\n";
        }
        previous = sample.name;
      }
      const auto write = [&](std::string_view suffix, auto value) {
        output << sample.name << suffix;
        if (!sample.labels.empty()) {
          output << '{';
          bool first = true;
          for (const auto& [key, label] : sample.labels) {
            if (!std::exchange(first, false)) {
              output << ',';
            }
            output << key << "=\"";
            for (char c : label) {
              if (c == '\\' || c == '"') {
                output << '\\';
              }
              if (c == '\n') {
                output << "\\n";
              } else {
                output << c;
              }
            }
            output << '"';
          }
          output << '}';
        }
        output << ' ' << value << '\n';
      };
      std::visit(
          [&](const auto& value) {
            if constexpr (std::is_same_v<std::decay_t<decltype(value)>, LatencySnapshot>) {
              write("_count", value.count);
              write("_sum", value.total_microseconds);
              write("_min", value.min_microseconds);
              write("_max", value.max_microseconds);
            } else {
              write("", value);
            }
          },
          sample.value);
    }
    return output.str();
  }

 private:
  using Handle = std::variant<std::shared_ptr<const Counter>, std::shared_ptr<const Gauge>,
                              std::shared_ptr<const LatencyRecorder>>;
  struct Entry {
    MetricLabels labels;
    Handle metric;
  };
  static bool ValidName(std::string_view name) {
    const auto letter = [](char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'; };
    if (name.empty() || !letter(name.front())) {
      return false;
    }
    return std::all_of(name.begin(), name.end(), [&](char c) { return letter(c) || (c >= '0' && c <= '9'); });
  }
  static bool SameLabelKeys(const MetricLabels& a, const MetricLabels& b) {
    return a.size() == b.size() &&
           std::equal(a.begin(), a.end(), b.begin(), [](const auto& x, const auto& y) { return x.first == y.first; });
  }
  static bool Conflicts(const std::string& a, std::size_t at, const std::string& b, std::size_t bt) {
    for (const auto suffix : {"_count", "_sum", "_min", "_max"}) {
      if ((at == 2 && a + suffix == b) || (bt == 2 && b + suffix == a)) {
        return true;
      }
    }
    return false;
  }
  const std::string prefix_;
  mutable absl::Mutex mutex_;
  absl::flat_hash_map<std::string, std::vector<Entry>> families_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace ant_server::metrics
