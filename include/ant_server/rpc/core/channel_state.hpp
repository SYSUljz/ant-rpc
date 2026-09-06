#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>

#include "ant_server/rpc/core/client_metrics.hpp"
#include "ant_server/rpc/slot_table.hpp"

namespace ant_server::rpc::detail {

// State shared by the public channel facade and its IO-thread driver.
struct ChannelState final : RpcCallCancellationTarget {
  explicit ChannelState(std::shared_ptr<ClientMetrics> client_metrics) : metrics(std::move(client_metrics)) {
    slots.SetClientMetrics(*metrics);
  }

  std::shared_ptr<ClientMetrics> metrics;
  SlotTable<65536> slots;
  std::atomic<bool> running {false};

  // Init remains a synchronous protobuf-compatible facade, but the actual
  // connection attempt runs on the owning Context. This condition variable is
  // only its compatibility bridge; it is not part of the IO state machine.
  std::mutex connect_mu;
  std::condition_variable connect_cv;
  bool connect_finished {false};
  int connect_result {-1};

  std::mutex close_mu;
  std::condition_variable close_cv;
  bool closed {true};

  bool CancelRpcSlot(uint64_t correlation_id) override { return slots.CancelSlot(correlation_id); }
};

}  // namespace ant_server::rpc::detail
