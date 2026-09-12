#pragma once

#include <cstdint>

#include "ant_rpc/metrics/latency_recorder.hpp"
#include "ant_rpc/metrics/metric.hpp"
#include "ant_rpc/rpc/error_code.hpp"

namespace ant_rpc::rpc {

// Framework-owned metrics for one RpcChannel instance. The counters are
// process-lifetime totals for that facade and remain valid across Init()/Close()
// cycles. SlotTable records terminal outcomes so response, timeout, cancel and
// connection-close races can only update a call once.
struct ClientMetrics {
  // Calls presented to StartUnaryCall, including calls rejected before a slot
  // can be allocated.
  ant_rpc::metrics::Counter calls_started;
  // Every call that reached either an inline or SlotTable-owned terminal path.
  ant_rpc::metrics::Counter calls_completed;
  ant_rpc::metrics::Counter calls_succeeded;
  ant_rpc::metrics::Counter call_errors;
  // Subsets of call_errors.
  ant_rpc::metrics::Counter calls_timed_out;
  ant_rpc::metrics::Counter calls_canceled;

  // Calls which currently own a slot. ARMING and IN_FLIGHT are both included.
  ant_rpc::metrics::Gauge active_in_flight;
  // Bytes retained by the channel command mailbox, outbound queue, or active
  // partial write.
  ant_rpc::metrics::Gauge outbound_bytes;

  ant_rpc::metrics::LatencyRecorder call_latency;

  void RecordCompletion(int error_code, uint64_t latency_us) noexcept {
    calls_completed.Add();
    if (error_code == RPC_SUCCESS) {
      calls_succeeded.Add();
    } else {
      call_errors.Add();
      if (error_code == RPC_ETIMEOUT) {
        calls_timed_out.Add();
      } else if (error_code == RPC_ECANCELED) {
        calls_canceled.Add();
      }
    }
    call_latency.RecordMicroseconds(latency_us);
  }
};

}  // namespace ant_rpc::rpc
