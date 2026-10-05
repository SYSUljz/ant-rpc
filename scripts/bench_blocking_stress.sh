#!/usr/bin/env bash
# bench_blocking_stress.sh:
# Stress test framework under slow blocking tasks (e.g. 50ms blocking sleep/SDK calls)
# to contrast Separate IO-Worker threading model vs In-place/Worker-coupled model.
#
# Metrics monitored:
#  1. Client QPS, success rate, and error codes.
#  2. Normal Echo request latency vs Tail request latency.
#  3. OS Kernel Socket Recv-Q backlog.
#  4. Kernel TCP buffer drops and errors (netstat -s diff).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PORT="${BENCH_PORT:-18080}"
ADMIN_PORT="${BENCH_ADMIN_PORT:-18081}"
TARGET_QPS="${TARGET_QPS:-4000}"
TAIL_RATE_PPM="${TAIL_RATE_PPM:-30000}"       # 3% slow requests
TAIL_LATENCY_US="${TAIL_LATENCY_US:-50000}"   # 50ms slow blocking task
DURATION="${DURATION:-10}"
WARMUP="${WARMUP:-2}"
CLIENT_THREADS="${CLIENT_THREADS:-32}"
SERVER_WORKERS="${SERVER_WORKERS:-8}"
SERVER_IO_THREADS="${SERVER_IO_THREADS:-4}"

SERVER_BIN="${ROOT_DIR}/build-release/benchmark/ant_rpc/ant_rpc_bench_server"
CLIENT_BIN="${ROOT_DIR}/build-release/benchmark/ant_rpc/ant_rpc_bench_client"

if [[ ! -x "${SERVER_BIN}" || ! -x "${CLIENT_BIN}" ]]; then
  echo "Benchmark binaries not found in build-release. Building..."
  cmake -B "${ROOT_DIR}/build-release" -DCMAKE_BUILD_TYPE=Release -DANT_RPC_BUILD_BENCHMARKS=ON "${ROOT_DIR}"
  cmake --build "${ROOT_DIR}/build-release" --target ant_rpc_bench_server ant_rpc_bench_client -j"$(nproc)"
fi

OUT_DIR="/tmp/bench_blocking_stress_$$"
mkdir -p "${OUT_DIR}"
trap 'rm -rf "${OUT_DIR}"' EXIT

echo "================================================================================"
echo "          BLOCKING STRESS BENCHMARK: ANTRPC SEPARATE IO MODEL"
echo "================================================================================"
echo " Configuration:"
echo "   Server Port:        ${PORT}"
echo "   Server IO Threads:  ${SERVER_IO_THREADS} (Dedicated Epoll / Net Reactor)"
echo "   Server Workers:     ${SERVER_WORKERS} (Worker Thread Pool)"
echo "   Client Concurrency: ${CLIENT_THREADS} Threads"
echo "   Target QPS:         ${TARGET_QPS}"
echo "   Slow Task Rate:     $(( TAIL_RATE_PPM / 10000 ))% (${TAIL_RATE_PPM} PPM)"
echo "   Slow Task Duration: $(( TAIL_LATENCY_US / 1000 ))ms (${TAIL_LATENCY_US} us)"
echo "   Test Duration:      ${DURATION}s (Warmup: ${WARMUP}s)"
echo "================================================================================"

# 1. Baseline netstat
netstat -s > "${OUT_DIR}/netstat_before.txt" 2>&1 || true

# 2. Start AntRPC Server in blocking mode
"${SERVER_BIN}" \
  --port="${PORT}" \
  --admin_port="${ADMIN_PORT}" \
  --io_threads="${SERVER_IO_THREADS}" \
  --worker_threads="${SERVER_WORKERS}" \
  --io_contexts="${SERVER_IO_THREADS}" \
  --tail_mode=blocking \
  --tail_latency_us="${TAIL_LATENCY_US}" > "${OUT_DIR}/server.log" 2>&1 &
SERVER_PID=$!

# Wait for server ready
for _ in $(seq 1 30); do
  timeout 1 bash -c "</dev/tcp/127.0.0.1/${PORT}" 2>/dev/null && break
  kill -0 "${SERVER_PID}" 2>/dev/null || { cat "${OUT_DIR}/server.log" >&2; exit 1; }
  sleep 0.1
done

# 3. Start background Recv-Q monitor
(
  max_recv_q=0
  while kill -0 "${SERVER_PID}" 2>/dev/null; do
    recv_q=$(ss -nt "sport = :${PORT}" 2>/dev/null | awk 'NR>1 { if ($2 > m) m = $2 } END { print m+0 }')
    if (( recv_q > max_recv_q )); then
      max_recv_q=$recv_q
    fi
    echo "${max_recv_q}" > "${OUT_DIR}/max_recv_q.txt"
    sleep 0.2
  done
) &
MONITOR_PID=$!

echo "[1/3] Running benchmark workload against AntRPC..."
"${CLIENT_BIN}" \
  --server="127.0.0.1:${PORT}" \
  --threads="${CLIENT_THREADS}" \
  --io_threads=2 \
  --warmup_seconds="${WARMUP}" \
  --duration_seconds="${DURATION}" \
  --payload_bytes=16 \
  --target_qps="${TARGET_QPS}" \
  --tail_rate_ppm="${TAIL_RATE_PPM}" \
  --timeout_ms=5000 > "${OUT_DIR}/client.log" 2>&1 || true

echo "[2/3] Stopping server and collecting kernel network metrics..."
kill -TERM "${MONITOR_PID}" 2>/dev/null || true
wait "${MONITOR_PID}" 2>/dev/null || true

kill -TERM "${SERVER_PID}" 2>/dev/null || true
wait "${SERVER_PID}" 2>/dev/null || true

netstat -s > "${OUT_DIR}/netstat_after.txt" 2>&1 || true
diff -u "${OUT_DIR}/netstat_before.txt" "${OUT_DIR}/netstat_after.txt" \
  | grep -E "^\+[ ]+.*(error|drop|prune|listen|reset)" > "${OUT_DIR}/netstat_diff.txt" 2>&1 || true

echo "[3/3] Benchmark completed. Results:"
echo "--------------------------------------------------------------------------------"
cat "${OUT_DIR}/client.log" | grep "BENCHMARK_RESULT" || cat "${OUT_DIR}/client.log"
echo "--------------------------------------------------------------------------------"
MAX_RECV_Q=$(cat "${OUT_DIR}/max_recv_q.txt" 2>/dev/null || echo 0)
echo " Peak Socket Recv-Q Backlog:  ${MAX_RECV_Q} bytes"
echo " Kernel TCP Buffer Drops:     $(wc -l < "${OUT_DIR}/netstat_diff.txt") events detected"
if [[ -s "${OUT_DIR}/netstat_diff.txt" ]]; then
  cat "${OUT_DIR}/netstat_diff.txt"
else
  echo " -> Clean run: Zero TCP buffer errors or kernel packet drops recorded!"
fi
echo "================================================================================"
