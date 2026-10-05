#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=========================================================="
echo " Running MPSC_queue AddressSanitizer & UBSan Test Suite"
echo "=========================================================="

export ASAN_OPTIONS="check_initialization_order=1:detect_stack_use_after_return=1:strict_init_order=1:halt_on_error=1:detect_invalid_pointer_pairs=2"
export UBSAN_OPTIONS="print_stacktrace=1:halt_on_error=1"

# 1. Build and run MPSC_queue's native test suite with ASan
echo ""
echo "[Step 1/3] Building and running official test suite..."
cmake -S "${ROOT_DIR}/3rd_party/MPSC_queue" -B "${ROOT_DIR}/build-mpsc-asan" \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer -g -mcx16" \
  -DCMAKE_C_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer -g -mcx16" > /dev/null

cmake --build "${ROOT_DIR}/build-mpsc-asan" --target mpsc_tests -j"$(nproc)" > /dev/null
"${ROOT_DIR}/build-mpsc-asan/mpsc_tests"

# 2. Build and run examples with ASan
echo ""
echo "[Step 2/3] Building and running official examples..."
cmake --build "${ROOT_DIR}/build-mpsc-asan" --target mpsc_log_system_example mpsc_command_dispatcher_example -j"$(nproc)" > /dev/null
"${ROOT_DIR}/build-mpsc-asan/mpsc_log_system_example" > /dev/null
"${ROOT_DIR}/build-mpsc-asan/mpsc_command_dispatcher_example" > /dev/null
rm -f app_log.txt
echo "  Official examples executed cleanly without ASan errors."

# 3. Compile and run comprehensive concurrent stress test
echo ""
echo "[Step 3/3] Compiling and running advanced ASan stress test (C++20, 16-32 threads, lifecycle & chunk recycling)..."
mkdir -p "${ROOT_DIR}/build-mpsc-asan/bin"
g++ -std=c++20 -O2 \
  -fsanitize=address,undefined -fno-omit-frame-pointer -g -mcx16 \
  -I"${ROOT_DIR}/3rd_party/MPSC_queue/include" \
  "${ROOT_DIR}/scripts/stress_mpsc_asan.cpp" \
  -lpthread -latomic \
  -o "${ROOT_DIR}/build-mpsc-asan/bin/stress_mpsc_asan"

"${ROOT_DIR}/build-mpsc-asan/bin/stress_mpsc_asan"

echo ""
echo "=========================================================="
echo "  All MPSC_queue ASan/UBSan checks PASSED successfully!   "
echo "=========================================================="
