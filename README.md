# ant_rpc

A high-performance, asynchronous HTTP/1.1 Web Server framework built with **C++20 Coroutines**, Linux **`io_uring`**, and a **3-Tier Work-Stealing Task Scheduler**.

---

## 🚀 Key Architectural Features

- **C++20 Stackless Coroutines**: Idiomatic `co_await` async pipeline (`ReadAwaiter`, `WriteAwaiter`, `CloseAwaiter`, `with_timeout`).
- **Linux `io_uring` Direct Demultiplexing**: High-throughput kernel ring-buffer event processing with `SO_REUSEPORT` multishot socket accept.
- **3-Tier Work-Stealing Scheduler**:
  - **Tier 1: `lifo_slot` (Capacity 1)**: Hot-path single-slot execution for maximum CPU L1 Data Cache locality.
  - **Tier 2: Chase-Lev SPMC Local Deque (Capacity 256)**: Lock-free per-worker array ring-buffer with LIFO push/pop and FIFO cross-thread stealing.
  - **Tier 3: Global MPMC Overflow Queue**: Shared lock-free injector queue with a 61-tick starvation-prevention polling mechanism.
- **Zero-Allocation Awaiters**: `BaseAwaiter` inherits from `CoroTask` and `IOHandler`, reusing memory pre-allocated inside the coroutine frame (zero runtime `malloc`/`free`).
- **C++26 Sender/Receiver Ready**: Erased C-style function pointer dispatch in `TaskNode` for future P2300 `std::execution` migration.

---

## 📁 Directory Structure

```
ant_rpc/
├── include/ant_rpc/
│   ├── awaiter/          # Async Socket & Timeout coroutine awaiters
│   ├── context/          # io_uring Context and Service Registry (IOuringSocketService, AntTimer)
│   ├── handler/          # Multishot Connection Acceptor
│   ├── http/             # Zero-copy HTTP/1.1 parser
│   ├── scheduler/        # Chase-Lev SPMCQueue, MpmcQueue, and Scheduler
│   ├── utils/            # High-precision TimingWheel and helper utilities
│   ├── server.hpp        # Server facade and HTTP connection loop
│   └── type.hpp          # TaskNode, CoroTask, LambdaTask, and IOHandler definitions
├── test/
│   └── main.cpp          # Entry point and integration test
├── .clang-format         # Google C++ Style formatting rules
├── Makefile              # Build, run, and format tasks
└── CMakeLists.txt        # CMake build configuration
```

---

## 🛠️ Prerequisites

- **Compiler**: GCC 10+ or Clang 12+ with C++20 support (`-std=c++20`).
- **Kernel**: Linux Kernel 5.6+ (recommended for full `io_uring` feature support).
- **Libraries**: `liburing` development headers (`liburing-dev` or `liburing-devel`).

---

## 🏗️ Quick Start

### Build and Run with Makefile

```bash
# Build the test binary
make build

# Run the web server (listens on port 8012)
make run

# Format C++ code with clang-format (Google Style)
make format

# Verify formatting without modifying files
make format-check

# Clean build artifacts
make clean
```

### Build with CMake

```bash
mkdir -p build && cd build
cmake ..
make -j$(nproc)
./test_server
```

---

## 🧪 Testing the HTTP Server

Send an HTTP GET request to test the server:

```bash
curl -i http://127.0.0.1:8012/
```

**Sample Response:**

```http
HTTP/1.1 200 OK
Server: MyAwesomeServer/1.0
Content-Length: 33
Content-Type: text/plain
Connection: keep-alive

Hello, C++20 io_uring Web Server!
```

---

## 📜 License

This project is licensed under the MIT License.
