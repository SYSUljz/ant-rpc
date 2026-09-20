# AntRPC

[English](#english) | [中文](#中文)

<a id="english"></a>

## English

AntRPC is an experimental C++20 **Protobuf unary RPC** framework for Linux. It offers a Protobuf `RpcChannel` / `Service` programming model, multiplexes concurrent calls over TCP connections, and separates network I/O from service execution.

It is intended for learning and evaluating RPC runtime design, not as a production-ready replacement for bRPC or gRPC.

### Scope

- Protobuf reflection-based unary calls with synchronous, callback-based, and coroutine-facing clients.
- A versioned binary protocol with correlation IDs, metadata, attachments, frame-size limits, and network-byte-order headers.
- Deadline, cancellation, connection-failure handling, and exactly-once completion when responses, cancellation, and timeouts race.
- Server limits for connections, in-flight calls, pending worker tasks, and per-connection outbound buffers.
- Graceful `Stop()` / `Join()` and a loopback admin endpoint for health, status, and RPC metrics.

Streaming RPC, TLS, service discovery, load balancing, retries, and gRPC/bRPC wire compatibility are out of scope.

### Architecture

```text
                          worker executor
                    +-----------------------+
                    | service method tasks  |
                    | local queues + steal  |
                    +-----------+-----------+
                                |
                         completion command
                                |
client TCP <-> acceptor -> I/O Context -> ServerConnection
                          (one I/O owner per accepted connection)
```

- **Multi-reactor I/O:** `Scheduler` owns dedicated I/O contexts and worker threads; an accepted connection is assigned once to an eligible I/O context.
- **Single-owner connections:** a connection's fd, buffers, in-flight calls, outbound queue, and write state are changed only by its I/O thread. Worker completions return through an I/O command mailbox.
- **Execution isolation:** parsed RPC calls run on a work-stealing worker executor; I/O threads do not run user service methods.
- **Backpressure and shutdown:** configurable admission limits bound resources. Shutdown stops accepts, waits until a deadline, then closes remaining connections through their I/O owners.

`epoll` is the default I/O backend. `io_uring` is selectable at configure time and requires a compatible Linux kernel and `liburing`.

### Build and test

Requirements: Linux, CMake 3.16+, and a C++20 compiler (GCC 10+ or Clang 12+). CMake fetches Protobuf, Abseil, and GoogleTest by default; use installed packages with `ANT_RPC_USE_SYSTEM_DEPS=ON`.

```bash
# Default epoll backend
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure

# Optional io_uring backend
cmake -S . -B build-uring -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DANT_RPC_IO_BACKEND=io_uring
cmake --build build-uring --parallel
```

The suite covers wire frames, timeout/cancellation/response races, the executor, server admission limits, graceful shutdown, and process-level RPC scenarios. Tests using loopback TCP require an environment that permits socket creation.

```bash
# ThreadSanitizer build
cmake -S . -B build-tsan -G Ninja -DANT_RPC_ENABLE_TSAN=ON \
  -DCMAKE_BUILD_TYPE=Debug
cmake --build build-tsan --parallel
ctest --test-dir build-tsan --output-on-failure
```

### Echo example

```bash
# Terminal 1: the system assigns both ports.
./build/rpc_e2e_server --port 0 --admin-port 0

# Terminal 2: use the RPC port printed by the server.
./build/rpc_e2e_client 127.0.0.1 <rpc-port> --requests 100 --threads 4 --seed 1
```

The example service is defined in [`proto/echo.proto`](proto/echo.proto). See [`doc/rpc-wire-protocol-v1.md`](doc/rpc-wire-protocol-v1.md) for the wire-format contract.

### Benchmarking

`benchmark/` provides a unary Protobuf Echo workload for AntRPC and bRPC. The GitLab pipeline uses `node1-shell` as server and `node2-shell` through `node8-shell` as seven clients. Stages run serially to prevent implementations or layouts from competing for the same machines.

It records achieved QPS, errors, normal-request p99/p999, and every QPS step. Deliberately injected tail requests are reported separately from normal latency. See [`benchmark/README.md`](benchmark/README.md) for runner requirements, inputs, artifacts, and result interpretation. Performance results are hardware-, workload-, and configuration-dependent; retain the pipeline artifact and full configuration with every reported result.

### Layout

```text
include/ant_rpc/rpc/       RPC channel, protocol, controller, server, runtime
include/ant_rpc/scheduler/ I/O contexts, timer keeper, worker executor
include/ant_rpc/metrics/   counters, gauges, latency recorder, registry
proto/                     Protobuf service and RPC metadata
test/                      unit, integration, and process-level tests
benchmark/                 AntRPC/bRPC benchmarks and CI scripts
doc/                       protocol documentation
```

<a id="中文"></a>

## 中文

AntRPC 是一个面向 Linux 的实验性 C++20 **Protobuf unary RPC** 框架，提供 Protobuf `RpcChannel` / `Service` 编程模型，支持 TCP 连接上的并发调用复用，并将网络 I/O 与业务执行分离。

本项目用于学习和验证 RPC runtime 设计，并非 bRPC 或 gRPC 的生产级替代品。

### 功能范围

- 支持基于 Protobuf 服务反射的 unary 调用，以及同步、回调和协程风格客户端。
- 提供带 correlation ID、metadata、attachment、帧长限制和网络字节序的 V1 二进制协议。
- 处理客户端 deadline、取消、连接失败，以及响应/取消/超时竞争下的 exactly-once 完成语义。
- 支持服务端连接数、in-flight 请求数、待执行 worker 任务和单连接出站缓冲区限额。
- 支持 `Stop()` / `Join()` 优雅停机，以及暴露健康状态、生命周期状态和 RPC 指标的 loopback 管理端点。

当前不支持 streaming RPC、TLS、服务发现、负载均衡、重试，也不兼容 gRPC/bRPC 线协议。

### 架构

- **多 Reactor I/O：** `Scheduler` 管理独立 I/O Context 和 worker 线程；连接仅在接入时分配一次。
- **连接 single-owner：** fd、缓冲区、in-flight 请求、出站队列和写状态只由所属 I/O 线程修改；worker 通过 I/O command mailbox 回投完成结果。
- **执行隔离：** 解析后的 RPC 请求由 work-stealing worker executor 执行，I/O 线程不执行用户 service 方法。
- **背压与停机：** 可配置准入限额控制资源；停机时先停止 accept，等待 deadline，之后由各连接 owner 关闭剩余连接。

默认 I/O 后端为 `epoll`；`io_uring` 可在配置时选择，需兼容的 Linux 内核和 `liburing`。

### 构建与测试

环境要求为 Linux、CMake 3.16+ 与 C++20 编译器（GCC 10+/Clang 12+）。默认 CMake 会获取 Protobuf、Abseil 和 GoogleTest；使用系统依赖时设置 `ANT_RPC_USE_SYSTEM_DEPS=ON`。

```bash
# 默认 epoll 后端
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

测试覆盖协议帧、超时/取消/响应竞争、执行器、服务端准入限制、优雅停机和进程级 RPC 场景。涉及 loopback TCP 的测试要求环境允许创建 socket。

```bash
# 可选 io_uring 后端
cmake -S . -B build-uring -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DANT_RPC_IO_BACKEND=io_uring
cmake --build build-uring --parallel

# ThreadSanitizer
cmake -S . -B build-tsan -G Ninja -DANT_RPC_ENABLE_TSAN=ON \
  -DCMAKE_BUILD_TYPE=Debug
cmake --build build-tsan --parallel
ctest --test-dir build-tsan --output-on-failure
```

### Echo 示例

```bash
# 终端 1：由系统自动分配 RPC 和管理端口。
./build/rpc_e2e_server --port 0 --admin-port 0

# 终端 2：使用 server 输出的 RPC 端口。
./build/rpc_e2e_client 127.0.0.1 <rpc-port> --requests 100 --threads 4 --seed 1
```

服务定义见 [`proto/echo.proto`](proto/echo.proto)，协议约定见 [`doc/rpc-wire-protocol-v1.md`](doc/rpc-wire-protocol-v1.md)。

### 压测

`benchmark/` 提供 AntRPC 与 bRPC 的 unary Protobuf Echo 对照负载。GitLab pipeline 使用 `node1-shell` 运行 server、`node2-shell` 至 `node8-shell` 运行 7 个 client，且各阶段串行执行以避免互相争抢机器。

压测记录实际 QPS、错误数、正常请求 p99/p999 与各 QPS 档位；刻意注入的长尾请求单独统计。详细运行条件、artifact 和结果解释见 [`benchmark/README.md`](benchmark/README.md)。性能结果依赖硬件、负载和配置，发布时应同时保留完整配置与 pipeline artifact。

### 目录结构

```text
include/ant_rpc/rpc/       RPC channel、协议、controller、server、runtime
include/ant_rpc/scheduler/ I/O Context、定时器、worker executor
include/ant_rpc/metrics/   counter、gauge、latency recorder、registry
proto/                     Protobuf service 与 RPC metadata
test/                      单元、集成和进程级测试
benchmark/                 AntRPC/bRPC 压测程序与 CI 脚本
doc/                       协议文档
```

## License / 许可证

This repository does not currently declare a top-level project license. Check the licenses of bundled components under `3rd_party/` before redistribution.

仓库目前未声明顶层项目许可证；分发前请分别检查 `3rd_party/` 中第三方组件的许可证。
