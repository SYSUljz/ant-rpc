#!/usr/bin/env python3
"""Exercise the benchmark server's real sigwait/Stop/Join shutdown path."""

import argparse
import selectors
import signal
import socket
import struct
import subprocess
import tempfile
import time


def available_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def varint(value):
    output = bytearray()
    while value >= 128:
        output.append((value & 127) | 128)
        value >>= 7
    output.append(value)
    return bytes(output)


def field(tag, value):
    return bytes((tag,)) + varint(len(value)) + value


def request(correlation_id, message):
    meta = b"\x08" + varint(correlation_id)
    meta += field(0x1A, b"ant_rpc.EchoService") + field(0x22, b"Echo")
    body = field(0x0A, message)
    return b"ANTR" + struct.pack(">III", len(body), len(meta), 0x01000000) + meta + body


def wait_ready(process, port):
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"server exited before READY (code={process.returncode})")
            if selector.select(min(0.1, max(0, deadline - time.monotonic()))):
                line = process.stdout.readline()
                if f"ANTRPC_BENCH_READY port={port} " in line:
                    return
                if not line:
                    break
    raise RuntimeError("server did not announce READY")


def scenario(server_path, name, mode="cooperative"):
    port = available_port()
    connections = []
    with tempfile.TemporaryFile(mode="w+t") as log:
        process = subprocess.Popen(
            [server_path, f"--port={port}", "--worker_threads=2", "--io_threads=2",
             "--io_contexts=2", "--tail_latency_us=500000", f"--tail_mode={mode}"],
            stdout=subprocess.PIPE, stderr=log, text=True,
        )
        try:
            wait_ready(process, port)
            if name != "idle":
                count = 8 if name == "burst" else 1
                for _ in range(count):
                    connection = socket.create_connection(("127.0.0.1", port), timeout=2)
                    connection.settimeout(2)
                    connections.append(connection)
                if name == "partial_frame":
                    connections[0].sendall(b"ANTR\x00\x00")
                elif name in ("tail", "burst"):
                    for index, connection in enumerate(connections):
                        if name == "burst":
                            # Pipelined calls exercise per-connection inbound
                            # accounting and worker/IO completion during Stop.
                            connection.sendall(b"".join(
                                request(index * 128 + sequence + 1,
                                        b"\x00tail" if sequence % 100 == 0 else b"normal")
                                for sequence in range(128)
                            ))
                        else:
                            connection.sendall(request(index + 1, b"\x00tail"))
                    # Ensure the server has received a request before signaling;
                    # a 500ms tail remains outstanding in both service modes.
                    time.sleep(0.1)

            process.send_signal(signal.SIGTERM)
            try:
                exit_code = process.wait(timeout=8)
            except subprocess.TimeoutExpired as error:
                raise AssertionError(f"{name}/{mode}: server did not exit within 8s after SIGTERM") from error
            if exit_code != 0:
                raise AssertionError(f"{name}/{mode}: expected exit 0, got {exit_code}")
            log.seek(0)
            shutdown_log = log.read()
            if "ANTRPC_SHUTDOWN_BEGIN signal=15" not in shutdown_log or "ANTRPC_SHUTDOWN_END drained=1" not in shutdown_log:
                raise AssertionError(f"{name}/{mode}: shutdown diagnostics missing:\n{shutdown_log}")
            print(f"PASS {name}/{mode}: exited cleanly after SIGTERM", flush=True)
        except BaseException:
            log.seek(0)
            print(f"FAIL {name}/{mode}: server stderr:\n{log.read()}", flush=True)
            raise
        finally:
            for connection in connections:
                connection.close()
            if process.poll() is None:
                process.kill()
                process.wait(timeout=3)
            if process.stdout is not None:
                process.stdout.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True)
    args = parser.parse_args()
    for name, mode in (("idle", "cooperative"), ("idle_connection", "cooperative"),
                       ("partial_frame", "cooperative"), ("tail", "cooperative"),
                       ("tail", "blocking"), ("burst", "cooperative")):
        scenario(args.server, name, mode)


if __name__ == "__main__":
    main()
