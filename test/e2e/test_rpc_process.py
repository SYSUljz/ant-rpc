#!/usr/bin/env python3
"""Cross-process RPC smoke test.

Python only orchestrates child processes. Both endpoints use the project's C++
Scheduler, io_uring transport, protobuf protocol, RpcServer, and RpcChannel.
"""

import argparse
import selectors
import subprocess
import sys
import time


def read_ready(server: subprocess.Popen[str], timeout_seconds: float) -> int:
    if server.stdout is None:
        raise RuntimeError("server stdout is not piped")
    selector = selectors.DefaultSelector()
    selector.register(server.stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + timeout_seconds
    lines: list[str] = []
    try:
        while time.monotonic() < deadline:
            if server.poll() is not None:
                break
            events = selector.select(max(0.0, deadline - time.monotonic()))
            if not events:
                continue
            line = server.stdout.readline()
            if not line:
                break
            lines.append(line.rstrip())
            parts = line.split()
            if len(parts) == 2 and parts[0] == "READY" and parts[1].isdigit():
                port = int(parts[1])
                if 0 < port <= 65535:
                    return port
        raise RuntimeError("server did not announce READY; stdout=" + repr(lines))
    finally:
        selector.close()


def stop_server(server: subprocess.Popen[str]) -> tuple[str, str]:
    if server.poll() is None and server.stdin is not None:
        try:
            server.stdin.write("quit\n")
            server.stdin.flush()
        except BrokenPipeError:
            pass
    try:
        stdout, stderr = server.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        server.terminate()
        try:
            stdout, stderr = server.communicate(timeout=3)
        except subprocess.TimeoutExpired:
            server.kill()
            stdout, stderr = server.communicate()
    return stdout, stderr


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True)
    parser.add_argument("--client", required=True)
    args = parser.parse_args()

    server = subprocess.Popen(
        [args.server, "--port", "0"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    failure: str | None = None
    try:
        port = read_ready(server, 5)
        client = subprocess.run([args.client, "127.0.0.1", str(port)], text=True, capture_output=True, timeout=10)
        if client.returncode != 0:
            raise RuntimeError(
                f"client exited {client.returncode}\nstdout:\n{client.stdout}\nstderr:\n{client.stderr}"
            )
        if "OK process-e2e" not in client.stdout:
            raise RuntimeError("client did not confirm RPC success:\n" + client.stdout)
    except (subprocess.TimeoutExpired, RuntimeError) as error:
        failure = str(error)
    finally:
        server_stdout, server_stderr = stop_server(server)

    if failure:
        print(
            f"rpc process E2E failed: {failure}\n"
            f"server stdout:\n{server_stdout}\nserver stderr:\n{server_stderr}",
            file=sys.stderr,
        )
        return 1
    if server.returncode != 0:
        print(
            f"server exited {server.returncode}\nstdout:\n{server_stdout}\nstderr:\n{server_stderr}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
