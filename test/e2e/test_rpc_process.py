#!/usr/bin/env python3
"""Process-level wire-protocol tests using C++ io_uring server and client."""

import argparse
import selectors
import socket
import struct
import subprocess
import sys
import threading
import time


HEADER_BYTES = 16
FLAGS_V1 = 0x01000000


def wire_header(magic: bytes = b"ANTR", body_len: int = 0, meta_len: int = 0, flags: int = FLAGS_V1) -> bytes:
    return magic + struct.pack(">III", body_len, meta_len, flags)


def varint(value: int) -> bytes:
    result = bytearray()
    while value >= 0x80:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def bytes_field(tag: int, value: bytes) -> bytes:
    return bytes([tag]) + varint(len(value)) + value


def request_frame(correlation_id: int, message: str) -> bytes:
    # RpcMeta: correlation_id, service_name, method_name. EchoRequest: message.
    meta = b"\x08" + varint(correlation_id)
    meta += bytes_field(0x1A, b"ant_rpc.EchoService") + bytes_field(0x22, b"Echo")
    body = bytes_field(0x0A, message.encode())
    return wire_header(body_len=len(body), meta_len=len(meta)) + meta + body


def non_response_frame() -> bytes:
    # A syntactically valid RpcMeta with its default msg_type (RPC_REQUEST).
    # A client channel must reject it even before correlation-id completion.
    meta = b"\x08\x01"
    return wire_header(meta_len=len(meta)) + meta


def non_request_frame() -> bytes:
    # RpcMeta: correlation_id=1, msg_type=RPC_RESPONSE. A server accepts only
    # RPC_REQUEST frames, even though this frame is otherwise wire-valid.
    meta = b"\x08\x01\x10\x01"
    return wire_header(meta_len=len(meta)) + meta


def read_ready(server: subprocess.Popen[str]) -> int:
    if server.stdout is None:
        raise RuntimeError("server stdout is not piped")
    selector = selectors.DefaultSelector()
    selector.register(server.stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + 5
    try:
        while time.monotonic() < deadline and server.poll() is None:
            if not selector.select(max(0.0, deadline - time.monotonic())):
                continue
            parts = server.stdout.readline().split()
            if len(parts) == 2 and parts[0] == "READY" and parts[1].isdigit():
                return int(parts[1])
    finally:
        selector.close()
    raise RuntimeError("server did not announce READY")


def stop_server(server: subprocess.Popen[str]) -> tuple[str, str]:
    if server.poll() is None and server.stdin is not None:
        try:
            server.stdin.write("quit\n")
            server.stdin.flush()
        except BrokenPipeError:
            pass
    try:
        return server.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        server.kill()
        return server.communicate()


def recv_exact(connection: socket.socket, count: int) -> bytes:
    result = bytearray()
    while len(result) < count:
        chunk = connection.recv(count - len(result))
        if not chunk:
            raise RuntimeError("peer closed before its complete response frame")
        result.extend(chunk)
    return bytes(result)


def recv_frame(connection: socket.socket) -> bytes:
    header = recv_exact(connection, HEADER_BYTES)
    body_len, meta_len, flags = struct.unpack(">III", header[4:])
    if header[:4] != b"ANTR" or flags != FLAGS_V1:
        raise RuntimeError("C++ server emitted an invalid wire header")
    return header + recv_exact(connection, body_len + meta_len)


def expect_server_rejects(port: int, name: str, frame: bytes) -> None:
    with socket.create_connection(("127.0.0.1", port), timeout=2) as connection:
        connection.settimeout(2)
        connection.sendall(frame)
        try:
            if connection.recv(1):
                raise RuntimeError(f"server accepted malformed {name} frame")
        except ConnectionResetError:
            pass
        except TimeoutError as error:
            raise RuntimeError(f"server did not reject malformed {name} frame") from error


def verify_fragmented_and_sticky_requests(port: int) -> None:
    first = request_frame(42, "fragmented")
    second = request_frame(43, "sticky-a")
    third = request_frame(44, "sticky-b")
    with socket.create_connection(("127.0.0.1", port), timeout=2) as connection:
        connection.settimeout(3)
        split = len(first) // 2
        connection.sendall(first[:split])
        time.sleep(0.05)
        connection.sendall(first[split:])
        if b"Echo: fragmented" not in recv_frame(connection):
            raise RuntimeError("fragmented request was not dispatched")
        connection.sendall(second + third)
        responses = {recv_frame(connection), recv_frame(connection)}
        if not any(b"Echo: sticky-a" in item for item in responses):
            raise RuntimeError("first sticky request was not dispatched")
        if not any(b"Echo: sticky-b" in item for item in responses):
            raise RuntimeError("second sticky request was not dispatched")


def verify_client_rejects_bad_response(client_path: str, name: str, response: bytes) -> None:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    errors: list[BaseException] = []

    def fake_server() -> None:
        try:
            connection, _ = listener.accept()
            with connection:
                connection.settimeout(3)
                if not connection.recv(1):
                    raise RuntimeError("C++ client did not send a request")
                connection.sendall(response)
                connection.shutdown(socket.SHUT_WR)
        except BaseException as error:
            errors.append(error)
        finally:
            listener.close()

    thread = threading.Thread(target=fake_server, daemon=True)
    thread.start()
    try:
        client = subprocess.run([client_path, "127.0.0.1", str(port)], text=True, capture_output=True, timeout=8)
    finally:
        thread.join(timeout=3)
    if thread.is_alive() or errors:
        raise RuntimeError(f"fake server failed in {name} response case: {errors}")
    if client.returncode == 0:
        raise RuntimeError(f"client accepted malformed {name} response")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True)
    parser.add_argument("--client", required=True)
    args = parser.parse_args()
    server = subprocess.Popen([args.server, "--port", "0"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True)
    failure: str | None = None
    try:
        port = read_ready(server)
        malformed = {
            "magic": wire_header(magic=b"NOPE"),
            "version": wire_header(flags=0x02000000),
            "feature": wire_header(flags=FLAGS_V1 | 1),
            "oversized-length": wire_header(body_len=0xFFFFFFFF, meta_len=0xFFFFFFFF),
            # RpcMeta field 9 attachment_size=2 while body_len=1.
            "attachment-boundary": wire_header(body_len=1, meta_len=2) + b"\x48\x02x",
            "message-type": non_request_frame(),
        }
        for name, frame in malformed.items():
            expect_server_rejects(port, name, frame)
        verify_fragmented_and_sticky_requests(port)
        verify_client_rejects_bad_response(args.client, "magic", wire_header(magic=b"NOPE"))
        verify_client_rejects_bad_response(args.client, "version", wire_header(flags=0x02000000))
        verify_client_rejects_bad_response(args.client, "oversized-length",
                                            wire_header(body_len=0xFFFFFFFF, meta_len=0xFFFFFFFF))
        verify_client_rejects_bad_response(args.client, "message-type", non_response_frame())
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
        failure = str(error)
    finally:
        stdout, stderr = stop_server(server)
    if failure:
        print(f"rpc process E2E failed: {failure}\nserver stdout:\n{stdout}\nserver stderr:\n{stderr}", file=sys.stderr)
        return 1
    if server.returncode != 0:
        print(f"server exited {server.returncode}\nstdout:\n{stdout}\nstderr:\n{stderr}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
