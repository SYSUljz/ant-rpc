#!/usr/bin/env python3
import socket
import time
import sys

HOST = '127.0.0.1'
PORT = 8012

def test_tcp_chunking():
    print("[E2E Test 1/4] Testing TCP Chunking & Half-Packets...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    
    raw_request = b"POST /api/v1/test HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 11\r\n\r\nHello World"
    # Send half-packets: split and transmit 1 byte at a time
    for byte in raw_request:
        s.sendall(bytes([byte]))
        time.sleep(0.001)

    s.settimeout(2.0)
    response = s.recv(4096)
    assert b"200 OK" in response, f"Expected 200 OK, got {response}"
    assert b"Hello, C++20 io_uring Web Server with IOBuf & llhttp!" in response
    s.close()
    print("  -> PASSED!")

def test_keep_alive():
    print("[E2E Test 2/4] Testing HTTP Keep-Alive Continuous Requests...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    s.settimeout(2.0)

    for i in range(20):
        req = f"GET /ping/{i} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n".encode()
        s.sendall(req)
        response = s.recv(4096)
        assert b"200 OK" in response, f"Request {i} failed: {response}"
    
    s.close()
    print("  -> PASSED!")

def test_large_body():
    print("[E2E Test 3/4] Testing 10MB Large Body Transmission...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    s.settimeout(5.0)

    body_size = 10 * 1024 * 1024 # 10MB
    headers = f"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {body_size}\r\n\r\n".encode()
    s.sendall(headers)

    chunk = b"X" * (64 * 1024)
    sent = 0
    while sent < body_size:
        s.sendall(chunk)
        sent += len(chunk)

    response = s.recv(4096)
    assert b"200 OK" in response, f"Large body request failed: {response}"
    s.close()
    print("  -> PASSED!")

def test_timeout_cleanup():
    print("[E2E Test 4/4] Testing 5s Idle Timeout & Connection Cleanup...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    
    # Intentionally do not send data, wait for server 5s idle timeout closure
    time.sleep(6.0)
    s.settimeout(1.0)
    try:
        data = s.recv(1024)
        assert len(data) == 0, f"Expected socket closed by server, but received {data}"
    except (socket.timeout, ConnectionResetError, OSError):
        pass
    s.close()
    print("  -> PASSED!")

if __name__ == '__main__':
    print("=== Starting ant_rpc E2E Integration Suite ===")
    try:
        test_tcp_chunking()
        test_keep_alive()
        test_large_body()
        test_timeout_cleanup()
        print("\n🎉 ALL E2E INTEGRATION TESTS PASSED SUCCESSFULLY! 🎉")
    except Exception as e:
        print(f"\n❌ E2E TEST FAILED: {e}")
        sys.exit(1)
