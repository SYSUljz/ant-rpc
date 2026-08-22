#include <unistd.h>

#include <iostream>
#include <string>

#include "butil/endpoint.h"

int main() {
  butil::EndPoint server_endpoint;
  if (butil::str2endpoint("127.0.0.1:8012", &server_endpoint) != 0) {
    std::cerr << "invalid address!" << std::endl;
    return -1;
  }

  int client_socket = butil::tcp_connect(server_endpoint, nullptr, 1000);
  if (client_socket < 0) {
    std::cerr << "Failed connect" << std::endl;
    return -1;
  }

  std::cout << "connecting to 127.0.0.1:8012 ..." << std::endl;
  std::cout << "connect success！\n" << std::endl;

  std::string http_request =
      "GET / HTTP/1.1\r\n"
      "Host: 127.0.0.1:8012\r\n"
      "Connection: close\r\n"
      "\r\n";

  send(client_socket, http_request.c_str(), http_request.length(), 0);
  std::cout << "--- send request ---\n" << http_request;

  char buffer[4096] = {0};
  std::cout << "--- recive request ---\n";

  int bytes_read;
  while ((bytes_read = read(client_socket, buffer, sizeof(buffer) - 1)) > 0) {
    std::cout << buffer;
    std::fill(std::begin(buffer), std::end(buffer), 0);
  }
  std::cout << "\n--------------------\n";

  close(client_socket);
  std::cout << "connect closed" << std::endl;

  return 0;
}
