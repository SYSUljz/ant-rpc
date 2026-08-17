#include <unistd.h>

#include <iostream>
#include <string>

#include <arpa/inet.h>
#include <sys/socket.h>

int main() {
  int client_socket = socket(AF_INET, SOCK_STREAM, 0);
  if (client_socket < 0) {
    std::cerr << "create Socket faild!" << std::endl;
    return -1;
  }
  // comment: use butils/endpoint
  sockaddr_in server_address;
  server_address.sin_family = AF_INET;
  server_address.sin_port = htons(8012);

  if (inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr) <= 0) {
    std::cerr << "invalid address!" << std::endl;
    return -1;
  }

  std::cout << "connecting to 127.0.0.1:8012 ..." << std::endl;
  if (connect(client_socket, reinterpret_cast<struct sockaddr*>(&server_address), sizeof(server_address)) < 0) {
    std::cerr << "Failed connect" << std::endl;
    return -1;
  }
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
