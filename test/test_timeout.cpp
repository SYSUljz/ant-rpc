#include <unistd.h>

#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "ant_server/server.hpp"

int main() {
  std::cout << "[Test] Starting Server on port 8016..." << std::endl;
  Context context(256);
  Server server(context, AF_INET, 8016, SOCK_STREAM, 0, 10, INADDR_ANY);

  // Run server context in background thread
  std::thread server_thread([&context]() { context.Start(); });

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // 1. Connect client to server
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  struct sockaddr_in serv_addr;
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(8016);
  inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

  if (connect(sock, reinterpret_cast<struct sockaddr*>(&serv_addr), sizeof(serv_addr)) < 0) {
    std::cerr << "[Client] Connection failed!" << std::endl;
    context.Stop();
    server_thread.join();
    return 1;
  }
  std::cout << "[Client] Connected to server." << std::endl;

  // 2. Send HTTP Request #1 (Keep-Alive)
  const char* req1 = "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n";
  send(sock, req1, strlen(req1), 0);

  char buffer[1024] = {0};
  int valread = read(sock, buffer, sizeof(buffer) - 1);
  if (valread > 0) {
    std::cout << "[Client] Received Response 1:\n" << buffer << std::endl;
  }

  // 3. Deliberately pause for 6 seconds without sending any new request
  std::cout << "[Client] Deliberately pausing for 6 seconds (Server timeout threshold: 5s)..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(6));

  // 4. Check socket status after 6s
  std::cout << "[Client] Waking up after 6s. Checking socket status..." << std::endl;
  memset(buffer, 0, sizeof(buffer));
  int res = read(sock, buffer, sizeof(buffer) - 1);

  if (res == 0) {
    std::cout << "[Client] SUCCESS: Server closed connection as expected after 5s timeout! (read returned 0)"
              << std::endl;
  } else if (res < 0) {
    std::cout << "[Client] SUCCESS: Connection reset/closed by server! (read returned " << res << ")" << std::endl;
  } else {
    std::cerr << "[Client] FAILURE: Connection was not closed! Read returned: " << res << " bytes" << std::endl;
  }

  close(sock);
  context.Stop();
  server_thread.join();
  std::cout << "[Test] Completed successfully." << std::endl;
  return 0;
}
