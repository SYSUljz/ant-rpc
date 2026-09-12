#include "ant_rpc/logging/logging.hpp"
#include "ant_rpc/scheduler/scheduler.hpp"
#include "ant_rpc/server.hpp"
int main() {
  ant_rpc::logging::Initialize();
  Scheduler scheduler(1, 1);
  Context& context = scheduler.GetIOContext(0);
  Server server = Server(context, AF_INET, 8012, SOCK_STREAM, 0, 10, INADDR_ANY);
  scheduler.Start();
  scheduler.Wait();
  return 0;
}
