#include <chrono>
#include <memory>
#include <thread>

#include <seastar/core/app-template.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>

using namespace seastar;

int main(int argc = 0, char **argv = NULL) {
  auto app_ = std::make_unique<seastar::app_template>();
  return app_->run(argc, argv, [&] {
    return make_ready_future();
  });
}

// int main(int args, char** argv, char** env) {
//     seastar::app_template app;
//     return app.run(args, argv, [&] {
//         auto fut1 = seastar::thread([] {
//             std::cout << "test 1" << std::endl;
//         });

//         return seastar::async([&] {
//             std::cout << "test 2" << std::endl;
//         });
//     });
// }