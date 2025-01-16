#include <chrono>
#include <memory>
#include <thread>

#include <boost/thread/thread.hpp>

#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>

#include <aster_hiredis/AsterHiredis.hh>

using namespace seastar;
using namespace AsterHiredis;

void ConstructArgs(int *argc, char ***argv, size_t core_num_) {
  *argc = 5;

  // Set av0.
  char *av0 = new char[sizeof("useless")];
  memcpy(av0, "useless", sizeof("useless"));

  // Set av1.
  char *av1 = NULL;
  std::string str("--smp=");
  str += std::to_string(core_num_);
  av1 = new char[str.size() + 1]();
  memcpy(av1, str.c_str(), str.size());

  // Set av2.
  char *av2 = NULL;
  std::string thread_affinity("--thread-affinity=1");
  av2 = new char[thread_affinity.size() + 1]();
  memcpy(av2, thread_affinity.c_str(), thread_affinity.size());

  // Set av3.
  char *av3 = NULL;
  av3 = new char[sizeof("--poll-mode")];
  memcpy(av3, "--poll-mode", sizeof("--poll-mode"));

  // Set av4 if necessary.
  char *av4 = NULL;
  av4 = new char[sizeof("--dpdk-pmd")];
  memcpy(av4, "--dpdk-pmd", sizeof("--dpdk-pmd"));

  // Allocate one extra char for 'NULL' at the end.
  *argv = new char *[(*argc) + 1]();
  (*argv)[0] = av0;
  (*argv)[1] = av1;
  (*argv)[2] = av2;
  (*argv)[3] = av3;
  (*argv)[4] = av4;

  std::cout << "Construct args result, argc: " << *(argc)
            << ", argv[0]: " << (*argv)[0] << ", argv[1]: " << (*argv)[1]
            << ", argv[2]: " << (*argv)[2] << ", argv[3]: " << (*argv)[3]
            << ", argv[4]: " << (*argv)[4] << std::endl;
}

void DeConstructArgs(int *argc, char ***argv) {
  for (int i = 0; i < (*argc) + 1; i++) {
    delete[](*argv)[i];
  }
  delete[](*argv);
}

class APP {
 public:
  APP(){};
  ~APP(){};

  future<> fuck() {
    std::cout << "I say fuck!" << std::endl;
    return make_ready_future();
  }

  future<> AsyncStartServer() {
    return CreateInstance()
      .then([this](auto ptr) {
        // this->instance_ptr.reset(ptr);
        this->instance_ptr = ptr;
        client_is_ready = true;
        std::cout << "Start AsyncStartServer Finish" << std::endl;
        return app_stop.wait(1);
      })
      .handle_exception([this](auto ep) {
        app_stop.signal();
        return make_exception_future(ep);
      });
  }

  seastar::semaphore app_stop{0};
  bool client_is_ready = false;

  // seastar::foreign_ptr<seastar::shared_ptr<RedisClientBase>> instance_ptr;
  seastar::shared_ptr<RedisClientBase> instance_ptr;

 private:
  future<seastar::shared_ptr<RedisClient<RedisClientType::CLUSTER>>>
  CreateInstance() {
    auto client_params = ClientParams();
    client_params.hosts_and_ports = Node("127.0.0.1", 1996);
    client_params.redis_instance_type = RedisClientType::CLUSTER;
    client_params.redis_role = RedisRole::MASTER;
    client_params.redis_password = "redis";
    return RedisClient<RedisClientType::CLUSTER>::GetSeastarRedisClient(
      client_params, 128);
  }

 public:
  future<RedisReplyVectorSPtr> set_ok_test() {
    auto client_instance_ptr = this->instance_ptr.get();
    return do_with(std::move(client_instance_ptr),
                   [](auto ptr) {
                     return ptr->Send("key", "set key val")
                       .then([ptr = std::move(ptr)] {
                         return ptr->Recv("key").then(
                           [ptr = std::move(ptr)](RedisReplyVectorSPtr rptr) {
                             for (auto reply_it = rptr->begin();
                                  reply_it != rptr->end(); ++reply_it) {
                               if ((*reply_it)->type == REDIS_REPLY_STRING ||
                                   (*reply_it)->type == REDIS_REPLY_STATUS) {
                                 std::cout << (*reply_it)->str << std::endl;
                               }
                             }
                             return rptr;
                           });
                       });
                   })
      .handle_exception([](auto ep) {
        return make_exception_future<ConnectionUnit::RedisReplyVectorSPtr>(ep);
      });
  }

  future<> set_test() {
    auto client_instance_ptr = this->instance_ptr.get();
    return do_with(std::move(client_instance_ptr),
                   [](auto ptr) { return ptr->Send("key", "set key val"); })
      .handle_exception([](auto ep) { return make_exception_future<>(ep); });
  }
};

void start_server(size_t core_num_, std::shared_ptr<app_template> app_,
                  std::shared_ptr<distributed<APP>> app_engine_,
                  std::shared_ptr<bool> app_started_) {
  int argc = 1;
  char **argv = NULL;
  ConstructArgs(&argc, &argv, core_num_);
  app_->run(argc, argv, [&] {
    return app_engine_->start()
      .then([&] {
        *app_started_ = true;
        return app_engine_->invoke_on_all(&APP::AsyncStartServer);
      })
      .then([&] { return app_engine_->stop(); })
      .handle_exception([&](auto ep) { return app_engine_->stop(); });
  });
  DeConstructArgs(&argc, &argv);
}

int main(int argc = 0, char **argv = NULL) {
  auto app_ = std::make_shared<app_template>();
  auto app_engine = std::make_shared<distributed<APP>>();
  const size_t core_num = 1;
  auto app_started = std::make_shared<bool>(false);

  std::thread thread_;
  thread_ =
    std::thread(&start_server, core_num, app_, app_engine, app_started);
  // boost::thread thread_(start_server, core_num, app_, app_engine, &app_started);

  const auto time_start = std::chrono::steady_clock::now();
  const auto timeout_ms = std::chrono::milliseconds(10000000) + time_start;
  bool client_is_ready[core_num];
  bool all_ready = false;
  size_t app_stop_au[core_num];
  while (*app_started == false)
    ;
  while (true) {
    for (auto icore = 0; icore < core_num; ++icore) {
      client_is_ready[icore] =
        seastar::alien::submit_to(app_->alien(), icore, [app_engine] {
          return make_ready_future<bool>(app_engine->local().client_is_ready);
        }).get();
      app_stop_au[icore] =
        seastar::alien::submit_to(app_->alien(), icore, [app_engine] {
          return make_ready_future<size_t>(
            app_engine->local().app_stop.available_units());
        }).get();
    }
    for (auto icore = 0; icore < core_num; ++icore) {
      if (client_is_ready[icore] == false) all_ready = false;
      if (std::chrono::steady_clock::now() > timeout_ms)
        throw std::runtime_error("timeout");
      if (app_stop_au[icore] > 0) {
        thread_.join();
        throw std::runtime_error("GG");
      }
    }
    if (all_ready) {
      break;
    } else {
      all_ready = true;
    }
  }

  auto set_ok_test = seastar::alien::submit_to(app_->alien(), 0, [app_engine] {
    return app_engine->local().set_ok_test();
  });
  try {
    set_ok_test.get();
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    // return -1;
  }

  std::cout << "test start" << std::endl;
  const size_t count = 1000;
  std::vector<std::future<void>> redis_set[core_num];
  for (auto icore = 0; icore < core_num; ++icore) {
    redis_set[icore].resize(count);
  }
  const auto tick = std::chrono::high_resolution_clock::now();
  while (true) {
    for (auto icore = 0; icore < core_num; ++icore) {
      for (size_t i = 0; i < count; ++i) {
        redis_set[icore].at(i) =
          seastar::alien::submit_to(app_->alien(), icore, [app_engine] {
            return app_engine->local().set_test();
          });
      }
    }
    for (auto icore = 0; icore < core_num; ++icore) {
      for (size_t i = 0; i < count; ++i) {
        try {
          redis_set[icore].at(i).get();
        } catch (const std::exception &e) {
          std::cerr << e.what() << std::endl;
          // return -1;
        }
      }
    }
    const auto tock = std::chrono::high_resolution_clock::now();
    const auto diff =
      std::chrono::duration_cast<std::chrono::nanoseconds>(tock - tick).count();
    std::cout << "test end diff=" << diff << std::endl;
    std::cout << 1E9 * static_cast<double>(count * core_num) /
                   static_cast<double>(diff)
              << std::endl;
  }

  for (auto icore = 0; icore < core_num; ++icore) {
    seastar::alien::submit_to(app_->alien(), icore, [app_engine] {
      app_engine->local().app_stop.signal();
      return seastar::make_ready_future<>();
    }).get();
  }
  thread_.join();

  return 0;
}
