#include <chrono>
#include <utility> 
#include <memory>
#include <thread>

#include <seastar/core/alien.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/api.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/net.hh>

#include <seastar/core/thread.hh>

class APP {
 private:
  void ConstructArgs(int *argc, char ***argv) {
    *argc = 4;

    // Set av0.
    char *av0 = new char[sizeof("useless")];
    memcpy(av0, "useless", sizeof("useless"));

    // Set av1.
    char *av1 = NULL;
    std::string str("--smp=");
    str += std::to_string(1);
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

  static auto timeout_end(uint32_t timeout_ms) {
    return std::chrono::steady_clock::now() +
           std::chrono::milliseconds(timeout_ms);
  }

  seastar::future<seastar::connected_socket>
  my_connect(const seastar::sstring &host, uint16_t port, uint32_t timeout_ms) {
    return seastar::net::dns::resolve_name(host).then(
      [port, timeout_ms](seastar::net::inet_address target_host) {
        sa_family_t family =
          target_host.is_ipv4() ? sa_family_t(AF_INET) : sa_family_t(AF_INET6);
        seastar::socket_address local =
          seastar::socket_address(::sockaddr_in{family, INADDR_ANY, {0}});
        auto f = target_host.is_ipv4()
                   ? seastar::connect(seastar::ipv4_addr{target_host, port},
                                      local, seastar::transport::TCP)
                   : seastar::connect(seastar::ipv6_addr{target_host, port},
                                      local, seastar::transport::TCP);
        auto f_timeout =
          seastar::with_timeout(timeout_end(timeout_ms), std::move(f));
        return f_timeout;
      });
  }

 public:
  APP();
  ~APP();

  std::thread thread_;

  seastar::app_template::seastar_options *opt_;
  std::unique_ptr<seastar::app_template> app_;

  seastar::connected_socket my_connected_socket;

  struct Connection {
    seastar::output_stream<char> write_buf_;
    seastar::input_stream<char> read_buf_;
    seastar::connected_socket fd_;
    bool connection_stream_alive_ = true;
    Connection(seastar::connected_socket &&fd)
      : fd_(std::move(fd)), write_buf_(fd.output().detach(), 8192),
        read_buf_(fd.input()){};
    ~Connection() {
      fd_.shutdown_output();
      fd_.shutdown_input();
    };
  };

  Connection *main_conn;
  std::vector<Connection *> conns;

  std::map<size_t, std::pair<bool, seastar::temporary_buffer<char>>> reply_map;

  seastar::temporary_buffer<char> tmp_buf;

  seastar::semaphore finished{0};
  bool client_is_ready = false;

  void AsyncStartServer() {
    int argc = 0;
    char **argv = NULL;
    ConstructArgs(&argc, &argv);

    app_ = std::make_unique<seastar::app_template>();

    app_->run(argc, argv, [&] {
      std::vector<int> numbers{0, 1, 2};
      return seastar::do_with(
        std::move(numbers), [&](std::vector<int> &numbers) {
          return seastar::do_for_each(
                   numbers,
                   [&](int number) {
                     return my_connect("127.0.0.1", 6379, 10)
                       .then([&](seastar::connected_socket &&socket) {
                         std::cout << " Connected " << std::endl;
                         this->conns.emplace_back(
                           new Connection(std::move(socket)));
                         this->main_conn = this->conns.back();
                         return seastar::make_ready_future();
                       });
                   })
            .then([&]() {
              client_is_ready = true;
              return this->finished.wait(1).then([&] {
                std::cout << "fuck" << std::endl;
                delete this->main_conn;
                return seastar::sleep(std::chrono::milliseconds(10));
              });
            });
        });
    });

    for (int i = 0; i < argc + 1; i++) {
      delete[] argv[i];
    }
    delete[] argv;
  };

  seastar::future<seastar::temporary_buffer<char>> ping_1_socket() {
    size_t id_send = 0;
    size_t id_recv = 0;
    return seastar::do_with(
      std::move(id_send), std::move(id_recv), [&](size_t id_send, size_t id_recv) {
        return main_conn->write_buf_.write("PING\r\n")
          .then([this, &id_send] {
            reply_map.insert({id_send, std::make_pair(true, seastar::temporary_buffer<char>())});
            ++id_send;
            return main_conn->write_buf_.flush();
          })
          .then([this] {
            return main_conn->write_buf_.write("PING haha\r\n");
          })
          .then([this, &id_send] {
            reply_map.insert({id_send, std::make_pair(true, seastar::temporary_buffer<char>())});
            ++id_send;
            return main_conn->write_buf_.flush();
          })
          // .then([this, &id_send, &id_recv]() {
          //   return main_conn->read_buf_.read()
          //     .then([this, &id_send, &id_recv](seastar::temporary_buffer<char> &&buf) {
          //       auto start = buf.begin();
          //       auto step = 7;
          //       while (id_recv < id_send) {
          //         std::cout << "a read_buf_ " << id_recv << ": "
          //               << std::string(buf.begin(), buf.size()) << std::endl;
          //         std::cout << "b read_buf_ " << id_recv << ": "
          //               << std::string(start, step) << std::endl;
          //         if (reply_map[id_recv].first == true) {
          //           reply_map[id_recv] = std::make_pair(true, seastar::temporary_buffer<char>(start, step));
          //         }
          //         start = start + step + 1;
          //         step = 12;
          //         auto &&tmp_buf = reply_map[id_recv].second;
          //         std::cout << "c read_buf_ " << id_recv << ": "
          //               << std::string(tmp_buf.begin(), tmp_buf.size()) << std::endl;
          //         ++id_recv;
          //       }
          //       exit(0);
          //       return seastar::make_ready_future();
          //     });
          // })
          .then([this] {
            return main_conn->write_buf_.write("PING hehe\r\n");
          })
          .then([this, &id_send] {
            reply_map.insert({id_send, std::make_pair(true, seastar::temporary_buffer<char>())});
            ++id_send;
            return main_conn->write_buf_.flush();
          })
          // .then([this, &id_send, &id_recv]() {
          //   return main_conn->read_buf_.read()
          //     .then([this, &id_send, &id_recv](seastar::temporary_buffer<char> &&buf) {
          //       auto start = buf.begin();
          //       auto step = 12;
          //       while (id_recv < id_send) {
          //         if (reply_map[id_recv].first == true) {
          //           reply_map[id_recv] = std::make_pair(true, seastar::temporary_buffer<char>(start, step));
          //         }
          //         ++id_recv;
          //       }
          //       return std::move(buf);
          //     });
          // });
          .then([this] {
            return seastar::temporary_buffer<char>();
          });
      });
  }

  seastar::future<seastar::temporary_buffer<char>> ping_3_socket() {
    return conns[0]
      ->write_buf_.write("PING\r\n")
      .then([this] {
        return conns[0]->write_buf_.flush();
      })
      .then([this] {
        return conns[1]->write_buf_.write("PING haha\r\n");
      })
      .then([this] {
        return conns[1]->write_buf_.flush();
      })
      // .then([this]() {
      //   return conns[1]->read_buf_.read()
      //     .then([this](seastar::temporary_buffer<char> &&buf) {
      //       // std::cout << "read_buf_1: "
      //       //       << std::string(buf.begin(), buf.size()) << std::endl;
      //       // return seastar::make_ready_future();
      //     });
      // })
      // .then([this]() {
      //   return conns[0]->read_buf_.read()
      //     .then([this](seastar::temporary_buffer<char> &&buf) {
      //       // std::cout << "read_buf_0: "
      //       //       << std::string(buf.begin(), buf.size()) << std::endl;
      //       // return seastar::make_ready_future();
      //     });
      // })
      .then([this] {
        return conns[2]->write_buf_.write("PING hehe\r\n");
      })
      .then([this] {
        return conns[2]->write_buf_.flush();
      })
      // .then([this]() {
      //   return conns[2]->read_buf_.read()
      //     .then([this](seastar::temporary_buffer<char> &&buf) {
      //       // std::cout << "read_buf_2: "
      //       //       << std::string(buf.begin(), buf.size()) << std::endl;
      //       return std::move(buf);
      //     });
      // });
      .then([this] {
        return seastar::temporary_buffer<char>();
      });
  }

  seastar::future<> nothing() {
    std::cout << "fuck" << std::endl;
    return seastar::make_ready_future();
  }
};

APP::APP() { thread_ = std::thread(&APP::AsyncStartServer, this); }

APP::~APP() {}

int main(int argc, char **argv) {
  auto app_engine = std::make_unique<APP>();

  while (!app_engine->client_is_ready) {
    std::cout << !app_engine->client_is_ready;
    std::cout << "\b ";
  }

  double core_num = 1;
  size_t count = 1000;
    
  auto tick = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < count; ++i) {
    auto fut0 =
    seastar::alien::submit_to(app_engine->app_->alien(), 0, [&app_engine] {
      return app_engine->ping_1_socket().then(
        [](seastar::temporary_buffer<char> &&buf) {
          std::cout << std::string(buf.begin(), buf.size()) << std::endl;
          buf.release();
          return seastar::make_ready_future();
        });
    });
    fut0.get();
  }
  auto tock = std::chrono::high_resolution_clock::now();
  const auto one_socket_diff =
    std::chrono::duration_cast<std::chrono::nanoseconds>(tock - tick).count();

  tick = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < count; ++i) {
    auto fut0 =
    seastar::alien::submit_to(app_engine->app_->alien(), 0, [&app_engine] {
      return app_engine->ping_3_socket().then(
        [](seastar::temporary_buffer<char> &&buf) {
          std::cout << std::string(buf.begin(), buf.size()) << std::endl;
          buf.release();
          return seastar::make_ready_future();
        });
    });
    fut0.get();
  }
  tock = std::chrono::high_resolution_clock::now();
  const auto multi_socket_diff =
    std::chrono::duration_cast<std::chrono::nanoseconds>(tock - tick).count();

  std::cout << "multi_socket test end diff=" << multi_socket_diff << std::endl;
  std::cout << "multi_socket Speed: " << 1E9 * static_cast<double>(count * core_num) /
                  static_cast<double>(multi_socket_diff)
            << " per seconds"
            << std::endl;

  std::cout << "one_socket_diff test end diff=" << one_socket_diff << std::endl;
  std::cout << "one_socket_diff Speed: " << 1E9 * static_cast<double>(count * core_num) /
                  static_cast<double>(one_socket_diff)
            << " per seconds"
            << std::endl;

  auto signal =
    seastar::alien::submit_to(app_engine->app_->alien(), 0, [&app_engine] {
      app_engine->finished.signal();
      return seastar::make_ready_future<>();
    });
  signal.get();
  app_engine->thread_.join();
  return 0;
}