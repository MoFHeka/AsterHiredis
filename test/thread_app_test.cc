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
  my_connect(const seastar::std::string &host, uint16_t port, uint32_t timeout_ms) {
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
      // seastar::thread th([this] {
      //     std::cout << "Hi0.\n";
      //     write_buf_.close().get();
      //     std::cout << "Hi1.\n";
      //     read_buf_.close().get();
      //     std::cout << "Hi2.\n";
      //     connection_stream_alive_ = false;
      // });
      // (void)do_with(std::move(th), [] (auto& th) {
      //   return th.join();
      // });
      // while(connection_stream_alive_);
    };
  };

  Connection *conn;
  
  // std::vector<Connection *> conns;

  seastar::temporary_buffer<char> tmp_buf;

  seastar::semaphore finished{0};
  bool client_is_ready = false;

  void AsyncStartServer() {
    int argc = 0;
    char **argv = NULL;
    ConstructArgs(&argc, &argv);

    app_ = std::make_unique<seastar::app_template>();

    app_->run(argc, argv, [&] {
      return my_connect("127.0.0.1", 6379, 10)
        .then([&](seastar::connected_socket &&socket) {
          this->my_connected_socket = std::move(socket);
          this->conn = new Connection(std::move(this->my_connected_socket));
          // std::cout << my_connected_socket.get_keepalive() << "\n";
          // this->my_connected_socket.set_keepalive(true);
          // std::cout << this->my_connected_socket.get_keepalive() << "\n";
          // this->read_buf_ = this->my_connected_socket.input();
          // this->write_buf_ = this->my_connected_socket.output();
          client_is_ready = true;
          return this->finished.wait(1).then([&] {
            // return this->read_buf_.close();
            // this->write_buf_.close();
            std::cout << "fuck" << std::endl;
            // this->conn->write_buf_.close().get();
            // this->conn->read_buf_.close().get();
            // auto fut0 = seastar::smp::submit_to(0,
            //     [&]{
            //       return this->conn->write_buf_.close().get();
            //     }
            // );
            delete this->conn;
            return seastar::sleep(std::chrono::milliseconds(10));
          });
        });
    });

    for (int i = 0; i < argc + 1; i++) {
      delete[] argv[i];
    }
    delete[] argv;
  };

  seastar::future<> set_keepalive(seastar::connected_socket &socket,
                                  bool setting) {
    std::cout << "Now keepalive is " << socket.get_keepalive() << "\n";
    socket.set_keepalive(setting);
    std::cout << "keepalive was modyfied to " << socket.get_keepalive() << "\n";
    return seastar::make_ready_future<>();
  }

  seastar::future<seastar::temporary_buffer<char>> ping(int times) {
    return conn->write_buf_.write("PING\r\n")
      .then([conn = std::move(conn), this] { return conn->write_buf_.flush(); })
      .then([conn = std::move(conn), this] {
        return conn->write_buf_.write("PING haha\r\n");
      })
      .then([conn = std::move(conn), this] { return conn->write_buf_.flush(); })
      .then([conn = std::move(conn), this, times] {
        return conn->read_buf_.read()
          .then([conn = std::move(conn), this,
                 times](seastar::temporary_buffer<char> &&buf) {
            // auto tmp_buf = conn->read_buf_.read().get();
            // std::cout << "fuck:" << std::string(tmp_buf.begin(),
            // tmp_buf.size()) << std::endl;
            if (buf.size() != 7) {
              fmt::print(std::cerr,
                         "illegal packet received: {}, buffer size is {}\n",
                         std::string(buf.begin(), buf.size()), buf.size());
              // return seastar::make_ready_future();
              // return std::move(buf);
              return seastar::make_exception_future<seastar::temporary_buffer<char>>(std::runtime_error(std::string(buf.begin(),buf.size())));
            }
            // auto str = std::string(buf.get(), buf.size());
            // if (str != "+PONG\r\n") {
            //   fmt::print(std::cerr, "illegal packet received: {}\n",
            //              std::string(buf.begin(), buf.size()));
            //   // return seastar::make_ready_future();
            //   return std::move(buf);
            // }
            // if (times > 0) {
            //   // auto &&tem_buf = ping(times - 1).get();
            //   // return std::move(tem_buf);
            //   return std::move(buf);
            // } else {
            //   std::cout << "success!" << std::endl;
            //   // return seastar::make_ready_future();
            //   return std::move(buf);
            // }
          })
          .then([conn = std::move(conn),
                 this](seastar::temporary_buffer<char> &&buf) {
            std::cout << "fuck" << std::endl;
            tmp_buf = seastar::temporary_buffer<char>(std::move(buf));
            std::cout << "fuck0 " << std::string(this->tmp_buf.begin(), this->tmp_buf.size())
                          << std::endl;
            return conn->write_buf_.write("PING hehe\r\n")
              .then([this, conn = std::move(conn)]() {
                std::cout << "fuck1 " << std::string(this->tmp_buf.begin(), this->tmp_buf.size())
                          << std::endl;
                return conn->write_buf_.flush().then(
                  [this] {
                    std::cout << "fuck2 " << std::string(this->tmp_buf.begin(), this->tmp_buf.size())
                          << std::endl;
                    return std::move(tmp_buf);
                  });
              });
          })
          .handle_exception([this](auto ep) {
            try {
              std::rethrow_exception(ep);
            } catch (const std::exception &e) {
              seastar::std::string err = "[AsterHiredis] Sending Command Error -- ";
              std::cerr << strlen(e.what()) << std::endl;
              err.append(e.what(), strlen(e.what()));
              std::cerr << err << std::endl;
              return seastar::make_exception_future<seastar::temporary_buffer<char>>(
                std::make_exception_ptr(std::runtime_error(err.begin())));
            }
          }).
          finally([this]{std::cerr << "finally" << std::endl;});
      });
  }

  seastar::future<> nothing() {
    std::cout << "fuck" << std::endl;
    return seastar::make_ready_future();
  }
};

APP::APP() { thread_ = std::thread(&APP::AsyncStartServer, this); }

APP::~APP() {
  // delete[] argv;
}

int main(int argc, char **argv) {
  auto app_engine = std::make_unique<APP>();

  while (true) {
    if (app_engine->client_is_ready) break;
  }

  auto fut0 =
    seastar::alien::submit_to(app_engine->app_->alien(), 0, [&app_engine] {
      return app_engine->ping(10).then(
        [](seastar::temporary_buffer<char> &&buf) {
          std::cout << std::string(buf.begin(), buf.size()) << std::endl;
          buf.release();
          return seastar::make_ready_future();
        });
    });

  // auto fut1 = seastar::alien::submit_to(
  //     app_engine->app_->alien(), 0,
  //     [&]{
  //       return app_engine->ping(10);
  //     }
  // );

  fut0.get();
  // fut1.get();

  // auto fut = std::async([&]{
  //   seastar::alien::submit_to(app_engine->app_->alien(), 0, [&app_engine]{
  //     return app_engine->nothing();
  //     }
  //   );
  // });
  // auto fut = seastar::alien::submit_to(0, [&app_engine]{return
  // app_engine->nothing();}); auto fut = app_engine->submit_to_test();

  auto signal =
    seastar::alien::submit_to(app_engine->app_->alien(), 0, [&app_engine] {
      app_engine->finished.signal();
      return seastar::make_ready_future<>();
    });
  signal.get();
  app_engine->thread_.join();
}