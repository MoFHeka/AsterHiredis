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

#include <seastar/core/distributed.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/defer.hh>

using namespace seastar;

class coroutine_test {
 private:
  std::vector<int> sleep_res;
  seastar::semaphore sleep_finish_{0};

 public:
  coroutine_test(/* args */);
  ~coroutine_test();

  seastar::future<int> sleep(const int timeout_ms) {
    return seastar::sleep(std::chrono::seconds(timeout_ms))
      .then([timeout_ms = std::move(timeout_ms)] { return timeout_ms; });
  }

  seastar::future<> my_timer(int &i_in, int &sleep_res_) {
    return seastar::async([this, i_in = std::move(i_in), &sleep_res_] {
      std::cout << "running on " << seastar::this_shard_id() << ": " << i_in
                << std::endl;
      auto tmp_res = sleep(5 - i_in).get();
      std::cout << "running on " << seastar::this_shard_id()
                << ": I should show in " << 5 - i_in << std::endl;
      sleep_res_ = tmp_res;
      // this->sleep_res[i_in] = tmp_res;
      // return tmp_res;
    });

    // seastar::thread th([&, this] {
    //   std::cout << "running on " << seastar::this_shard_id() << ": " <<
    //   timeout_ms << std::endl; auto tmp_res = sleep(5-timeout_ms).get();
    //   std::cout << "running on " << seastar::this_shard_id() << ": I sleep "
    //   << timeout_ms << std::endl; this->sleep_res.push_back(tmp_res);
    // });
    // return do_with(std::move(th), [] (auto& th) {
    //     return th.join();
    // });
    // std::cout << "running on " << seastar::this_shard_id() << ": " << i_in <<
    // std::endl; return sleep(5-i_in).then([&sleep_res_](int i_in){
    //   std::cout << "running on " << seastar::this_shard_id() << ": I should
    //   show in " << 5-i_in << std::endl; sleep_res_ = 5-i_in; return
    //   make_ready_future<>();
    // });
  }

  seastar::future<> worker() {
    sleep_res.resize(5);
    std::vector<seastar::future<>> res;

    (void)seastar::async([&, this] {
      seastar::parallel_for_each(std::views::iota(0, 5), [&, this](int i_in) {
        return my_timer(i_in, this->sleep_res[i_in]);
      }).get();
      for (int i = 0; i < 5; ++i) {
        throw std::exception();
        std::cout << "running on " << seastar::this_shard_id() << ": I'm " << i
                  << " sleep " << this->sleep_res[i] << std::endl;
      }
      sleep_finish_.signal();
    });
    return sleep_finish_.wait();
  }

  // seastar::future<> worker() {
  //   return seastar::do_for_each(boost::counting_iterator<int>(1),
  //       boost::counting_iterator<int>(5), [] (int i) {
  //       return my_timer(i);
  //   });
  // }
};

coroutine_test::coroutine_test(/* args */) {}

coroutine_test::~coroutine_test() {}

int main(int argc, char **argv) {
  auto app_ = std::make_unique<app_template>();
  // auto coroutine_tests = new distributed<coroutine_test>;

  // app_->run(argc, argv, [&coroutine_tests] {
  //   return coroutine_tests->start().then(
  //     [&] { return coroutine_tests->invoke_on_all(&coroutine_test::worker); });
  // });

  auto coroutine_tests = new coroutine_test();
  app_->run(argc, argv, [&coroutine_tests] {
      return coroutine_tests->worker();
  });
}