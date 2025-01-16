#include <iostream>
#include <memory>

#include <stdio.h>

#include <seastar/core/deleter.hh>
#include <seastar/core/seastar.hh>

// template <typename... T>
// int TestSnprintf_impl(T... args) {
//   char buffer[50];
//   // int j = snprintf(buffer, 50, "%hh %s %l %s\n", args...);
//   int j = snprintf(buffer, 50, "%hd", args...);
//   std::cout << buffer << std::endl;
//   return j;
// }

// template< typename... Ts >
// struct vct
// { ... };

// template< typename... Ts >
// struct make_vct< std::tuple< Ts... > >
// {
//     typedef vct< Ts... > type;
// };

class redisWriter {
 public:
  char *target = NULL;
  int len = 0;
  seastar::temporary_buffer<char> *tmp_buf = NULL;
  seastar::deleter *tmp_buf_del = NULL;
  redisWriter() {
    tmp_buf_del = new seastar::deleter();
    *tmp_buf_del = seastar::make_free_deleter(target);
  };
  ~redisWriter() {
    if (tmp_buf) {delete tmp_buf;}
    if (tmp_buf_del) { delete tmp_buf_del; }
  };
};

int main(int argc, char *argv[]) {
  // auto i = TestSnprintf_impl(1, "2", 3, "4");
  // short tmp = 1;
  // auto i = TestSnprintf_impl(tmp);
  char *src = "hello!";
  char *str = (char *)malloc(6);
  memcpy(str, src, 6);
  redisWriter w = redisWriter();
  w.target = str;
  w.len = 6;
  w.tmp_buf = new seastar::temporary_buffer<char>(
      w.target, w.len, std::move(*(w.tmp_buf_del)));
  
  return 0;
}