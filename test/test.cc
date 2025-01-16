#include <iostream>
#include <list>
#include <memory>
#include <stdlib.h>

struct client_params {
  uint32_t redis_instance_type = 0;
};

template <typename Client>
class Connection {
 private:
  template <typename U>
  static auto TCheckIns(client_params)
    -> std::is_same<decltype(std::declval<U>().client_params),
                    client_params>::type;

  template <typename U>
  static std::false_type TCheckIns(...);

 public:
  Connection(std::shared_ptr<Client> client){};
  ~Connection(){};

  std::weak_ptr<Client> client_;

  enum {
    TCheckInsRes = std::is_same<decltype(TCheckIns<Client>(client_params())),
                                std::true_type>::value
  };

  int testfunc() {
    static_assert(
      TCheckInsRes == true,
      "Warning: When construct Connection class, there must be a member "
      "client_params struct named client_params in typename Client. For "
      "example, you could try \"client_params client_params;\"");
    return TCheckInsRes;
  }
};

int main(int argc, char *argv[]) {
  class Client {
   private:
   public:
    client_params client_params;
    // uint32_t& redis_instance_type = client_params.redis_instance_type;
    Client(/* args */){};
    ~Client(){};
  };

  std::shared_ptr client = std::make_shared<Client>();
  auto connection = std::make_shared<Connection<Client>>(client);
  std::cout << connection->testfunc() << std::endl;
  // client->client_params.redis_instance_type = 1;
  // std::cout << client->redis_instance_type << std::endl;

  char abc[3] = {'a', 'b', 'c'};
  std::list<std::unique_ptr<const char>> reply_list;
  for (size_t i = 0; i < 3; i++) {
    char *tmp_reply = new char;
    reply_list.emplace_back(std::unique_ptr<const char>(tmp_reply));
  }

  return 0;
}