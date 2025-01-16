/*
 * Copyright (c) 2022, Jia He <mofhejia@163.com>
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <chrono>
#include <deque>
#include <map>
#include <seastar/core/alien.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>

#include "connection.hh"
#include "crc16.hpp"
#include "shards.hh"

namespace AsterHiredis {

using ConnectionSPtr = seastar::shared_ptr<ConnectionUnit::Connection>;
using RedisReplyUPtr = std::unique_ptr<redisReply, ReplyDeleter>;
using RedisReplyVectorSPtr = ConnectionUnit::RedisReplyVectorSPtr;
using RedisReplyVectorHandler = ConnectionUnit::RedisReplyVectorHandler;
using InputStreamConsumerFunctor =
  ConnectionUnit::InputStreamConsumer::InputStreamConsumerFunctor;
using InputStreamConsumerFunction =
  ConnectionUnit::InputStreamConsumer::InputStreamConsumerFunction;

enum class RedisClientType { CLUSTER = 0, STANDALONE = 1, SENTINEL = 2 };

typedef struct ClientParams {
  RedisClientType redis_instance_type = RedisClientType::STANDALONE;
  RedisRole redis_role =
    RedisRole::MASTER;  // take no effect at STANDALONE mode
  std::variant<Node, std::vector<Node>> hosts_and_ports;
  std::string redis_master_name = "master";
  std::string redis_user = "default";
  std::string redis_password = "";
  int redis_db = 0;
  // database nodes connection options
  std::chrono::milliseconds redis_connect_timeout =
    std::chrono::milliseconds(2000);  // milliseconds
  std::chrono::milliseconds redis_wait_timeout =
    std::chrono::milliseconds(2000);  // milliseconds
  std::chrono::milliseconds redis_socket_timeout =
    std::chrono::milliseconds(1000);  // milliseconds
  // sentinel node connection options
  std::string redis_sentinel_user = "default";
  std::string redis_sentinel_password = "";
  std::chrono::milliseconds redis_sentinel_connect_timeout =
    std::chrono::milliseconds(1000);  // milliseconds
  std::chrono::milliseconds redis_sentinel_socket_timeout =
    std::chrono::milliseconds(1000);  // milliseconds
  // tcp_keep_alive only available when using the Unix network stack
  bool tcp_keep_alive = false;
  uint32_t tcp_keep_intvl = 0;
  // auto reconnect when errors happened in network stack
  bool auto_reconnect = true;
} ClientParams;

typedef struct HostsAndPortFrontVisitor {
  Node operator()(const Node &val) { return val; }
  Node operator()(const std::vector<Node> &val) { return val.front(); }
  Node operator()(const std::deque<Node> &val) { return val.front(); }
} HostsAndPortFrontVisitor;

typedef struct HostsAndPortVisitor {
  Node operator()(const Node &val) { return val; }
  std::vector<Node> operator()(const std::vector<Node> &val) { return val; }
  std::deque<Node> operator()(const std::deque<Node> &val) { return val; }
} HostsAndPortVisitor;

class ClientChannel;
class ClientPipeLineChannel;

class RedisClientBase {
 protected:
  alien::instance *seastar_instance_;

  const ClientParams client_params_;

  const size_t max_ops_capacity_;
  seastar::basic_semaphore<ConnectionUnit::ConnectionSemaphoreExceptionFactory>
    connection_capacity_{0};

  std::exception_ptr connection_update_ex_ = nullptr;
  std::exception_ptr connection_send_ex_ = nullptr;
  std::exception_ptr connection_recv_ex_ = nullptr;
  std::exception_ptr connection_cosum_cb_ex_ = nullptr;

  std::atomic<bool> update_connection_working_{false};
  std::atomic<bool> update_connection_success_{false};
  int update_connection_random_wait_coefficient = 0;

  size_t channel_id_gennerator_ = 0;  // Do we need atomics here?
  std::variant<seastar::weak_ptr<RedisClientBase>,
               std::weak_ptr<RedisClientBase>>
    client_ptr_for_channel_;

 protected:
  RedisClientBase(const ClientParams &client_params,
                  const size_t max_ops_capacity,
                  const alien::instance *seastar_instance)
    : client_params_(client_params), max_ops_capacity_(max_ops_capacity),
      seastar_instance_(const_cast<alien::instance *>(seastar_instance)){};
  ~RedisClientBase(){};

#define ReturnConnectionEX(name)                                               \
  std::exception_ptr connection_##name##_ex() const {                          \
    return connection_##name##_ex_;                                            \
  }
  ReturnConnectionEX(update);
  ReturnConnectionEX(send);
  ReturnConnectionEX(recv);
  ReturnConnectionEX(cosum_cb);
#undef ReturnConnectionEX

 protected:
  future<ConnectionSPtr> CreateConnection(const std::string &host,
                                          const uint16_t port,
                                          const ClientParams &client_params) {
    return ConnectionUnit::CreateSocketFD(host, port,
                                          client_params.redis_connect_timeout)
      .then([this, client_params = client_params](connected_socket &&fd) {
        return seastar::make_shared<ConnectionUnit::Connection>(
          std::move(fd), client_params.redis_wait_timeout,
          client_params.redis_socket_timeout, client_params.tcp_keep_alive,
          client_params.tcp_keep_intvl);
      })
      .handle_exception([](std::exception_ptr ep) {
        std::string err = "[AsterHiredis] Creating Connection Error -- ";
        return ConnectionUnit::ReturnConnectionException<ConnectionSPtr>(err,
                                                                         ep);
      });
  }

  virtual void RemoveClientChannel(uint64_t channel_id) = 0;

 private:
  const std::string &no_impl_err() {
    static std::string no_impl_err_(
      "There is no corresponding implementation for this function");
    return no_impl_err_;
  }

 protected:
  virtual future<> Send(const char *source) {
    return ConnectionUnit::ReturnConnectionException(no_impl_err(),
                                                     std::current_exception());
  }
  virtual future<> Send(const Node &node, const char *source) {
    return ConnectionUnit::ReturnConnectionException(no_impl_err(),
                                                     std::current_exception());
  }
  virtual future<> Send(const std::string_view &guide_key, const char *source) {
    return ConnectionUnit::ReturnConnectionException(no_impl_err(),
                                                     std::current_exception());
  }

  virtual future<> Send(int argc, const char **argv, const size_t *argvlen) {
    return ConnectionUnit::ReturnConnectionException(no_impl_err(),
                                                     std::current_exception());
  }
  virtual future<> Send(const Node &node, int argc, const char **argv,
                        const size_t *argvlen) {
    return ConnectionUnit::ReturnConnectionException(no_impl_err(),
                                                     std::current_exception());
  }
  virtual future<> Send(const std::string_view &guide_key, int argc,
                        const char **argv, const size_t *argvlen) {
    return ConnectionUnit::ReturnConnectionException(no_impl_err(),
                                                     std::current_exception());
  }

  virtual future<> WritePipeLine(const char *source) {
    return ConnectionUnit::ReturnConnectionException(no_impl_err(),
                                                     std::current_exception());
  }
  virtual future<> WritePipeLine(const Node &node, const char *source) {
    return ConnectionUnit::ReturnConnectionException(no_impl_err(),
                                                     std::current_exception());
  }
  virtual future<> WritePipeLine(const std::string_view &guide_key,
                                 const char *source) {
    return ConnectionUnit::ReturnConnectionException(no_impl_err(),
                                                     std::current_exception());
  }

  virtual future<> WritePipeLine(int argc, const char **argv,
                                 const size_t *argvlen) {
    return ConnectionUnit::ReturnConnectionException(no_impl_err(),
                                                     std::current_exception());
  }
  virtual future<> WritePipeLine(const Node &node, int argc, const char **argv,
                                 const size_t *argvlen) {
    return ConnectionUnit::ReturnConnectionException(no_impl_err(),
                                                     std::current_exception());
  }
  virtual future<> WritePipeLine(const std::string_view &guide_key, int argc,
                                 const char **argv, const size_t *argvlen) {
    return ConnectionUnit::ReturnConnectionException(no_impl_err(),
                                                     std::current_exception());
  }

  virtual future<RedisReplyPromise> SendPipeLine() {
    return ConnectionUnit::ReturnConnectionException(no_impl_err(),
                                                     std::current_exception());
  }
  virtual future<RedisReplyPromise> SendPipeLine(const Node &node) {
    return ConnectionUnit::ReturnConnectionException(no_impl_err(),
                                                     std::current_exception());
  }
  virtual future<RedisReplyPromise>
  SendPipeLine(const std::string_view &guide_key) {
    return ConnectionUnit::ReturnConnectionException(no_impl_err(),
                                                     std::current_exception());
  }

  virtual future<>
  ConsumeWithCallback(const InputStreamConsumerFunction input_consumer) {
    return ConnectionUnit::ReturnConnectionException<>(
      no_impl_err(), std::current_exception());
  }
  virtual future<>
  ConsumeWithCallback(const Node &node,
                      const InputStreamConsumerFunction input_consumer) {
    return ConnectionUnit::ReturnConnectionException<>(
      no_impl_err(), std::current_exception());
  }
  virtual future<>
  ConsumeWithCallback(const std::string_view &guide_key,
                      const InputStreamConsumerFunction input_consumer) {
    return ConnectionUnit::ReturnConnectionException<>(
      no_impl_err(), std::current_exception());
  }

 public:
  size_t connection_capacity() const {
    return connection_capacity_.available_units();
  }

  virtual std::shared_ptr<ClientChannel> GetClientChannel() = 0;

  virtual seastar::shared_ptr<ClientChannel> GetSeastarClientChannel() = 0;

  virtual std::shared_ptr<ClientPipeLineChannel> GetClientPipeLineChannel() = 0;

  virtual seastar::shared_ptr<ClientPipeLineChannel> GetSeastarPipeLineChannel() = 0;

  friend class ClientChannel;
  friend class ClientPipeLineChannel;
};

template <RedisClientType RCT, typename = void>
class RedisClient : public RedisClientBase {};

class ClientChannel {
  std::variant<seastar::weak_ptr<RedisClientBase>,
               std::weak_ptr<RedisClientBase>>
    client_ptr_;
  uint64_t channel_id_;
  seastar::lw_shared_ptr<seastar::queue<RedisReplyUPtr>> reply_queue_ptr_;

 public:
  ClientChannel(std::variant<seastar::weak_ptr<RedisClientBase>,
                             std::weak_ptr<RedisClientBase>>
                  client_ptr,
                uint64_t channel_id, size_t cqueue_size) {
    client_ptr_ = client_ptr;
    channel_id_ = channel_id;
    reply_queue_ptr_ =
      seastar::make_lw_shared<seastar::queue<RedisReplyUPtr>>(cqueue_size);
  }
  ~ClientChannel() {
    std::get<client_ptr_.index()>(client_ptr_)
      ->RemoveClientChannel(channel_id_);
  };

 public:

};

class ClientPipeLineChannel {
  std::variant<seastar::weak_ptr<RedisClientBase>,
               std::weak_ptr<RedisClientBase>>
    client_ptr_;
  uint64_t channel_id_;
  seastar::lw_shared_ptr<seastar::queue<RedisReplyUPtr>> reply_queue_ptr_;

 public:
  ClientPipeLineChannel(std::variant<seastar::weak_ptr<RedisClientBase>,
                             std::weak_ptr<RedisClientBase>>
                  client_ptr,
                uint64_t channel_id, size_t cqueue_size) {
    client_ptr_ = client_ptr;
    channel_id_ = channel_id;
    reply_queue_ptr_ =
      seastar::make_lw_shared<seastar::queue<RedisReplyUPtr>>(cqueue_size);
  }
  ~ClientPipeLineChannel() {
    std::get<client_ptr_.index()>(client_ptr_)
      ->RemoveClientChannel(channel_id_);
  };

 public:

};

}  // namespace AsterHiredis
