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

#include <atomic>
#include <chrono>
#include <deque>
#include <memory>

#include <seastar/core/alien.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>

#include "alloc.hh"
#include "client_util.hpp"
#include "reader.hh"
#include "writer.hh"

namespace AsterHiredis {

template <RedisClientType RCT>
class RedisClient<RCT, std::enable_if_t<(RCT == RedisClientType::SENTINEL)>>
  : public seastar::enable_shared_from_this<
      RedisClient<RCT, std::enable_if_t<(RCT == RedisClientType::SENTINEL)>>>,
    public RedisClientBase {

 protected:
  ConnectionSPtr sentinel_connection_;
  Node main_connection_host_port_;
  ConnectionSPtr standalone_connection_;
  std::deque<Node> available_hosts_ports_;

 protected:
  future<> CommonActionAfterConnectSentinel(ConnectionSPtr conn);

  future<> CommonActionAfterConnectStandalone(ConnectionSPtr connection);

  future<> RepeatConnectAvailableList();

  future<> ConnectHPPair_impl(const Node &pair);

  future<> RepeatConnectOriginalList();

 private:
  RedisClient(const ClientParams &client_params,
              const size_t max_ops_capacity = 1 << 7,
              const alien::instance *seastar_instance =
                alien::internal::default_instance);

 public:
#define MakeSharedEnablerClass                                                 \
  class make_shared_enabler : public RedisClient<RedisClientType::SENTINEL> {  \
   public:                                                                     \
    make_shared_enabler(Args &&... args)                                       \
      : RedisClient<RedisClientType::SENTINEL>(std::forward<Args>(args)...) {} \
  };

  template <typename T>
  static future<> GetClientImplFunc(T instance_ptr) {
    return instance_ptr->UpdateConnection();
  }

  template <typename FUC, typename INS>
  static future<INS> GetClientRetFunc(FUC f_uc, INS instance_ptr) {
    return seastar::with_timeout<
             ConnectionUnit::ConnectionTimeoutExceptionFactory>(
             ConnectionUnit::TimeoutEndMilliSec(
               instance_ptr->client_params_.redis_connect_timeout),
             std::move(f_uc))
      .then([instance_ptr = std::move(instance_ptr)] { return instance_ptr; })
      .handle_exception([](auto ep) {
        std::string err = "[AsterHiredis] Fail to construct Redis server "
                          "connection class -- ";
        return ConnectionUnit::ReturnConnectionException<INS>(err, ep,
                                                              seastar_logger);
      });
  }

  template <typename... Args>
  static future<std::shared_ptr<RedisClient<RedisClientType::SENTINEL>>>
  GetRedisClient(Args &&... args) {
    MakeSharedEnablerClass;
    std::shared_ptr<RedisClient<RedisClientType::SENTINEL>> instance_ptr =
      std::make_shared<make_shared_enabler>(std::forward<Args>(args)...);
    auto f_uc = do_with(instance_ptr, [](auto &instance_ptr) {
      return GetClientImplFunc<decltype(instance_ptr)>(instance_ptr);
    });
    return GetClientRetFunc<decltype(f_uc), decltype(instance_ptr)>(
      std::move(f_uc), instance_ptr);
  };

  template <typename... Args>
  static future<seastar::shared_ptr<RedisClient<RedisClientType::SENTINEL>>>
  GetSeastarRedisClient(Args &&... args) {
    MakeSharedEnablerClass;
    seastar::shared_ptr<RedisClient<RedisClientType::SENTINEL>> instance_ptr =
      seastar::make_shared<make_shared_enabler>(std::forward<Args>(args)...);
    auto f_uc = do_with(instance_ptr, [](auto &instance_ptr) {
      return GetClientImplFunc<decltype(instance_ptr)>(instance_ptr);
    });
    return GetClientRetFunc<decltype(f_uc), decltype(instance_ptr)>(
      std::move(f_uc), instance_ptr);
  };
#undef MakeSharedEnablerClass

  ~RedisClient(){};

 private:
  template <typename R = void>
  future<R> HandleCommandException(std::exception_ptr ep) {
    return do_with(std::move(ep), [this](auto &ep) {
      bool network_stack_error = false;
      try {
        if (ep) { std::rethrow_exception(ep); }
      } catch (const ConnectionUnit::ConnectionException &e) {
        network_stack_error = e.network_stack_error();
      } catch (const std::exception &e) { network_stack_error = false; }
      bool expect_val = false;
      if (network_stack_error &&
          update_connection_working_.compare_exchange_strong(expect_val,
                                                             true)) {
        this->update_connection_success_.store(false);
        seastar_logger.info(
          "[AsterHiredis] Start to update connection to Redis");
        (void)smp::submit_to(this_shard_id(), [this] {
          return seastar::sleep(client_params_.redis_wait_timeout *
                                update_connection_random_wait_coefficient)
            .then([this] {
              if (!standalone_connection_->connection_gate_.is_closed()) {
                return standalone_connection_->connection_gate_.close();
              }
              return make_ready_future();
            });
        });
        (void)smp::submit_to(this_shard_id(), [this] {
          return seastar::sleep(client_params_.redis_wait_timeout *
                                update_connection_random_wait_coefficient)
            .finally([this] {
              if (IsUpdateFail() && client_params_.auto_reconnect) {
                return UpdateConnection();
              } else {
                update_connection_working_.store(false);
                return make_ready_future();
              }
            });
        });
        update_connection_random_wait_coefficient = rand() % 6;
      }
      std::string err = "";
      return ConnectionUnit::ReturnConnectionException<R>(std::move(err), ep);
    });
  }

 public:
  future<RedisReplyVectorSPtr> CheckReplyFullError(RedisReplyVectorSPtr rRlp);

  virtual future<> Send(const char *source) override {
    return this->connection_capacity_
      .wait(
        ConnectionUnit::TimeoutEndMilliSec(client_params_.redis_wait_timeout),
        1)
      .then(
        [this, source] { return this->standalone_connection_->Send(source); })
      .handle_exception([this](auto ep) {
        connection_send_ex_ = ep;
        return HandleCommandException<>(ep);
      })
      .finally([this] { this->connection_capacity_.signal(); });
  }

  virtual future<> Send(int argc, const char **argv,
                        const size_t *argvlen) override {
    return this->connection_capacity_
      .wait(
        ConnectionUnit::TimeoutEndMilliSec(client_params_.redis_wait_timeout),
        1)
      .then([this, argc, argv, argvlen] {
        return this->standalone_connection_->Send(argc, argv, argvlen);
      })
      .handle_exception([this](auto ep) {
        connection_send_ex_ = ep;
        return HandleCommandException<>(ep);
      })
      .finally([this] { this->connection_capacity_.signal(); });
  }

  template <typename... Args>
  future<> Send(const Args &... args) {
    return this->connection_capacity_
      .wait(
        ConnectionUnit::TimeoutEndMilliSec(client_params_.redis_wait_timeout),
        1)
      .then([this, args = std::make_tuple(
                     std::forward<const Args &>(args)...)]() mutable {
        return std::apply(  // std::apply compatible with C++17 to capture pack
          [this](auto &&... args) {
            return this->standalone_connection_->Send(args...);
          },
          std::move(args));
      })
      .handle_exception([this](auto ep) {
        connection_send_ex_ = ep;
        return HandleCommandException<>(ep);
      })
      .finally([this] { this->connection_capacity_.signal(); });
  }

  template <typename... Args>
  future<> WritePipeLine(const Args &... args) {
    return this->connection_capacity_
      .wait(
        ConnectionUnit::TimeoutEndMilliSec(client_params_.redis_wait_timeout),
        1)
      .then([this, args = std::make_tuple(
                     std::forward<const Args &>(args)...)]() mutable {
        return std::apply(  // std::apply compatible with C++17 to capture pack
          [this](auto &&... args) {
            return this->standalone_connection_->WritePipeLine(args...);
          },
          std::move(args));
      })
      .handle_exception([this](auto ep) {
        connection_send_ex_ = ep;
        return HandleCommandException<>(ep);
      })
      .finally([this] { this->connection_capacity_.signal(); });
  }

  virtual future<> SendPipeLine() override {
    return this->connection_capacity_
      .wait(
        ConnectionUnit::TimeoutEndMilliSec(client_params_.redis_wait_timeout),
        1)
      .then([this] { return this->standalone_connection_->SendPipeLine(); })
      .handle_exception([this](auto ep) {
        connection_send_ex_ = ep;
        return HandleCommandException<>(ep);
      })
      .finally([this] { this->connection_capacity_.signal(); });
  }

  virtual future<RedisReplyVectorSPtr> Recv() override {
    return this->connection_capacity_
      .wait(
        ConnectionUnit::TimeoutEndMilliSec(client_params_.redis_wait_timeout),
        1)
      .then([this] { return this->standalone_connection_->Recv(); })
      .then([this](auto rRlp) { return CheckReplyFullError(rRlp); })
      .handle_exception([this](auto ep) {
        connection_recv_ex_ = ep;
        return HandleCommandException<RedisReplyVectorSPtr>(ep);
      })
      .finally([this] { this->connection_capacity_.signal(); });
  }

#define ConsumeWithCallbackImpl                                                \
  return do_with(std::move(input_consumer), [this](auto &input_consumer) {     \
    return this->connection_capacity_.wait(1)                                  \
      .then([this, &input_consumer] {                                          \
        return this->standalone_connection_->ConsumeWithCallback(              \
          input_consumer);                                                     \
      })                                                                       \
      .handle_exception([this](auto ep) {                                      \
        connection_cosum_cb_ex_ = ep;                                          \
        return HandleCommandException<>(ep);                                   \
      })                                                                       \
      .finally([this] { this->connection_capacity_.signal(); });               \
  });

  // Keeping handle input stream by ConsumeCallback util reaching eof.
  template <class S>
  future<> ConsumeWithCallback(const S input_consumer) {
    ConsumeWithCallbackImpl
  }
  virtual future<> ConsumeWithCallback(
    const InputStreamConsumerFunction input_consumer) override {
    ConsumeWithCallbackImpl
  }
#undef ConsumeWithCallbackImpl

  template <typename T,
            std::enable_if_t<std::is_constructible<std::string, T>::value &&
                             !std::is_constructible<std::optional<std::string>,
                                                    T>::value> * = nullptr>
  future<> UpdateConnection(const T &&host,
                            std::optional<const uint16_t> port = std::nullopt) {
    return UpdateConnection(std::string(std::forward<T>(host)), port);
  }
  future<>
  UpdateConnection(std::optional<const std::string> host = std::nullopt,
                   std::optional<const uint16_t> port = std::nullopt);

 private:
  future<> CreateSentinelConnection(const Node &pair);
  future<> SetAliveSentinelConnection(const Node &node);
  future<Node> GetMasterAddrFromSentinelConnectionByName();
  future<ConnectionSPtr> CreateConnectionAndTest(const std::string &host,
                                                 const uint16_t port);
  future<> CreateStandaloneConnection(const Node &node);

  inline bool IsUpdateFail() {
    return (!this->sentinel_connection_) ||
           (this->update_connection_success_.load() == false);
  }

  future<std::optional<Node>> FetchRoleMachingNode() {
    return ConnectionUnit::GetRoleMachingNode(this->standalone_connection_,
                                              this->main_connection_host_port_,
                                              this->client_params_.redis_role);
  }
};

}  // namespace AsterHiredis