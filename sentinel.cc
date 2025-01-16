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

#include "sentinel.hh"

namespace AsterHiredis {

template <>
RedisClient<RedisClientType::SENTINEL>::RedisClient(
  const ClientParams &client_params, const size_t max_ops_capacity,
  const alien::instance *seastar_instance)
  : RedisClientBase(client_params, max_ops_capacity, seastar_instance) {

  if (client_params.redis_instance_type != RedisClientType::SENTINEL) {
    std::string type_err =
      "[AsterHiredis] redis_instance_type in client_params should be "
      "RedisClientType::SENTINEL when construct SENTINEL RedisClient "
      "class";
    seastar_logger.error("{}", type_err);
    ConnectionUnit::throw_connection_exception(type_err);
  }

  try {
    const auto params_pair_index = this->client_params_.hosts_and_ports.index();
    if (params_pair_index == 0) {
      std::optional<Node> params_pair =
        std::get<Node>(this->client_params_.hosts_and_ports);
      main_connection_host_port_ = params_pair.value();
      this->available_hosts_ports_.emplace_back(params_pair.value());
    } else if (params_pair_index == 1) {
      std::optional<std::vector<Node>> params_pair =
        std::get<std::vector<Node>>(this->client_params_.hosts_and_ports);
      main_connection_host_port_ = params_pair.value().at(0);
      for (auto pair : params_pair.value()) {
        this->available_hosts_ports_.emplace_back(pair);
      }
    } else {
      throw std::runtime_error("Unknown Errors");
    }
  } catch (const std::exception &e) {
    seastar_logger.error(
      "{}. The type of var hosts_and_ports is neither "
      "std::vector<std::pair<std::string, uint16_t>> or std::pair<std::string, "
      "uint16_t> in client_params_ struct.",
      e);
    throw e;
  }

  connection_capacity_.signal(max_ops_capacity_);
  connection_capacity_.ensure_space_for_waiters(max_ops_capacity_ >> 1);
};

template <>
RedisClient<RedisClientType::SENTINEL>::~RedisClient(){};

template <>
future<RedisReplyVectorSPtr>
RedisClient<RedisClientType::SENTINEL>::CheckReplyFullError(
  RedisReplyVectorSPtr rRlp) {
  bool recv_has_error = false;
  bool recv_all_error = true;
  std::string err = "[AsterHiredis] Received error reply from Redis";
  for (size_t i = 0; i < rRlp->size(); ++i) {
    if (rRlp->at(i)->type == REDIS_REPLY_ERROR) {
      recv_has_error = true;
      err += fmt::format(" -- At reply {}:", i);
      err.append(rRlp->at(i)->str, rRlp->at(i)->len);
    } else {
      recv_all_error = false;
    }
  }
  if (recv_all_error == true) {
    return make_exception_future<RedisReplyVectorSPtr>(
      std::make_exception_ptr(ConnectionUnit::ConnectionException(
        err, std::make_any<RedisReplyVectorSPtr>(rRlp), false)));
  } else {
    if (recv_has_error) seastar_logger.error("{}", err);
    return make_ready_future<RedisReplyVectorSPtr>(std::move(rRlp));
  }
}

template <>
future<>
RedisClient<RedisClientType::SENTINEL>::CommonActionAfterConnectSentinel(
  ConnectionSPtr conn) {
  return conn
    ->Authorization(this->client_params_.redis_sentinel_user,
                    this->client_params_.redis_sentinel_password)
    .then([this, conn] { return conn->PingPongTest(std::nullopt); });
}

template <>
future<>
RedisClient<RedisClientType::SENTINEL>::CommonActionAfterConnectStandalone(
  ConnectionSPtr conn) {
  return conn
    ->Authorization(this->client_params_.redis_user,
                    this->client_params_.redis_password)
    .then([this, conn] { return conn->PingPongTest(std::nullopt); })
    .then(
      [this, conn] { return conn->SelectDB(this->client_params_.redis_db); });
}

template <>
future<> RedisClient<RedisClientType::SENTINEL>::RepeatConnectAvailableList() {
  return repeat([this] {
    if (this->available_hosts_ports_.empty()) {
      return make_ready_future<stop_iteration>(stop_iteration::yes);
    }
    const auto &pair = this->available_hosts_ports_.front();
    this->available_hosts_ports_.pop_front();
    const auto &host = pair.first;
    const auto &port = pair.second;
    return CreateConnection(host, port, this->client_params_)
      .then([this, &pair](ConnectionSPtr conn) {
        return CommonActionAfterConnectSentinel(conn).then(
          [this, &pair, conn = std::move(conn)]() mutable {
            bool expect_val = false;
            if (update_connection_success_.compare_exchange_strong(expect_val,
                                                                   true)) {
              this->sentinel_connection_ = std::move(conn);
              this->main_connection_host_port_ = pair;
              this->available_hosts_ports_.emplace_back(pair);
            }
            return make_ready_future<stop_iteration>(stop_iteration::yes);
          });
      })
      .handle_exception([this](auto ep) {
        connection_update_ex_ = ep;
        return stop_iteration::no;
      });
  });
}

template <>
future<>
RedisClient<RedisClientType::SENTINEL>::ConnectHPPair_impl(const Node &pair) {
  const auto pair_ = pair;
  return do_with(
           std::move(pair_),
           [this](auto &pair_) {
             const auto &host = pair_.first;
             const auto &port = pair_.second;
             return CreateConnection(host, port, this->client_params_)
               .then([this, &pair_](ConnectionSPtr conn) {
                 return CommonActionAfterConnectSentinel(conn).then(
                   [this, &pair_, conn = std::move(conn)]() mutable {
                     bool expect_val = false;
                     if (update_connection_success_.compare_exchange_strong(
                           expect_val, true)) {
                       this->sentinel_connection_ = std::move(conn);
                       this->main_connection_host_port_ = pair_;
                     }
                     return make_ready_future();
                   });
               });
           })
    .handle_exception([this](auto ep) {
      connection_update_ex_ = ep;
      return make_exception_future(ep);
    });
}

template <>
future<> RedisClient<RedisClientType::SENTINEL>::RepeatConnectOriginalList() {
  const auto params_pair_index = this->client_params_.hosts_and_ports.index();
  if (params_pair_index == 0) {
    std::optional<Node> params_pair =
      std::get<Node>(this->client_params_.hosts_and_ports);
    return ConnectHPPair_impl(params_pair.value());
  } else {
    std::optional<std::vector<Node>> params_pair =
      std::get<std::vector<Node>>(this->client_params_.hosts_and_ports);
    return do_for_each(params_pair.value(), [this](const Node &pair) {
      if (IsUpdateFail()) {
        return ConnectHPPair_impl(pair).handle_exception(
          [this](auto ep) { return make_ready_future(); });
      } else {
        return make_ready_future();
      }
    });
  }
}

template <>
future<> RedisClient<RedisClientType::SENTINEL>::CreateSentinelConnection(
  const Node &pair) {
  return ConnectHPPair_impl(pair)
    .finally([this] {
      if (IsUpdateFail()) {
        ConnectionUnit::PrintException(connection_update_ex_,
                                       "CreateSentinelConnection first stage");
        return RepeatConnectAvailableList().finally([this] {
          if (IsUpdateFail()) {
            ConnectionUnit::PrintException(
              connection_update_ex_, "CreateSentinelConnection second stage");
            return RepeatConnectOriginalList();
          } else {
            return make_ready_future();
          }
        });
      } else {
        return make_ready_future();
      }
    })
    .finally([this] {
      if (IsUpdateFail()) {
        std::string err =
          "[AsterHiredis] Fail to create or update Redis "
          "server connection in CreateSentinelConnection third stage -- ";
        return ConnectionUnit::ReturnConnectionException<>(
          std::move(err), connection_update_ex_, seastar_logger);
      } else {
        return make_ready_future();
      }
    });
}

template <>
future<ConnectionSPtr>
RedisClient<RedisClientType::SENTINEL>::CreateConnectionAndTest(
  const std::string &host, const uint16_t port) {
  return CreateConnection(host, port, this->client_params_)
    .then([this](ConnectionSPtr conn) {
      return CommonActionAfterConnectStandalone(conn).then(
        [this, conn = std::move(conn)] {
          return make_ready_future<ConnectionSPtr>(std::move(conn));
        });
    });
}

template <>
future<> RedisClient<RedisClientType::SENTINEL>::CreateStandaloneConnection(
  const Node &node) {
  auto &&host = node.first;
  auto &&port = node.second;
  return CreateConnectionAndTest(host, port)
    .then([this, &node = node](ConnectionSPtr conn) {
      return ConnectionUnit::GetRoleMachingNode(conn, node,
                                                this->client_params_.redis_role)
        .then(
          [this, conn = std::move(conn)](std::optional<Node> &&hp_pair_opt) {
            if (hp_pair_opt.has_value()) {
              auto &&hp_pair = hp_pair_opt.value();
              return CreateConnectionAndTest(hp_pair.first, hp_pair.second)
                .then([this](ConnectionSPtr conn) {
                  this->standalone_connection_ = std::move(conn);
                  return make_ready_future<>();
                });
            } else {
              this->standalone_connection_ = std::move(conn);
              return make_ready_future<>();
            }
          });
    })
    .handle_exception([this, &node = node](auto ep) {
      std::string err =
        "[AsterHiredis] Fail to create standalone node connection to host:" +
        node.first + " port:" + std::to_string(node.second) + " -- ";
      return ConnectionUnit::ReturnConnectionException<>(err, ep,
                                                         seastar_logger);
    });
}

template <>
future<> RedisClient<RedisClientType::SENTINEL>::SetAliveSentinelConnection(
  const Node &node) {
  if (this->sentinel_connection_) {
    return this->sentinel_connection_->PingPongTest(std::nullopt)
      .handle_exception([this, &node = node](auto ep) {
        return CreateSentinelConnection(node);
      });
  } else {
    return CreateSentinelConnection(node);
  }
}

template <>
future<Node> RedisClient<
  RedisClientType::SENTINEL>::GetMasterAddrFromSentinelConnectionByName() {
  assert(this->sentinel_connection_);
  auto &&name = this->client_params_.redis_master_name;
  return this->sentinel_connection_
    ->Send("SENTINEL GET-MASTER-ADDR-BY-NAME %b", name.data(), name.size())
    .then([this] { return this->sentinel_connection_->Recv(); })
    .then([this](const RedisReplyVectorSPtr reply_vev_sptr) {
      auto reply = std::move(reply_vev_sptr->at(0));
      std::optional<std::pair<std::string, std::string>> master;
      try {
        master = reply::parse<std::pair<std::string, std::string>>(reply.get());
      } catch (const std::exception &e) {
        std::string err = "[AsterHiredis] Fail to "
                          "GetMasterAddrFromSentinelConnectionByName -- ";
        err.append(e.what());
        return make_exception_future<Node>(
          std::make_exception_ptr(ConnectionUnit::ConnectionException(err)));
      }
      if (!master.has_value()) {
        std::string err =
          "[AsterHiredis] Fail to "
          "GetMasterAddrFromSentinelConnectionByName -- Unknown Errors";
        return make_exception_future<Node>(
          std::make_exception_ptr(ConnectionUnit::ConnectionException(err)));
      }
      uint16_t port = 0;
      try {
        port = static_cast<uint16_t>(std::stoi(master.value().second));
      } catch (const std::exception &e) {
        std::string err =
          "[AsterHiredis] Fail to GetMasterAddrFromSentinelConnectionByName -- "
          "Master port is invalid: " +
          master.value().second;
        err.append(e.what());
        return make_exception_future<Node>(
          std::make_exception_ptr(ConnectionUnit::ConnectionException(err)));
      }

      return make_ready_future<Node>(
        std::move(Node{master.value().first, port}));
    });
};

template <>
future<> RedisClient<RedisClientType::SENTINEL>::UpdateConnection(
  std::optional<const std::string> host, std::optional<const uint16_t> port) {
  update_connection_working_.store(true);
  update_connection_success_.store(false);
  connection_update_ex_ = nullptr;

  const auto &conn_host = this->main_connection_host_port_.first;
  const auto &conn_port = this->main_connection_host_port_.second;
  const auto pair = Node(host.value_or(conn_host), port.value_or(conn_port));

  std::cout << "[AsterHiredis] Start UpdateConnection to Host " << pair.first
            << " Port " << pair.second << std::endl;
  return SetAliveSentinelConnection(pair)
    .then([this] { return GetMasterAddrFromSentinelConnectionByName(); })
    .then([this](const auto &node) {
      return do_with(std::move(node), [this](const auto &node) {
        return CreateStandaloneConnection(node);
      });
    })
    .handle_exception([this](auto ep) {
      std::string err = "[AsterHiredis] Fail to create or update Redis server "
                        "connection in UpdateConnection -- ";
      return ConnectionUnit::ReturnConnectionException<>(err, ep,
                                                         seastar_logger);
    })
    .finally([this] { update_connection_working_.store(false); });
}

}  // namespace AsterHiredis