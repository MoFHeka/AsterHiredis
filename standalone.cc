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

#include "standalone.hh"

namespace AsterHiredis {

template <>
RedisClient<RedisClientType::STANDALONE>::RedisClient(
  const ClientParams &client_params, const size_t max_ops_capacity,
  const alien::instance *seastar_instance)
  : RedisClientBase(client_params, max_ops_capacity, seastar_instance) {

  if (client_params.redis_instance_type != RedisClientType::STANDALONE) {
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
RedisClient<RedisClientType::STANDALONE>::~RedisClient(){};

template <>
future<RedisReplyVectorSPtr>
RedisClient<RedisClientType::STANDALONE>::CheckReplyFullError(
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
future<> RedisClient<RedisClientType::STANDALONE>::CommonActionAfterConnect(
  ConnectionSPtr conn) {
  return conn
    ->Authorization(this->client_params_.redis_user,
                    this->client_params_.redis_password)
    .then([this, conn] { return conn->PingPongTest(std::nullopt); })
    .then(
      [this, conn] { return conn->SelectDB(this->client_params_.redis_db); });
}

template <>
future<>
RedisClient<RedisClientType::STANDALONE>::RepeatConnectAvailableList() {
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
        return CommonActionAfterConnect(conn).then(
          [this, &pair, conn = std::move(conn)]() mutable {
            bool expect_val = false;
            if (update_connection_success_.compare_exchange_strong(expect_val,
                                                                   true)) {
              this->standalone_connection_ = std::move(conn);
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
RedisClient<RedisClientType::STANDALONE>::ConnectHPPair_impl(const Node &pair) {
  const auto pair_ = pair;
  return do_with(
           std::move(pair_),
           [this](auto &pair_) {
             const auto &host = pair_.first;
             const auto &port = pair_.second;
             return CreateConnection(host, port, this->client_params_)
               .then([this, &pair_](ConnectionSPtr conn) {
                 return CommonActionAfterConnect(conn).then(
                   [this, &pair_, conn = std::move(conn)]() mutable {
                     bool expect_val = false;
                     if (update_connection_success_.compare_exchange_strong(
                           expect_val, true)) {
                       this->standalone_connection_ = std::move(conn);
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
future<> RedisClient<RedisClientType::STANDALONE>::RepeatConnectOriginalList() {
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
future<> RedisClient<RedisClientType::STANDALONE>::UpdateConnection(
  std::optional<const std::string> host, std::optional<const uint16_t> port) {
  update_connection_working_.store(true);
  update_connection_success_.store(false);
  connection_update_ex_ = nullptr;

  const auto &conn_host = this->main_connection_host_port_.first;
  const auto &conn_port = this->main_connection_host_port_.second;
  const auto pair = Node(host.value_or(conn_host), port.value_or(conn_port));

  std::cout << "[AsterHiredis] Start UpdateConnection to Host " << pair.first
            << " Port " << pair.second << std::endl;
  return ConnectHPPair_impl(pair)
    .finally([this] {
      if (IsUpdateFail()) {
        ConnectionUnit::PrintException(connection_update_ex_,
                                       "UpdateConnection first stage");
        return RepeatConnectAvailableList().finally([this] {
          if (IsUpdateFail()) {
            ConnectionUnit::PrintException(connection_update_ex_,
                                           "UpdateConnection second stage");
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
          "server connection in UpdateConnection third stage -- ";
        return ConnectionUnit::ReturnConnectionException<>(
          std::move(err), connection_update_ex_, seastar_logger);
      } else {
        return make_ready_future();
      }
    })
    .finally([this] { update_connection_working_.store(false); });
}

}  // namespace AsterHiredis