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

#include "cluster.hh"

namespace AsterHiredis {

template <>
RedisClient<RedisClientType::CLUSTER>::RedisClient(
  const ClientParams &client_params, const size_t max_ops_capacity,
  const alien::instance *seastar_instance)
  : RedisClientBase(client_params, max_ops_capacity, seastar_instance) {

  if (client_params.redis_instance_type != RedisClientType::CLUSTER) {
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
RedisClient<RedisClientType::CLUSTER>::~RedisClient(){};

template <>
future<RedisReplyVectorSPtr>
RedisClient<RedisClientType::CLUSTER>::CheckReplyFullError(
  RedisReplyVectorSPtr rRlp) {
  bool recv_has_error = false;
  bool recv_all_error = true;
  short err_bit = 0b00;
  std::string err = "[AsterHiredis] Received error reply from Redis";
  for (size_t i = 0; i < rRlp->size(); ++i) {
    if (rRlp->at(i)->type == REDIS_REPLY_ERROR) {
      recv_has_error = true;
      err += fmt::format(" -- At reply {}:", i);
      std::string_view err_str(rRlp->at(i)->str, rRlp->at(i)->len);
      auto parse_err = ReplyErrorType::ERR;
      try {
        parse_err = ConnectionUnit::ParseErrorReply(err_str);
      } catch (const std::exception &e) {}
      switch (parse_err) {
      case ReplyErrorType::ERR:
        err_bit &= 0b00;
      case ReplyErrorType::ASK:
        err_bit &= 0b01;
      case ReplyErrorType::MOVED:
        err_bit &= 0b10;
      default:
        break;
      }
      err.append(err_str.data(), err_str.size());
    } else {
      recv_all_error = false;
    }
  }
  switch (err_bit) {
  case 0b00:
    if (recv_all_error == true) {
      return make_exception_future<RedisReplyVectorSPtr>(
        std::make_exception_ptr(ConnectionUnit::ConnectionException(
          err, std::make_any<RedisReplyVectorSPtr>(rRlp), false)));
    } else {
      if (recv_has_error) seastar_logger.error("{}", err);
      return make_ready_future<RedisReplyVectorSPtr>(std::move(rRlp));
    }
  case 0b01:
    return make_exception_future<RedisReplyVectorSPtr>(std::make_exception_ptr(
      AskError(err, std::make_any<RedisReplyVectorSPtr>(rRlp), false)));
  case 0b10:
    return make_exception_future<RedisReplyVectorSPtr>(std::make_exception_ptr(
      MovedError(err, std::make_any<RedisReplyVectorSPtr>(rRlp), true)));
  case 0b11:
    return make_exception_future<RedisReplyVectorSPtr>(std::make_exception_ptr(
      MovedError(err, std::make_any<RedisReplyVectorSPtr>(rRlp), true)));
  default:
    return make_ready_future<RedisReplyVectorSPtr>(std::move(rRlp));
  }
}

template <>
future<> RedisClient<RedisClientType::CLUSTER>::CommonActionAfterConnect(
  ConnectionSPtr conn) {
  return conn
    ->Authorization(this->client_params_.redis_user,
                    this->client_params_.redis_password)
    .then([this, conn] { return conn->PingPongTest(std::nullopt); });
}

template <>
future<> RedisClient<RedisClientType::CLUSTER>::RepeatConnectAvailableList() {
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
              this->main_connection_ = std::move(conn);
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
RedisClient<RedisClientType::CLUSTER>::ConnectHPPair_impl(const Node &pair) {
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
                       this->main_connection_ = std::move(conn);
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
future<> RedisClient<RedisClientType::CLUSTER>::RepeatConnectOriginalList() {
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
future<> RedisClient<RedisClientType::CLUSTER>::CreateInitialConnection(
  const Node &pair) {
  return ConnectHPPair_impl(pair)
    .finally([this] {
      if (IsUpdateFail()) {
        ConnectionUnit::PrintException(connection_update_ex_,
                                       "CreateInitialConnection first stage");
        return RepeatConnectAvailableList().finally([this] {
          if (IsUpdateFail()) {
            ConnectionUnit::PrintException(
              connection_update_ex_, "CreateInitialConnection second stage");
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
          "server connection in CreateInitialConnection third stage -- ";
        return ConnectionUnit::ReturnConnectionException<>(
          std::move(err), connection_update_ex_, seastar_logger);
      } else {
        return make_ready_future();
      }
    });
}

template <>
future<ConnectionSPtr>
RedisClient<RedisClientType::CLUSTER>::CreateConnectionAndTest(
  const std::string &host, const uint16_t port) {
  return CreateConnection(host, port, this->client_params_)
    .then([this](ConnectionSPtr conn) {
      return CommonActionAfterConnect(conn).then(
        [this, conn = std::move(conn)] {
          return make_ready_future<ConnectionSPtr>(std::move(conn));
        });
    });
}

template <>
future<> RedisClient<RedisClientType::CLUSTER>::CreateClusterConnection() {
  auto &shards = this->slot_range_map_;
  return do_for_each(shards.begin(), shards.end(), [this](auto &shards_iter) {
    auto &&node = shards_iter.second;
    auto &&host = node.first;
    auto &&port = node.second;
    return CreateConnectionAndTest(host, port)
      .then([this, &shards_iter = shards_iter](ConnectionSPtr conn) {
        return ConnectionUnit::GetRoleMachingNode(
                 conn, shards_iter.second, this->client_params_.redis_role)
          .then(
            [this, conn = std::move(conn)](std::optional<Node> &&hp_pair_opt) {
              if (hp_pair_opt.has_value()) {
                auto &&hp_pair = hp_pair_opt.value();
                return CreateConnectionAndTest(hp_pair.first, hp_pair.second);
              } else {
                return make_ready_future<ConnectionSPtr>(std::move(conn));
              }
            })
          .then([this, &shards_iter = shards_iter](ConnectionSPtr conn) {
            // Attention! The connection here does not correspond to the node's
            // IP. It may be a slave node of the node's IP
            this->cluster_connections_[shards_iter.second] = std::move(conn);
          });
      })
      .handle_exception([this, &shards_iter = shards_iter](auto ep) {
        auto &&node = shards_iter.second;
        std::string err =
          "[AsterHiredis] Fail to create cluster node connection to host:" +
          node.first + " port:" + std::to_string(node.second) + " -- ";
        return ConnectionUnit::ReturnConnectionException<>(err, ep,
                                                           seastar_logger);
      });
  });
}

template <>
future<> RedisClient<RedisClientType::CLUSTER>::UpdateConnection(
  std::optional<const std::string> host, std::optional<const uint16_t> port) {
  update_connection_working_.store(true);
  update_connection_success_.store(false);
  connection_update_ex_ = nullptr;

  const auto &conn_host = this->main_connection_host_port_.first;
  const auto &conn_port = this->main_connection_host_port_.second;
  const auto pair = Node(host.value_or(conn_host), port.value_or(conn_port));

  std::cout << "[AsterHiredis] Start UpdateConnection to Host " << pair.first
            << " Port " << pair.second << std::endl;
  return CreateInitialConnection(pair)
    .then([this] { return shards::GetClusterSlots(this->main_connection_); })
    .then([this](Shards &&shards) { this->slot_range_map_ = shards; })
    .then([this] { return CreateClusterConnection(); })
    .handle_exception([this](auto ep) {
      std::string err = "[AsterHiredis] Fail to create or update Redis server "
                        "connection in UpdateConnection -- ";
      return ConnectionUnit::ReturnConnectionException<>(err, ep,
                                                         seastar_logger);
    })
    .finally([this] { update_connection_working_.store(false); });
}

}  // namespace AsterHiredis