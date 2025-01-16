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

#include <charconv>
#include <random>
#include <tuple>

#include <sys/time.h>

#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>

#include "connection.hh"

using namespace seastar;

namespace AsterHiredis {
namespace ConnectionUnit {

Connection::Connection(connected_socket fd,
                       const std::chrono::milliseconds wait_timeout,
                       const std::chrono::milliseconds socket_timeout,
                       const bool tcp_keep_alive, const uint32_t tcp_keep_intvl)
  : fd_(std::move(fd)), write_buf_(fd_.output()), read_buf_(fd_.input()),
    wait_timeout_(wait_timeout), socket_timeout_(socket_timeout),
    tcp_keep_alive_(tcp_keep_alive), tcp_keep_intvl_(tcp_keep_intvl)) {

  // Set POSIX tcp socket timeout
  static constexpr int SOL_SOCKET_ = 1;    // level
  static constexpr int SO_RCVTIMEO_ = 20;  // optname
  static constexpr int SO_SNDTIMEO_ = 21;  // optname
  auto sec = std::chrono::duration_cast<std::chrono::seconds>(socket_timeout_);
  auto msec = std::chrono::duration_cast<std::chrono::microseconds>(
    socket_timeout_ - sec);
  struct timeval tv {
    .tv_sec = sec.count(), .tv_usec = msec.count()
  };
  const void *tv_to_ptr = &tv;
  size_t tv_to_sz = sizeof(tv);
  fd_.set_sockopt(SOL_SOCKET_, SO_RCVTIMEO_, tv_to_ptr, tv_to_sz);
  fd_.set_sockopt(SOL_SOCKET_, SO_SNDTIMEO_, tv_to_ptr, tv_to_sz);

  // Set tcp socket keep_alive
  if (tcp_keep_alive_) {
    fd_.set_keepalive(tcp_keep_alive_);
    if (tcp_keep_intvl_ > 0) {
      struct net::tcp_keepalive_params tcp_keepalive_params_ = {
        .idle = std::chrono::seconds(tcp_keep_intvl_ / 3),
        .interval = std::chrono::seconds(tcp_keep_intvl_),
        .count = 3};
      fd_.set_keepalive_parameters(tcp_keepalive_params_);
    }
  }

  // Set tcp nodelay
  fd_.set_nodelay(true);
}

Connection::~Connection() {
  while (connection_gate_.get_count()) {
    connection_gate_.leave();
  };
  fd_.shutdown_output();
  fd_.shutdown_input();
}

future<stop_iteration>
Connection::RepeatParseRecvErr(Reader::redisReader *r,
                               redisReply *err_tmp_reply) {
  auto repeat_return_err = [this](redisReply *err_tmp_reply) {
    seastar::do_until([this] { return channel_id_queue_.empty() == false; },
                      [this, err_tmp_reply = err_tmp_reply] {
                        return WriteReplyPipe(channel_id_queue_.front(),
                                              err_tmp_reply)
                          .then([this] { channel_id_queue_.pop(); });
                      })
  };
  if (likely(err_tmp_reply)) {
    return repeat_return_err(static_cast<redisReply *>(err_tmp_reply))
      .then([this] {
        return make_ready_future<stop_iteration>(stop_iteration::yes);
      });
  } else {
    auto err_tmp_reply_p = new redisReply();
    err_tmp_reply->err = 2;
    err_tmp_reply->errstr = "Unknow parse error.";
    return do_with(std::move(err_tmp_reply_p),
                   [this](auto err_tmp_reply_p) {
                     return repeat_return_err(err_tmp_reply_p).then([this] {
                       return make_ready_future<stop_iteration>(
                         stop_iteration::yes);
                     });
                   })
      .finally(
        [this, err_tmp_reply_p = err_tmp_reply_p] { delete err_tmp_reply_p; });
  }
}

future<stop_iteration> Connection::RepeatParseRecvOk(Reader::redisReader *r) {
  if (r->pos == r->len) {
    return make_ready_future<stop_iteration>(stop_iteration::yes);
  } else {
    return make_ready_future<stop_iteration>(stop_iteration::no);
  }
}

future<> RepeatParseRecvImpl(Reader::redisReader *r REDIS_STATUS *redis_statu) {
  return repeat([this, r = r, redis_statu = redis_statu] {
    void *tmp_reply = nullptr;
    *redis_statu = Reader::redisReaderGetReply(r, &tmp_reply);
    if (*redis_statu != REDIS_STATUS::OK) {
      if (unlikely(r->err)) {
        if (!connection_gate_.is_closed()) {
          return connection_gate_.close().then(
            [this, r = r, tmp_reply = tmp_reply] {
              return RepeatParseRecvErr(r, tmp_reply)
            });
        } else {
          return RepeatParseRecvErr(r, tmp_reply);
        }
      } else {
        return make_ready_future<stop_iteration>(stop_iteration::yes);
      }
    } else if (*redis_statu == REDIS_STATUS::OK && tmp_reply) {
      if (Reader::redisIsPushReply(static_cast<redisReply *>(tmp_reply))) {
        return WritePushMsgPipe(channel_id_queue_.front(), tmp_reply)
          .then([this, r = r] {
            channel_id_queue_.pop();
            return RepeatParseRecvOk(r);
          });
      }
#if defined(RESP2)
      else if (Reader::redisIsMessageReply(
                 static_cast<redisReply *>(tmp_reply))) {
        return WritePushMsgPipe(channel_id_queue_.front(), tmp_reply)
          .then([this] {
            channel_id_queue_.pop();
            return RepeatParseRecvOk(r);
          });
      }
#endif
      else {
        return WriteReplyPipe(channel_id_queue_.front(), tmp_reply)
          .then([this] {
            channel_id_queue_.pop();
            return RepeatParseRecvOk(r);
          });
      }
    }
    return make_ready_future<stop_iteration>(stop_iteration::yes);
  });
}

future<Reader::RESP_PARSER_STATUS>
Connection::RepeatParseRecv(Reader::redisReader *r) {
  auto redis_statu = new REDIS_STATUS(REDIS_STATUS::OK);
  return do_with(std::move(redis_statu),
                 [this, r = r](auto redis_statu) {
                   return RepeatParseRecvImpl(r, redis_statu)
                     .then([this, r = r, redis_statu = redis_statu] {
                       if (likely(*redis_statu == REDIS_STATUS::OK)) {
                         return make_ready_future<Reader::RESP_PARSER_STATUS>(
                           Reader::RESP_PARSER_STATUS::OK);
                       } else {
                         if (unlikely(r->err)) {
                           return make_ready_future<Reader::RESP_PARSER_STATUS>(
                             Reader::RESP_PARSER_STATUS::ERR);
                         } else {
                           return make_ready_future<Reader::RESP_PARSER_STATUS>(
                             Reader::RESP_PARSER_STATUS::INCOMPLETE);
                         }
                       }
                       return make_ready_future<Reader::RESP_PARSER_STATUS>(
                         Reader::RESP_PARSER_STATUS::ERR);
                     });
                 })
    .finally([this, redis_statu = redis_statu] { delete redis_statu; });
}

future<stop_iteration> RepeatParseRecvAndReturnRepeat(Reader::redisReader *r) {
  return RepeatParseRecv(r).then(
    [this, r = r](const Reader::RESP_PARSER_STATUS &&ret) {
      switch (ret) {
      case Reader::RESP_PARSER_STATUS::OK:
      case Reader::RESP_PARSER_STATUS::INCOMPLETE:
        return make_ready_future<stop_iteration>(stop_iteration::no);
        break;
      case Reader::RESP_PARSER_STATUS::ERR:
      default:
        return make_ready_future<stop_iteration>(stop_iteration::yes);
        break;
      }
    });
}

future<> Connection::RepeatRecv(Reader::redisReader *r,
                                temporary_buffer<char> &&tmp_buf) {
  if (unlikely(tmp_buf.empty())) {
    return make_ready_future<stop_iteration>(stop_iteration::yes);
  }
  if (r->gather_builder._value.empty()) {
    if (Reader::redisReaderFeed(r, tmp_buf) != REDIS_STATUS::OK) {
      return make_exception_future<>(
        std::make_exception_ptr(ConnectionException(std::string(r->errstr))));
    }
    return RepeatParseRecvAndReturn(r);
  } else {
    if (r->res_bulk_len > 0) {
      auto start = tmp_buf.begin();
      r->gather_builder._value += sstring(start, start + r->res_bulk_len);
      if (Reader::redisReaderFeed(r, r->gather_builder.get()) !=
          REDIS_STATUS::OK) {
        return make_exception_future<>(
          std::make_exception_ptr(ConnectionException(std::string(r->errstr))));
      }
      return RepeatParseRecv(r).then([this, r = r, &tmp_buf] {
        r->gather_builder.reset();
        tmp_buf.trim_front(r->res_bulk_len + 1);
        if (Reader::redisReaderFeed(r, tmp_buf) != REDIS_STATUS::OK) {
          return make_exception_future<>(std::make_exception_ptr(
            ConnectionException(std::string(r->errstr))));
        }
        return RepeatParseRecvAndReturn(r);
      });
    }
  }
}

future<> Connection::Recv() {
  auto reader = new Reader::redisReader();
  return do_with(
           std::move(reader),
           [this](auto r) {
             return try_with_gate(connection_gate_, [this, r = r] {
               auto f_r = read_buf_.read().handle_exception([this](auto ep) {
                 std::string err = "";
                 try {
                   std::rethrow_exception(ep);
                 } catch (const std::exception &e) { err.append(e.what()); }
                 return make_exception_future<>(std::make_exception_ptr(
                   ConnectionException(err, std::any{}, true)));
               });
               return seastar::repeat([this, r = r, f_r = std::move(f_r)] {
                 return seastar::with_timeout<
                          ConnectionTimeoutExceptionFactory>(
                          TimeoutEndMilliSec(socket_timeout_), std::move(f_r))
                   .then([this, r = r](temporary_buffer<char> &&tmp_buf) {
                     return RepeatRecv(r, std::move(tmp_buf));
                   });
               });
             });
           })
    .handle_exception([this](auto ep) {
      std::string err = "[AsterHiredis] Receiving Response Error -- ";
      return ReturnConnectionException<>(err, ep);
    })
    .finally([this, reader = reader] { delete reader; });
}

future<> Connection::PingPongTest(
  std::optional<std::function<future<>()>> PingPongFunc) {
  auto default_function = [this]() -> future<> {
    seastar::temporary_buffer<char> ping_cmd("PING\r\n", 6);
    return write_buf_.write(std::move(ping_cmd))
      .then([this] { return write_buf_.flush(); })
      .then([this] {
        return read_buf_.read().then([this](
                                       seastar::temporary_buffer<char> &&buf) {
          auto str = std::string(buf.get(), buf.size());
          if (buf.size() != 7) {
            const std::string err =
              fmt::format("[AsterHiredis] illegal packet received: {}, "
                          "buffer size is {}\n",
                          str, str.size());
            return make_exception_future(
              std::make_exception_ptr(ConnectionException(err)));
          } else if (str != "+PONG\r\n") {
            const std::string err =
              fmt::format("[AsterHiredis] illegal packet received: {}\n", str);
            return make_exception_future(
              std::make_exception_ptr(ConnectionException(err)));
          } else {
            return seastar::make_ready_future();
          }
        });
      })
      .handle_exception([this](auto ep) {
        std::string err =
          "[AsterHiredis] Received Error Response When PingPongTest -- ";
        return ReturnConnectionException<>(err, ep);
      });
  };
  auto pingpongfunc = PingPongFunc.value_or(default_function);
  return seastar::with_timeout<ConnectionTimeoutExceptionFactory>(
    TimeoutEndMilliSec(wait_timeout_), std::move(pingpongfunc()));
}

future<> Connection::CheckOKRead(seastar::temporary_buffer<char> &&buf) {
  auto str = std::string(buf.get(), buf.size());
  if (buf.size() != 5) {
    const std::string err =
      fmt::format("[AsterHiredis] illegal packet received: {}, "
                  "buffer size is {}\n",
                  str, str.size());
    return make_exception_future(
      std::make_exception_ptr(ConnectionException(err)));
  } else if (str != "+OK\r\n") {
    const std::string err =
      fmt::format("[AsterHiredis] illegal packet received: {}\n", str);
    return make_exception_future(
      std::make_exception_ptr(ConnectionException(err)));
  } else {
    return seastar::make_ready_future();
  }
}

future<> Connection::Authorization(const std::string &redis_user,
                                   const std::string &redis_password) {
  const std::string DEFAULT_USER = "default";
  bool is_default_user = redis_user == DEFAULT_USER || redis_user.empty();
  if (redis_password.empty()) { return make_ready_future<>(); }
  std::string auth_cmd;
  if (is_default_user) {
    auth_cmd = "AUTH " + redis_password + "\r\n";
  } else {
    // Redis 6.0 or latter
    auth_cmd = "AUTH " + redis_user + " " + redis_password + "\r\n";
  }
  return write_buf_.write(auth_cmd)
    .then([this] { return write_buf_.flush(); })
    .then([this] { return read_buf_.read(); })
    .then([this](seastar::temporary_buffer<char> &&buf) {
      return CheckOKRead(std::move(buf));
    })
    .handle_exception([this, redis_user, redis_password](auto ep) {
      std::string err =
        "[AsterHiredis] Received Error Response When Authorization to user:" +
        redis_user + " and password:" + redis_password + " -- ";
      return ReturnConnectionException<>(err, ep);
    });
}

future<> Connection::SelectDB(const int db) {
  auto db_str = std::to_string(db);
  std::string select_cmd = "SELECT " + db_str + "\r\n";
  return write_buf_.write(select_cmd)
    .then([this] { return write_buf_.flush(); })
    .then([this] { return read_buf_.read(); })
    .then([this](seastar::temporary_buffer<char> &&buf) {
      return CheckOKRead(std::move(buf));
    })
    .handle_exception([this, db_str](auto ep) {
      std::string err =
        "[AsterHiredis] Failed to select database: " + db_str + " -- ";
      return ReturnConnectionException<>(err, ep);
    });
}

future<connected_socket>
CreateSocketFD(const std::string &host, const uint16_t port,
               const std::chrono::milliseconds timeout_ms) {
  auto f =
    net::dns::resolve_name(host).then([port](net::inet_address target_host) {
      sa_family_t family =
        target_host.is_ipv4() ? sa_family_t(AF_INET) : sa_family_t(AF_INET6);
      seastar::socket_address local =
        seastar::socket_address(::sockaddr_in{family, INADDR_ANY, {0}});
      return target_host.is_ipv4()
               ? seastar::connect(seastar::ipv4_addr{target_host, port}, local,
                                  seastar::transport::TCP)
               : seastar::connect(seastar::ipv6_addr{target_host, port}, local,
                                  seastar::transport::TCP);
    });
  return seastar::with_timeout<ConnectionTimeoutExceptionFactory>(
           TimeoutEndMilliSec(timeout_ms), std::move(f))
    .handle_exception([](auto ep) {
      std::string err = "[AsterHiredis] Creating Socket FD Error -- ";
      return ReturnConnectionException<connected_socket>(err, ep);
    });
}

ReplyErrorType ParseErrorReply(const std::string_view &err) {
  // The error contains an Error Prefix, and an optional error message.
  auto idx = err.find_first_of(" \n");

  if (idx == std::string::npos) {
    std::string proto_err =
      "No Error Prefix: " + std::string(err.data(), err.size());
    throw ProtoError(proto_err);
  }

  auto err_prefix = err.substr(0, idx);
  auto err_type = ReplyErrorType::ERR;

  const std::unordered_map<std::string_view, ReplyErrorType>
    redirection_error_map = {{"MOVED", ReplyErrorType::MOVED},
                             {"ASK", ReplyErrorType::ASK}};

  auto iter = redirection_error_map.find(err_prefix);
  if (iter != redirection_error_map.end()) {
    // Specific error.
    err_type = iter->second;
  }  // else Generic error.

  return err_type;
}

RedisRole GetConnectionRoleFromInfo(const std::string_view &info) {
  size_t found = 15, role_start = 0, role_end = 0;
  found = info.find("role:", found);
  if (found != std::string::npos) {
    role_start = found + 5;
    ++found;
    found = info.find("\r\n", found);
    if (found != std::string::npos) {
      role_end = found - 1;
      auto role_str = info.substr(role_start, role_end - role_start + 1);
      if (role_str == "slave") { return RedisRole::SLAVE; }
      return RedisRole::MASTER;
    }
  }
  return RedisRole::MASTER;
}

std::vector<Node> GetMasterNodeFromSlaveInfo(const std::string_view &info) {
  size_t found = 15;
  size_t ip_start = 0, ip_end = 0, port_start = 0, port_end = 0;
  std::vector<Node> slave_hp_pair;
  found = info.find("master_host", found);
  if (found != std::string::npos) {
    ip_start = found + 12;
    ++found;
    found = info.find("master_port", found);
    if (found != std::string::npos) {
      ip_end = found - 3;
      port_start = found + 12;
      ++found;
      found = info.find("master_link_status", found);
      if (found != std::string::npos) {
        port_end = found - 3;
        auto &&ip = info.substr(ip_start, ip_end - ip_start + 1);
        int &&tmp_port = 6379;
        std::from_chars(info.data() + port_start, info.data() + port_end + 1,
                        tmp_port);
        auto &&port = static_cast<uint16_t>(tmp_port);
        slave_hp_pair.emplace_back(std::make_pair(ip, port));
      }
    }
  }
  return slave_hp_pair;
}

std::vector<Node> GetSlaveNodeFromMasterInfo(std::string_view &info) {
  size_t found = 15;
  size_t ip_start = 0, ip_end = 0, port_start = 0, port_end = 0;
  std::vector<Node> slave_hp_pair;
  while (found != std::string::npos) {
    ++found;
    found = info.find("slave", found);
    if (found != std::string::npos) {
      ++found;
      found = info.find("ip", found);
      if (found != std::string::npos) {
        ip_start = found + 3;
        ++found;
        found = info.find("port", found);
        if (found != std::string::npos) {
          ip_end = found - 2;
          port_start = found + 5;
          ++found;
          found = info.find("state", found);
          if (found != std::string::npos) {
            port_end = found - 2;
            auto &&ip = info.substr(ip_start, ip_end - ip_start + 1);
            int &&tmp_port = 6379;
            std::from_chars(info.data() + port_start,
                            info.data() + port_end + 1, tmp_port);
            auto &&port = static_cast<uint16_t>(tmp_port);
            auto &&pair = std::make_pair(ip, port);
            slave_hp_pair.emplace_back(pair);
          }
        }
      }
    }
  }
  return slave_hp_pair;
}

future<std::optional<Node>> GetRoleMachingNode(const ConnectionSPtr conn,
                                               const Node &master_node,
                                               const RedisRole params_role) {
  if (conn && conn.get()) {
    return conn->Send("info Replication")
      .then([conn, &master_node = master_node, params_role] {
        return conn->Recv().then([&master_node = master_node, params_role](
                                   RedisReplyVectorSPtr replylist_p) {
          if (replylist_p->size() <= 0)
            return make_ready_future<std::optional<Node>>(std::nullopt);
          auto reply = std::move(replylist_p->front());
          if (reply->type != REDIS_REPLY_STRING)
            return make_ready_future<std::optional<Node>>(std::nullopt);
          auto buf_str = std::string_view(reply->str, reply->len);
          auto conn_role = GetConnectionRoleFromInfo(buf_str);
          std::vector<Node> alter_hppair;
          if (conn_role != params_role) {
            switch (conn_role) {
            case RedisRole::MASTER:
              alter_hppair = GetSlaveNodeFromMasterInfo(buf_str);
              break;
            case RedisRole::SLAVE:
              alter_hppair = GetMasterNodeFromSlaveInfo(buf_str);
              break;
            default:
              break;
            }
            if (alter_hppair.size() == 0) {
              return make_ready_future<std::optional<Node>>(std::nullopt);
            }
            if (params_role == RedisRole::RANDOM) {
              alter_hppair.emplace_back(master_node);
            }
            thread_local std::default_random_engine engine;
            std::uniform_int_distribution<std::size_t> uniform_dist(
              0, alter_hppair.size() - 1);
            auto &&ret = alter_hppair.at(uniform_dist(engine));
            return make_ready_future<std::optional<Node>>(ret);
          } else {
            return make_ready_future<std::optional<Node>>(std::nullopt);
          }
        });
      });
  }
  return make_ready_future<std::optional<Node>>(std::nullopt);
}

}  // namespace ConnectionUnit
}  // namespace AsterHiredis