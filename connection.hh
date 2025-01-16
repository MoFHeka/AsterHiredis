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

#include <any>
#include <chrono>
#include <map>
#include <queue>

#ifndef _MSC_VER
#include <sys/time.h> /* for struct timeval */
#else
struct timeval; /* forward declaration */
#endif

#include <seastar/core/future-util.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/api.hh>

#include "alloc.hh"
#include "reader.hh"
#include "writer.hh"

using namespace seastar;

namespace AsterHiredis {

using Node = std::pair<std::string, uint16_t>;

enum class RedisRole { MASTER = 0, SLAVE = 1, RANDOM = 2 };

enum ReplyErrorType { ERR = 0b00, MOVED = 0b01, ASK = 0b10 };

namespace ConnectionUnit {

using RedisReplyUPtr = std::unique_ptr<redisReply, ReplyDeleter>;
using RedisReplyVector = std::vector<RedisReplyUPtr>;
using RedisReplyVectorSPtr = std::shared_ptr<RedisReplyVector>;

class ConnectionException : public std::exception {
 protected:
  const std::string message_;
  const std::any incomplete_return_;
  const bool network_stack_error_;

 public:
  ConnectionException(const std::string message,
                      std::any incomplete_return = std::any{},
                      bool network_stack_error = false)
    : message_(std::move(message)),
      incomplete_return_(std::move(incomplete_return)),
      network_stack_error_(network_stack_error){};
  virtual const char *what() const noexcept override { return message_.data(); }
  virtual const std::any incomplete_return() const noexcept {
    return incomplete_return_;
  }
  virtual const bool network_stack_error() const noexcept {
    return network_stack_error_;
  }
};

inline static void
throw_connection_exception(const std::string &message,
                           std::any incomplete_return = std::any{}) {
  std::throw_with_nested(ConnectionException(message, incomplete_return));
}

template <typename T,
          std::enable_if_t<std::is_same<std::exception, T>::value> * = nullptr>
void PrintException(const T &e, std::string prefix = "", int level = 0) {
  seastar_logger.error("[AsterHiredis] Exception from {} -- level: {} -- {}",
                       prefix, level, e);
  try {
    std::rethrow_if_nested(e);
  } catch (const std::exception &nestedException) {
    PrintException(nestedException, "", level + 1);
  }
}

template <typename T, std::enable_if_t<
                        std::is_same<std::exception_ptr, T>::value> * = nullptr>
void PrintException(const T eptr, std::string prefix = "", int level = 0) {
  if (eptr) {
    seastar_logger.error("[AsterHiredis] Exception from {} -- level: {} -- {}",
                         prefix, level, eptr);
    try {
      std::rethrow_if_nested(eptr);
    } catch (const std::exception &nestedException) {
      PrintException(nestedException, "", level + 1);
    }
  }
}

template <typename R = void>
static future<R> ReturnConnectionException(std::string err,
                                           const std::exception_ptr ep,
                                           seastar::logger &logger) {
  try {
    if (ep) {
      std::rethrow_exception(ep);
    } else {
      throw std::runtime_error("Unknown Errors");
    }
  } catch (const ConnectionException &e) {
    err.append(e.what());
    logger.error("{}", err);
    return make_exception_future<R>(std::make_exception_ptr(ConnectionException(
      err, e.incomplete_return(), e.network_stack_error())));
  } catch (const seastar::nested_exception &ne) {
    auto &&ne_str =
      fmt::format("{} (while cleaning up after {})", ne.inner, ne.outer);
    err.append(ne_str.c_str(), ne_str.length());
    logger.error("{}", err);
    return make_exception_future<R>(
      std::make_exception_ptr(ConnectionException(err)));
  } catch (const std::exception &e) {
    err.append(e.what());
    logger.error("{}", err);
    return make_exception_future<R>(
      std::make_exception_ptr(ConnectionException(err)));
  }
}

template <typename R = void>
static future<R> ReturnConnectionException(std::string err,
                                           const std::exception_ptr ep) {
  try {
    if (ep) {
      std::rethrow_exception(ep);
    } else {
      throw std::runtime_error("Unknown Errors");
    }
  } catch (const gate_closed_exception &e) {
    err.append(e.what());
    return make_exception_future<R>(
      std::make_exception_ptr(ConnectionException(err, std::any{}, true)));
  } catch (const ConnectionException &e) {
    err.append(e.what());
    return make_exception_future<R>(std::make_exception_ptr(ConnectionException(
      err, e.incomplete_return(), e.network_stack_error())));
  } catch (const seastar::nested_exception &ne) {
    auto &&ne_str =
      fmt::format("{} (while cleaning up after {})", ne.inner, ne.outer);
    err.append(ne_str.c_str(), ne_str.length());
    return make_exception_future<R>(
      std::make_exception_ptr(ConnectionException(err)));
  } catch (const std::exception &e) {
    err.append(e.what());
    return make_exception_future<R>(
      std::make_exception_ptr(ConnectionException(err)));
  }
}

struct ConnectionTimeoutExceptionFactory {
  static auto timeout() {
    return ConnectionException("[AsterHiredis] timedout");
  }
};

struct ConnectionSemaphoreExceptionFactory {
  static auto timeout() {
    return ConnectionException("[AsterHiredis] timedout");
  }
  static auto broken() {
    return ConnectionException("[AsterHiredis] semaphore is broken");
  }
};

namespace InputStreamConsumer {
using consumption_result_type =
  typename input_stream<char>::consumption_result_type;
using stop_consuming_type =
  typename consumption_result_type::stop_consuming_type;
using InputStreamConsumerFunction =
  std::function<future<consumption_result_type>(stop_consuming_type::tmp_buf)>;
typedef struct InputStreamConsumerFunctor {
  Reader::redisReader redis_reader();
  /*
   * consumer used like this:
   *    auto in = input_stream<char>();
   *    struct my_consumer : InputStreamConsumerFunctor { ...}
   *    in.consume(my_consumer{})
   */
  virtual future<consumption_result_type>
  operator()(stop_consuming_type::tmp_buf buf) = 0;
  /*
  implementation example:
  {
    if (condition_0) {
      return make_ready_future<consumption_result_type>(
        continue_consuming{});
    } else if (condition_1) {
      return make_ready_future<consumption_result_type>(skip_bytes{100});
    } else {
      return make_ready_future<consumption_result_type>(
        stop_consuming_type({}));
    }
  }
  */
} InputStreamConsumerFunctor;
}  // namespace InputStreamConsumer

static inline auto
TimeoutEndMilliSec(const std::chrono::milliseconds timeout_ms) {
  return std::chrono::steady_clock::now() + timeout_ms;
}

static inline auto TimeoutEndSec(const std::chrono::seconds timeout_s) {
  return std::chrono::steady_clock::now() + timeout_s;
}

static inline auto TimeoutEndMin(const std::chrono::minutes timeout_m) {
  return std::chrono::steady_clock::now() + timeout_m;
}

class Connection {
 protected:
  connected_socket fd_;  // Please make sure this socket is reliable.
                         // Check socket is OK outside this Connection class.
  output_stream<char> write_buf_;
  input_stream<char> read_buf_;
  std::queue<uint64_t> channel_id_queue_;
  std::map<uint64_t, seastar::lw_shared_ptr<seastar::queue<RedisReplyUPtr>>>
    reply_map_;
  std::map<uint64_t, seastar::lw_shared_ptr<seastar::queue<RedisReplyUPtr>>>
    push_msg_map_;

  std::chrono::milliseconds wait_timeout_;
  std::chrono::milliseconds socket_timeout_;
  const bool tcp_keep_alive_;
  const uint32_t tcp_keep_intvl_;

 public:
  seastar::gate connection_gate_;

 public:
  Connection(connected_socket fd,
             const std::chrono::milliseconds wait_timeout =
               std::chrono::milliseconds(20000),
             const std::chrono::milliseconds socket_timeout =
               std::chrono::milliseconds(10000),
             const bool tcp_keep_alive = false,
             const uint32_t tcp_keep_intvl = 0);
  ~Connection();

  // sending functions
 private:
  constexpr static auto HandleFormatCommandExcep =
    [](std::exception_ptr ep) -> future<> {
    std::string err = "";
    bool network_stack_error = false;
    try {
      std::rethrow_exception(ep);
    } catch (const ConnectionException &e) {
      err.append(e.what());
    } catch (const std::exception &e) {
      err.append(e.what());
      network_stack_error = true;
    }
    return make_exception_future(std::make_exception_ptr(
      ConnectionException(err, std::any{}, network_stack_error)));
  };

  template <typename... Args>
  future<> FormatCommandAndSend(const uint64_t channel_id,
                                Writer::redisWriter *w, const Args &... args) {
    return FormatCommand<Args...>(w, args...).then([this, w = w]() {
      auto f_w =
        this->write_buf_.write(std::move(*(w->tmp_buf))).then([this]() {
          return this->write_buf_.flush().then([this]() {
            this->channel_id_queue_.push(channel_id);
            return make_ready_future();
          });
        });
      return seastar::with_timeout<ConnectionTimeoutExceptionFactory>(
               TimeoutEndMilliSec(socket_timeout_), std::move(f_w))
        .handle_exception(HandleFormatCommandExcep);
    });
  }

  template <typename... Args>
  future<> FormatCommandAndWrite(const uint64_t channel_id,
                                 Writer::redisWriter *w, const Args &... args) {
    return FormatCommand<Args...>(w, args...).then([this, w = w]() {
      auto f_w =
        this->write_buf_.write(std::move(*(w->tmp_buf))).then([this]() {
          this->channel_id_queue_.push(channel_id);
          return make_ready_future();
        });
      return seastar::with_timeout<ConnectionTimeoutExceptionFactory>(
               TimeoutEndMilliSec(socket_timeout_), std::move(f_w))
        .handle_exception(HandleFormatCommandExcep);
    });
  }

  template <typename... Args>
  future<> FormatCommandAndSendWithWriter(const uint64_t channel_id,
                                          std::tuple<Args...> &args) {
    auto apply_f = [this, channel_id, args = args](auto w) mutable {
      return std::apply(  // std::apply compatible with C++17 to capture pack
        [this, channel_id, w = w](auto &&... args) {
          return FormatCommandAndSend(channel_id, w, args...);
        },
        std::move(args));
    };
    auto writer = new Writer::redisWriter();
    return do_with(std::move(writer), std::move(apply_f))
      .finally([this, writer = writer] { delete writer; });
  }

  template <typename... Args>
  future<> FormatCommandAndWriteWithWriter(const uint64_t channel_id,
                                           std::tuple<Args...> &args) {
    auto apply_f = [this, channel_id, args = args](auto w) mutable {
      return std::apply(  // std::apply compatible with C++17 to capture pack
        [this, channel_id, w = w](auto &&... args) {
          return FormatCommandAndWrite(channel_id, w, args...);
        },
        std::move(args));
    };
    auto writer = new Writer::redisWriter();
    return do_with(std::move(writer), std::move(apply_f))
      .finally([this, writer = writer] { delete writer; });
  }

 public:
  void AddChannelInConnection(
    uint64_t channel_id,
    seastar::shared_ptr<seastar::queue<RedisReplyUPtr>> channel_reply_pipe_ptr,
    seastar::shared_ptr<seastar::queue<RedisReplyUPtr>>
      channel_push_msg_pipe_ptr) {
    this->reply_map_.insert_or_assign(channel_id, channel_reply_pipe_ptr);
    this->push_msg_map_.insert_or_assign(channel_id, channel_push_msg_pipe_ptr);
  }

  void RemoveChannelInConnection(uint64_t channel_id) {
    this->reply_map_.erase(channel_id);
  }

  template <typename... Args>
  future<> FormatCommand(Writer::redisWriter *const w, const Args &... args) {
    int len = Writer::redisFormatCommand(w, args...);
    if (len >= 0) {
      return make_ready_future<>();
    } else {
      const std::string err = "[AsterHiredis] Unkown commands formatting error "
                              "happended, return length is " +
                              std::to_string(len);
      return make_exception_future(
        std::make_exception_ptr(ConnectionException(err)));
    }
  }

  template <typename... Args>
  future<> Send(const uint64_t channel_id, const Args &... args) {
    return try_with_gate(connection_gate_,
                         [this, channel_id,
                          args = std::make_tuple(
                            std::forward<const Args &>(args)...)]() mutable {
                           return FormatCommandAndSendWithWriter(channel_id,
                                                                 args);
                         })
      .handle_exception([this](auto ep) {
        std::string err = "[AsterHiredis] Sending Command Error -- ";
        return ReturnConnectionException<>(err, ep);
      });
  }

  template <typename... Args>
  future<> WritePipeLine(const uint64_t channel_id, const Args &... args) {
    return try_with_gate(connection_gate_,
                         [this, channel_id,
                          args = std::make_tuple(
                            std::forward<const Args &>(args)...)]() mutable {
                           return FormatCommandAndWriteWithWriter(channel_id,
                                                                  args);
                         })
      .handle_exception([this](auto ep) {
        std::string err =
          "[AsterHiredis] Writing Command to network stack pipe Error -- ";
        return ReturnConnectionException<>(err, ep);
      });
  }

  future<> SendPipeLine() {
    return try_with_gate(connection_gate_,
                         [this] { return this->write_buf_.flush(); })
      .handle_exception([this](auto ep) {
        std::string err =
          "[AsterHiredis] Sending Command from network stack pipe Error -- ";
        return ReturnConnectionException<>(err, ep);
      });
  }

  // receiving functions
 private:
  future<stop_iteration>
  Connection::RepeatParseRecvErr(Reader::redisReader *r,
                                 redisReply *err_tmp_reply);

  future<stop_iteration> Connection::RepeatParseRecvOk(Reader::redisReader *r);

  future<>
  RepeatParseRecvImpl(Reader::redisReader *r REDIS_STATUS *redis_statu);

  future<Reader::RESP_PARSER_STATUS> RepeatParseRecv(Reader::redisReader *r);

  future<stop_iteration> RepeatParseRecvAndReturnRepeat(Reader::redisReader *r);

  future<> WriteReplyPipe(const uint64_t channel_id,
                          const redisReply *const reply) {
    auto id_pipe_pair = reply_map_.find(channel_id);
    if (id_pipe_pair != reply_map_.end()) {
      return id_pipe_pair->second->push_eventually(
        RedisReplyUPtr(const_cast<redisReply *>(reply)));
    }
  }

  future<> WritePushMsgPipe(const uint64_t channel_id,
                            const redisReply *const reply) {
    auto id_pipe_pair = push_msg_map_.find(channel_id);
    if (id_pipe_pair != push_msg_map_.end()) {
      return id_pipe_pair->second->push_eventually(
        RedisReplyUPtr(const_cast<redisReply *>(reply)));
    }
  }

 public:
  future<> Connection::RepeatRecv(Reader::redisReader *r,
                                  temporary_buffer<char> &&tmp_buf);

  future<> Recv();

  // Keeping handle input stream by ConsumeCallback util reaching eof.
  template <class S>
  future<> ConsumeWithCallback(const S &input_consumer) {
    return try_with_gate(connection_gate_,
                         [this, &input_consumer = input_consumer] {
                           return read_buf_.consume(input_consumer);
                         })
      .handle_exception([this](auto ep) {
        std::string err =
          "[AsterHiredis] Consuming Response With Callback Error -- ";
        return ReturnConnectionException<>(err, ep);
      });
  }

 public:
  future<> CheckOKRead(seastar::temporary_buffer<char> &&buf);

  future<> PingPongTest(
    std::optional<std::function<future<>()>> PingPongFunc = std::nullopt);

  future<> Authorization(const std::string &redis_user,
                         const std::string &redis_password);

  future<> SelectDB(const int db);
};

using ConnectionSPtr = seastar::shared_ptr<Connection>;

future<connected_socket>
CreateSocketFD(const std::string &host, const uint16_t port,
               const std::chrono::milliseconds timeout_ms);

ReplyErrorType ParseErrorReply(const std::string_view &msg);

RedisRole GetConnectionRoleFromInfo(const std::string_view &info);

std::vector<Node> GetMasterNodeFromSlaveInfo(const std::string_view &info);

std::vector<Node> GetSlaveNodeFromMasterInfo(const std::string_view &info);

future<std::optional<Node>> GetRoleMachingNode(const ConnectionSPtr conn,
                                               const Node &master_node,
                                               const RedisRole params_role);

}  // namespace ConnectionUnit
}  // namespace AsterHiredis