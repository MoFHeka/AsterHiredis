/*
 * Copyright (c) 2017 sewenew
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

#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>

namespace AsterHiredis {

#define REDIS_REPLY_STRING 1
#define REDIS_REPLY_ARRAY 2
#define REDIS_REPLY_INTEGER 3
#define REDIS_REPLY_NIL 4
#define REDIS_REPLY_STATUS 5
#define REDIS_REPLY_ERROR 6
#define REDIS_REPLY_DOUBLE 7
#define REDIS_REPLY_BOOL 8
#define REDIS_REPLY_MAP 9
#define REDIS_REPLY_SET 10
#define REDIS_REPLY_ATTR 11
#define REDIS_REPLY_PUSH 12
#define REDIS_REPLY_BIGNUM 13
#define REDIS_REPLY_VERB 14

typedef struct redisReply {
  int type;          /* REDIS_REPLY_* */
  long long integer; /* The integer when type is REDIS_REPLY_INTEGER */
  double dval;       /* The double when type is REDIS_REPLY_DOUBLE */
  size_t len;        /* Length of string */
  char *str;         /* Used for REDIS_REPLY_ERROR, REDIS_REPLY_STRING
                        REDIS_REPLY_VERB, REDIS_REPLY_DOUBLE (in additional to dval),
                        and REDIS_REPLY_BIGNUM. */
  char vtype[4];     /* Used for REDIS_REPLY_VERB, contains the null
                        terminated 3 character content type, such as "txt". */
  size_t elements;   /* number of elements, for REDIS_REPLY_ARRAY */
  struct redisReply **element; /* elements vector for REDIS_REPLY_ARRAY */
} redisReply;

/* Function to free the reply objects hiredis returns by default. */
void freeReplyObject(void *reply);

typedef struct ReplyDeleter {
  void operator()(redisReply *reply) const {
    if (reply != nullptr) { freeReplyObject(reply); }
  };
} ReplyDeleter;

class ProtoError : public std::runtime_error {
 public:
  explicit ProtoError(const std::string &message)
    : std::runtime_error(message){};

  ProtoError(const ProtoError &) = default;
  ProtoError &operator=(const ProtoError &) = default;

  ProtoError(ProtoError &&) = default;
  ProtoError &operator=(ProtoError &&) = default;

  virtual ~ProtoError() = default;
};

namespace reply {

template <typename T>
struct ParseTag {};

template <typename T>
inline T parse(const redisReply *const reply) {
  return parse(ParseTag<T>(), reply);
}

inline bool is_error(const redisReply *const reply) {
  return reply->type == REDIS_REPLY_ERROR;
}

inline bool is_nil(const redisReply *const reply) {
  return reply->type == REDIS_REPLY_NIL; /**/
}

inline bool is_string(const redisReply *const reply) {
  return reply->type == REDIS_REPLY_STRING;
}

inline bool is_status(const redisReply *const reply) {
  return reply->type == REDIS_REPLY_STATUS;
}

inline bool is_integer(const redisReply *const reply) {
  return reply->type == REDIS_REPLY_INTEGER;
}

inline bool is_array(const redisReply *const reply) {
  return reply->type == REDIS_REPLY_ARRAY;
}

template <typename T>
std::optional<T> parse(ParseTag<std::optional<T>>,
                       const redisReply *const reply) {
  if (reply::is_nil(reply)) { return std::nullopt; }

  return std::optional<T>(parse<T>(reply));
}

std::string to_status(const redisReply *const reply);

std::string parse(ParseTag<std::string>, const redisReply *const reply);

long long parse(ParseTag<long long>, const redisReply *const reply);

double parse(ParseTag<double>, const redisReply *const reply);

bool parse(ParseTag<bool>, const redisReply *const reply);

void parse(ParseTag<void>, const redisReply *const reply);

template <typename T, typename U>
std::pair<T, U> parse(ParseTag<std::pair<T, U>>,
                      const redisReply *const reply) {
  if (!is_array(reply)) { throw ProtoError("Expect ARRAY reply"); }

  if (reply->elements != 2) { throw ProtoError("NOT key-value PAIR reply"); }

  if (reply->element == nullptr) { throw ProtoError("Null PAIR reply"); }

  auto *first = reply->element[0];
  auto *second = reply->element[1];
  if (first == nullptr || second == nullptr) {
    throw ProtoError("Null pair reply");
  }

  return std::make_pair(parse<typename std::decay<T>::type>(first),
                        parse<typename std::decay<U>::type>(second));
}

}  // namespace reply

}  // namespace AsterHiredis