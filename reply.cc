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

#include "reply.hh"
#include "alloc.hh"

namespace AsterHiredis {

/* Free a reply object */
void freeReplyObject(void *reply) {
  redisReply *r = static_cast<redisReply *>(reply);
  size_t j;

  if (r == NULL) return;

  switch (r->type) {
  case REDIS_REPLY_INTEGER:
  case REDIS_REPLY_NIL:
  case REDIS_REPLY_BOOL:
    break; /* Nothing to free */
  case REDIS_REPLY_ARRAY:
  case REDIS_REPLY_MAP:
  case REDIS_REPLY_SET:
  case REDIS_REPLY_PUSH:
    if (r->element != NULL) {
      for (j = 0; j < r->elements; j++)
        freeReplyObject(r->element[j]);
      hi_free(r->element);
    }
    break;
  case REDIS_REPLY_ERROR:
  case REDIS_REPLY_STATUS:
  case REDIS_REPLY_STRING:
  case REDIS_REPLY_DOUBLE:
  case REDIS_REPLY_VERB:
  case REDIS_REPLY_BIGNUM:
    hi_free(r->str);
    break;
  }
  hi_free(r);
}

namespace reply {

std::string to_status(const redisReply *const reply) {
  if (!is_status(reply)) {
    throw ProtoError("[AsterHiredis] Expect STATUS reply");
  }

  if (reply->str == nullptr) {
    throw ProtoError("[AsterHiredis] A null status reply");
  }

  // Old version hiredis' *redisReply::len* is of type int.
  // So we CANNOT have something like: *return {reply->str, reply->len}*.
  return std::string(reply->str, reply->len);
}

std::string parse(ParseTag<std::string>, const redisReply *const reply) {
  if (!is_string(reply) && !is_status(reply)) {
    throw ProtoError("[AsterHiredis] Expect STRING reply");
  }

  if (reply->str == nullptr) {
    throw ProtoError("[AsterHiredis] A null string reply");
  }

  // Old version hiredis' *redisReply::len* is of type int.
  // So we CANNOT have something like: *return {reply->str, reply->len}*.
  return std::string(reply->str, reply->len);
}

long long parse(ParseTag<long long>, const redisReply *const reply) {
  if (!is_integer(reply)) {
    throw ProtoError("[AsterHiredis] Expect INTEGER reply");
  }

  return reply->integer;
}

double parse(ParseTag<double>, const redisReply *const reply) {
  return std::stod(parse<std::string>(reply));
}

bool parse(ParseTag<bool>, const redisReply *const reply) {
  auto ret = parse<long long>(reply);

  if (ret == 1) {
    return true;
  } else if (ret == 0) {
    return false;
  } else {
    throw ProtoError("[AsterHiredis] Invalid bool reply: " +
                     std::to_string(ret));
  }
}

void parse(ParseTag<void>, const redisReply *const reply) {
  if (!is_status(reply)) {
    throw ProtoError("[AsterHiredis] Expect STATUS reply");
  }

  if (reply->str == nullptr) {
    throw ProtoError("[AsterHiredis] A null status reply");
  }

  static const std::string OK = "OK";

  // Old version hiredis' *redisReply::len* is of type int.
  // So we have to cast it to an unsigned int.
  if (static_cast<std::size_t>(reply->len) != OK.size() ||
      OK.compare(0, OK.size(), reply->str, reply->len) != 0) {
    throw ProtoError("[AsterHiredis] NOT ok status reply: " +
                     reply::to_status(reply));
  }
}

}  // namespace reply
}  // namespace AsterHiredis