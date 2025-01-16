/*
 * Copyright (c) 2009-2011, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2010-2014, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2015, Matt Stancliff <matt at genges dot com>,
 *                     Jan-Erik Rediger <janerik at fnordig dot com>
 * Copyright (c) 2022, Jia He <mofhejia@163.com>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/* When an error occurs, the err flag in a context is set to hold the type of
 * error that occurred. REDIS_ERR_IO means there was an I/O error and you
 * should use the "errno" variable to find out what is wrong.
 * For other values, the "errstr" field will hold a description. */
#pragma once

#include <stdio.h> /* for size_t */

#include <seastar/core/ragel.hh>
#include <seastar/core/seastar.hh>

#include "alloc.hh"
#include "reply.hh"

namespace AsterHiredis {
namespace ConnectionUnit {
namespace Reader {

/* Initial size of our nested reply stack and how much we grow it when needd */
static const int REDIS_READER_STACK_SIZE = 9;

/* Default multi-bulk element limit */
static const long long REDIS_READER_MAX_ARRAY_ELEMENTS = ((1LL << 32) - 1);

enum class RESP_PARSER_STATUS { ERR = -1, OK = 0, INCOMPLETE = 1 };

typedef struct redisReadTask {
  int type;
  long long elements;           /* number of elements in multibulk container */
  int idx;                      /* index in parent (array) object */
  void *obj;                    /* holds user-generated value for a read task */
  struct redisReadTask *parent; /* parent task */
  void *privdata;               /* user-settable arbitrary field */
} redisReadTask;

redisReply *createReplyObject(int type);
void *createStringObject(const redisReadTask *task, char *str, size_t len);
void *createArrayObject(const redisReadTask *task, size_t elements);
void *createIntegerObject(const redisReadTask *task, long long value);
void *createDoubleObject(const redisReadTask *task, double value, char *str,
                         size_t len);
void *createNilObject(const redisReadTask *task);
void *createBoolObject(const redisReadTask *task, int bval);

typedef struct redisReplyObjectFunctions {
  void *(*createString)(const redisReadTask *, char *, size_t);
  void *(*createArray)(const redisReadTask *, size_t);
  void *(*createInteger)(const redisReadTask *, long long);
  void *(*createDouble)(const redisReadTask *, double, char *, size_t);
  void *(*createNil)(const redisReadTask *);
  void *(*createBool)(const redisReadTask *, int);
  void (*freeObject)(void *);
} redisReplyObjectFunctions;

/* Default set of functions to build the reply. Keep in mind that such a
 * function returning NULL is interpreted as OOM. */
static redisReplyObjectFunctions defaultFunctions = {
  createStringObject, createArrayObject, createIntegerObject,
  createDoubleObject, createNilObject,   createBoolObject,
  freeReplyObject};

class reader_builder {
 public:
  sstring _value;
  const char *_start = nullptr;

 public:
  sstring get() && { return std::move(_value); }
  void reset() {
    _value = {};
    _start = nullptr;
  }
};

class redisReader {
 public:
  int err = 0;      /* Error flags, 0 when there is no error */
  char errstr[128]; /* String representation of error when applicable */

  reader_builder gather_builder; /* String builder for gathering bulk
                                        or line item across multiple packets */
  unsigned long res_bulk_len =
    0; /* Length of residual bulk item to be filled in bulk_builder */

  seastar::lw_shared_ptr<seastar::temporary_buffer<char>>
    raw_buf;                 /* Origin Seastar buffer */
  char *buf = NULL;          /* Read buffer */
  size_t pos = 0;            /* Buffer cursor */
  size_t len = 0;            /* Buffer length */
  long long maxelements = 0; /* Max multi-bulk elements */

  redisReadTask **task = NULL;
  int tasks = 0;

  int ridx = -1;      /* Index of current read task */
  void *reply = NULL; /* Temporary reply pointer */

  redisReplyObjectFunctions *fn = &defaultFunctions;
  void *privdata = NULL;

  redisReader(redisReplyObjectFunctions *fn = &defaultFunctions) {
    _builder.reset();
    raw_buf = seastar::make_lw_shared<seastar::temporary_buffer<char>>();
    buf = NULL;
    task = static_cast<redisReadTask **>(
      hi_calloc(REDIS_READER_STACK_SIZE, sizeof(*task)));
    if (task == NULL) goto oom;
    for (; tasks < REDIS_READER_STACK_SIZE; tasks++) {
      task[tasks] = static_cast<redisReadTask *>(hi_calloc(1, sizeof(**task)));
      if (task[tasks] == NULL) goto oom;
    }
    fn = fn;
    maxelements = REDIS_READER_MAX_ARRAY_ELEMENTS;
    ridx = -1;
    goto finish;

  oom:
    if (reply != NULL && fn && fn->freeObject) fn->freeObject(reply);
    if (task) {
      /* We know task[i] is allocated if i < tasks */
      for (int i = 0; i < tasks; i++) {
        hi_free(task[i]);
      }
      hi_free(task);
    }
    buf = NULL;
    throw std::bad_alloc();
  finish:
    NULL;
  }

  ~redisReader() {
    if (reply != NULL && fn && fn->freeObject) fn->freeObject(reply);
    if (task) {
      /* We know task[i] is allocated if i < tasks */
      for (int i = 0; i < tasks; i++) {
        hi_free(task[i]);
      }
      hi_free(task);
    }
    buf = NULL;
  }
};

static void __redisReaderSetError(redisReader *r, REDIS_ERR type,
                                  const char *str) {
  size_t len;

  if (r->reply != NULL && r->fn && r->fn->freeObject) {
    r->fn->freeObject(r->reply);
    r->reply = NULL;
  }

  /* Clear input buffer on errors. */
  r->raw_buf = seastar::make_lw_shared<seastar::temporary_buffer<char>>();
  r->buf = NULL;
  r->pos = r->len = 0;

  /* Reset task stack. */
  r->ridx = -1;

  /* Set error. */
  r->err = static_cast<std::underlying_type<REDIS_ERR>::type>(type);
  len = strlen(str);
  len = len < (sizeof(r->errstr) - 1) ? len : (sizeof(r->errstr) - 1);
  memcpy(r->errstr, str, len);
  r->errstr[len] = '\0';
}

/* Public API for the protocol parser. */
redisReader *redisReaderCreateWithFunctions(
  redisReplyObjectFunctions *fn = &defaultFunctions);
void redisReaderFree(redisReader *r);
template <typename STRING>
REDIS_STATUS redisReaderFeed(redisReader *r, STRING &in_buf) {
  /* Return early when this reader is in an erroneous state. */
  if (r->err) return REDIS_STATUS::ERR;
  /* Copy the provided buffer. */
  if (!in_buf.empty()) {
    /* Destroy internal buffer when it is exist. */
    if constexpr (std::is_same<STRING,
                               seastar::temporary_buffer<char>>::value) {
      r->raw_buf = seastar::make_lw_shared<seastar::temporary_buffer<char>>(
        std::move(in_buf));
    } else if constexpr (std::is_same<STRING, seastar::sstring>::value) {
      r->raw_buf = seastar::make_lw_shared<seastar::temporary_buffer<char>>(
        std::move(in_buf.release()));
    } else {
      r->raw_buf = seastar::make_lw_shared<seastar::temporary_buffer<char>>(
        in_buf.begin(), in_buf.size());
    }
  }
  r->buf = const_cast<char *>(r->raw_buf->begin());
  if (!(r->buf)) goto oom;
  r->len = r->raw_buf->size();
  return REDIS_STATUS::OK;
oom:
  __redisReaderSetError(r, REDIS_ERR::OTHER,
                        "Unreachable std::string buffer when redisReaderFeed.");
  return REDIS_STATUS::ERR;
}
REDIS_STATUS redisReaderGetReply(redisReader *r, void **reply);

static inline bool redisIsPushReply(redisReply *reply) {
  return (reply->type == REDIS_REPLY_PUSH);
}

static inline bool redisIsMessageReply(redisReply *reply) {
  const char *str = reply->element[0]->str;
  const size_t len = reply->element[0]->len;
  /* We will always have at least one string with the subscribe/message type */
  if (reply->elements < 1 || reply->element[0]->type != REDIS_REPLY_STRING ||
      len < sizeof("message") - 1) {
    return false;
  }
  return !strncasecmp(str, "message", len);
}

static inline bool redisIsSubscribeReply(redisReply *reply) {
  char *str;
  size_t len, off;
  /* We will always have at least one string with the subscribe/message type */
  if (reply->elements < 1 || reply->element[0]->type != REDIS_REPLY_STRING ||
      reply->element[0]->len < sizeof("message") - 1) {
    return 0;
  }
  /* Get the string/len moving past 'p' if needed */
  off = tolower(reply->element[0]->str[0]) == 'p';
  str = reply->element[0]->str + off;
  len = reply->element[0]->len - off;
  return !strncasecmp(str, "subscribe", len) ||
         !strncasecmp(str, "message", len) ||
         !strncasecmp(str, "unsubscribe", len);
}

static inline void redisReaderSetPrivdata(void *_r, void *_p) {
  static_cast<redisReader *>(_r)->privdata = _p;
}

static inline void *redisReaderGetObject(void *_r) {
  return static_cast<redisReader *>(_r)->reply;
}

static inline auto redisReaderGetError(void *_r) -> char * {
  return static_cast<redisReader *>(_r)->errstr;
}

}  // namespace Reader
}  // namespace ConnectionUnit
}  // namespace AsterHiredis