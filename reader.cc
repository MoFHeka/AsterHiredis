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

#include <climits>

#include "alloc.hh"
#include "reader.hh"

namespace AsterHiredis {
namespace ConnectionUnit {
namespace Reader {

/* Create a reply object */
redisReply *createReplyObject(int type) {
  redisReply *r = static_cast<redisReply *>(hi_calloc(1, sizeof(*r)));

  if (r == NULL) return NULL;

  r->type = type;
  return r;
}

void *createStringObject(const redisReadTask *task, char *str, size_t len) {
  redisReply *r, *parent;
  char *buf;

  r = createReplyObject(task->type);
  if (r == NULL) return NULL;

  assert(task->type == REDIS_REPLY_ERROR || task->type == REDIS_REPLY_STATUS ||
         task->type == REDIS_REPLY_STRING || task->type == REDIS_REPLY_VERB ||
         task->type == REDIS_REPLY_BIGNUM);

  /* Copy string value */
  if (task->type == REDIS_REPLY_VERB) {
    buf = static_cast<char *>(
      hi_malloc(len - 4 + 1)); /* Skip 4 bytes of verbatim type header. */
    if (buf == NULL) goto oom;

    memcpy(r->vtype, str, 3);
    r->vtype[3] = '\0';
    memcpy(buf, str + 4, len - 4);
    buf[len - 4] = '\0';
    r->len = len - 4;
  } else {
    buf = static_cast<char *>(hi_malloc(len + 1));
    if (buf == NULL) goto oom;

    memcpy(buf, str, len);
    buf[len] = '\0';
    r->len = len;
  }
  r->str = buf;

  if (task->parent) {
    parent = static_cast<redisReply *>(task->parent->obj);
    assert(parent->type == REDIS_REPLY_ARRAY ||
           parent->type == REDIS_REPLY_MAP || parent->type == REDIS_REPLY_SET ||
           parent->type == REDIS_REPLY_PUSH);
    parent->element[task->idx] = r;
  }
  return r;

oom:
  freeReplyObject(r);
  return NULL;
}

void *createArrayObject(const redisReadTask *task, size_t elements) {
  redisReply *r, *parent;

  r = createReplyObject(task->type);
  if (r == NULL) return NULL;

  if (elements > 0) {
    r->element =
      static_cast<redisReply **>(hi_calloc(elements, sizeof(redisReply *)));
    if (r->element == NULL) {
      freeReplyObject(r);
      return NULL;
    }
  }

  r->elements = elements;

  if (task->parent) {
    parent = static_cast<redisReply *>(task->parent->obj);
    assert(parent->type == REDIS_REPLY_ARRAY ||
           parent->type == REDIS_REPLY_MAP || parent->type == REDIS_REPLY_SET ||
           parent->type == REDIS_REPLY_PUSH);
    parent->element[task->idx] = r;
  }
  return r;
}

void *createIntegerObject(const redisReadTask *task, long long value) {
  redisReply *r, *parent;

  r = createReplyObject(REDIS_REPLY_INTEGER);
  if (r == NULL) return NULL;

  r->integer = value;

  if (task->parent) {
    parent = static_cast<redisReply *>(task->parent->obj);
    assert(parent->type == REDIS_REPLY_ARRAY ||
           parent->type == REDIS_REPLY_MAP || parent->type == REDIS_REPLY_SET ||
           parent->type == REDIS_REPLY_PUSH);
    parent->element[task->idx] = r;
  }
  return r;
}

void *createDoubleObject(const redisReadTask *task, double value, char *str,
                         size_t len) {
  redisReply *r, *parent;

  r = createReplyObject(REDIS_REPLY_DOUBLE);
  if (r == NULL) return NULL;

  r->dval = value;
  r->str = static_cast<char *>(hi_malloc(len + 1));
  if (r->str == NULL) {
    freeReplyObject(r);
    return NULL;
  }

  /* The double reply also has the original protocol string representing a
   * double as a null terminated string. This way the caller does not need
   * to format back for string conversion, especially since Redis does efforts
   * to make the string more human readable avoiding the calssical double
   * decimal string conversion artifacts. */
  memcpy(r->str, str, len);
  r->str[len] = '\0';
  r->len = len;

  if (task->parent) {
    parent = static_cast<redisReply *>(task->parent->obj);
    assert(parent->type == REDIS_REPLY_ARRAY ||
           parent->type == REDIS_REPLY_MAP || parent->type == REDIS_REPLY_SET ||
           parent->type == REDIS_REPLY_PUSH);
    parent->element[task->idx] = r;
  }
  return r;
}

void *createNilObject(const redisReadTask *task) {
  redisReply *r, *parent;

  r = createReplyObject(REDIS_REPLY_NIL);
  if (r == NULL) return NULL;

  if (task->parent) {
    parent = static_cast<redisReply *>(task->parent->obj);
    assert(parent->type == REDIS_REPLY_ARRAY ||
           parent->type == REDIS_REPLY_MAP || parent->type == REDIS_REPLY_SET ||
           parent->type == REDIS_REPLY_PUSH);
    parent->element[task->idx] = r;
  }
  return r;
}

void *createBoolObject(const redisReadTask *task, int bval) {
  redisReply *r, *parent;

  r = createReplyObject(REDIS_REPLY_BOOL);
  if (r == NULL) return NULL;

  r->integer = bval != 0;

  if (task->parent) {
    parent = static_cast<redisReply *>(task->parent->obj);
    assert(parent->type == REDIS_REPLY_ARRAY ||
           parent->type == REDIS_REPLY_MAP || parent->type == REDIS_REPLY_SET ||
           parent->type == REDIS_REPLY_PUSH);
    parent->element[task->idx] = r;
  }
  return r;
}

static size_t chrtos(char *buf, size_t size, char byte) {
  size_t len = 0;

  switch (byte) {
  case '\\':
  case '"':
    len = snprintf(buf, size, "\"\\%c\"", byte);
    break;
  case '\n':
    len = snprintf(buf, size, "\"\\n\"");
    break;
  case '\r':
    len = snprintf(buf, size, "\"\\r\"");
    break;
  case '\t':
    len = snprintf(buf, size, "\"\\t\"");
    break;
  case '\a':
    len = snprintf(buf, size, "\"\\a\"");
    break;
  case '\b':
    len = snprintf(buf, size, "\"\\b\"");
    break;
  default:
    if (isprint(byte))
      len = snprintf(buf, size, "\"%c\"", byte);
    else
      len = snprintf(buf, size, "\"\\x%02x\"", (unsigned char)byte);
    break;
  }

  return len;
}

static void __redisReaderSetErrorProtocolByte(redisReader *r, char byte) {
  char cbuf[8], sbuf[128];

  chrtos(cbuf, sizeof(cbuf), byte);
  snprintf(sbuf, sizeof(sbuf), "Protocol error, got %s as reply type byte",
           cbuf);
  __redisReaderSetError(r, REDIS_ERR::PROTOCOL, sbuf);
}

static void __redisReaderSetErrorOOM(redisReader *r) {
  __redisReaderSetError(r, REDIS_ERR::OOM, "Out of memory");
}

static char *readBytes(redisReader *r, unsigned int bytes) {
  char *p;
  if (r->len - r->pos >= bytes) {
    p = r->buf + r->pos;
    r->pos += bytes;
    return p;
  }
  return NULL;
}

/* Find pointer to \r\n. */
static char *seekNewline(char *s, size_t len) {
  char *ret;

  /* We cannot match with fewer than 2 bytes */
  if (len < 2) return NULL;

  /* Search up to len - 1 characters */
  len--;

  /* Look for the \r */
  while ((ret = static_cast<char *>(memchr(s, '\r', len))) != NULL) {
    if (ret[1] == '\n') {
      /* Found. */
      break;
    }
    /* Continue searching. */
    ret++;
    len -= ret - s;
    s = ret;
  }

  return ret;
}

/* Convert a string into a long long. Returns REDIS_OK if the string could be
 * parsed into a (non-overflowing) long long, REDIS_ERR otherwise. The value
 * will be set to the parsed value when appropriate.
 *
 * Note that this function demands that the string strictly represents
 * a long long: no spaces or other characters before or after the string
 * representing the number are accepted, nor zeroes at the start if not
 * for the string "0" representing the zero number.
 *
 * Because of its strictness, it is safe to use this function to check if
 * you can convert a string into a long long, and obtain back the string
 * from the number without any loss in the string representation. */
static REDIS_STATUS string2ll(const char *s, size_t slen, long long *value) {
  const char *p = s;
  size_t plen = 0;
  int negative = 0;
  unsigned long long v;

  if (plen == slen) return REDIS_STATUS::ERR;

  /* Special case: first and only digit is 0. */
  if (slen == 1 && p[0] == '0') {
    if (value != NULL) *value = 0;
    return REDIS_STATUS::OK;
  }

  if (p[0] == '-') {
    negative = 1;
    p++;
    plen++;

    /* Abort on only a negative sign. */
    if (plen == slen) return REDIS_STATUS::ERR;
  }

  /* First digit should be 1-9, otherwise the string should just be 0. */
  if (p[0] >= '1' && p[0] <= '9') {
    v = p[0] - '0';
    p++;
    plen++;
  } else if (p[0] == '0' && slen == 1) {
    *value = 0;
    return REDIS_STATUS::OK;
  } else {
    return REDIS_STATUS::ERR;
  }

  while (plen < slen && p[0] >= '0' && p[0] <= '9') {
    if (v > (ULLONG_MAX / 10)) /* Overflow. */
      return REDIS_STATUS::ERR;
    v *= 10;

    if (v > (ULLONG_MAX - (p[0] - '0'))) /* Overflow. */
      return REDIS_STATUS::ERR;
    v += p[0] - '0';

    p++;
    plen++;
  }

  /* Return if not all bytes were used. */
  if (plen < slen) return REDIS_STATUS::ERR;

  if (negative) {
    if (v > ((unsigned long long)(-(LLONG_MIN + 1)) + 1)) /* Overflow. */
      return REDIS_STATUS::ERR;
    if (value != NULL) *value = -v;
  } else {
    if (v > LLONG_MAX) /* Overflow. */
      return REDIS_STATUS::ERR;
    if (value != NULL) *value = v;
  }
  return REDIS_STATUS::OK;
}

static char *readLine(redisReader *r, int *_len) {
  char *p, *s;
  int len;

  p = r->buf + r->pos;
  s = seekNewline(p, (r->len - r->pos));
  if (s != NULL) {
    len = s - (r->buf + r->pos);
    r->pos += len + 2; /* skip \r\n */
    if (_len) *_len = len;
    return p;
  }
  return NULL;
}

static void moveToNextTask(redisReader *r) {
  redisReadTask *cur, *prv;
  while (r->ridx >= 0) {
    /* Return a.s.a.p. when the stack is now empty. */
    if (r->ridx == 0) {
      r->ridx--;
      return;
    }

    cur = r->task[r->ridx];
    prv = r->task[r->ridx - 1];
    assert(prv->type == REDIS_REPLY_ARRAY || prv->type == REDIS_REPLY_MAP ||
           prv->type == REDIS_REPLY_SET || prv->type == REDIS_REPLY_PUSH);
    if (cur->idx == prv->elements - 1) {
      r->ridx--;
    } else {
      /* Reset the type because the next item can be anything */
      assert(cur->idx < prv->elements);
      cur->type = -1;
      cur->elements = -1;
      cur->idx++;
      return;
    }
  }
}

static REDIS_STATUS processLineItem(redisReader *r) {
  redisReadTask *cur = r->task[r->ridx];
  void *obj;
  char *p;
  int len;

  if ((p = readLine(r, &len)) != NULL) {
    if (cur->type == REDIS_REPLY_INTEGER) {
      long long v;

      if (string2ll(p, len, &v) == REDIS_STATUS::ERR) {
        __redisReaderSetError(r, REDIS_ERR::PROTOCOL, "Bad integer value");
        return REDIS_STATUS::ERR;
      }

      if (r->fn && r->fn->createInteger) {
        obj = r->fn->createInteger(cur, v);
      } else {
        obj = (void *)REDIS_REPLY_INTEGER;
      }
    } else if (cur->type == REDIS_REPLY_DOUBLE) {
      char buf[326], *eptr;
      double d;

      if ((size_t)len >= sizeof(buf)) {
        __redisReaderSetError(r, REDIS_ERR::PROTOCOL,
                              "Double value is too large");
        return REDIS_STATUS::ERR;
      }

      memcpy(buf, p, len);
      buf[len] = '\0';

      if (len == 3 && strcasecmp(buf, "inf") == 0) {
        d = INFINITY; /* Positive infinite. */
      } else if (len == 4 && strcasecmp(buf, "-inf") == 0) {
        d = -INFINITY; /* Negative infinite. */
      } else {
        d = strtod((char *)buf, &eptr);
        /* RESP3 only allows "inf", "-inf", and finite values, while
         * strtod() allows other variations on infinity, NaN,
         * etc. We explicity handle our two allowed infinite cases
         * above, so strtod() should only result in finite values. */
        if (buf[0] == '\0' || eptr != &buf[len] || !std::isfinite(d)) {
          __redisReaderSetError(r, REDIS_ERR::PROTOCOL, "Bad double value");
          return REDIS_STATUS::ERR;
        }
      }

      if (r->fn && r->fn->createDouble) {
        obj = r->fn->createDouble(cur, d, buf, len);
      } else {
        obj = (void *)REDIS_REPLY_DOUBLE;
      }
    } else if (cur->type == REDIS_REPLY_NIL) {
      if (len != 0) {
        __redisReaderSetError(r, REDIS_ERR::PROTOCOL, "Bad nil value");
        return REDIS_STATUS::ERR;
      }

      if (r->fn && r->fn->createNil)
        obj = r->fn->createNil(cur);
      else
        obj = (void *)REDIS_REPLY_NIL;
    } else if (cur->type == REDIS_REPLY_BOOL) {
      int bval;

      if (len != 1 || !strchr("tTfF", p[0])) {
        __redisReaderSetError(r, REDIS_ERR::PROTOCOL, "Bad bool value");
        return REDIS_STATUS::ERR;
      }

      bval = p[0] == 't' || p[0] == 'T';
      if (r->fn && r->fn->createBool)
        obj = r->fn->createBool(cur, bval);
      else
        obj = (void *)REDIS_REPLY_BOOL;
    } else if (cur->type == REDIS_REPLY_BIGNUM) {
      /* Ensure all characters are decimal digits (with possible leading
       * minus sign). */
      for (int i = 0; i < len; i++) {
        /* XXX Consider: Allow leading '+'? Error on leading '0's? */
        if (i == 0 && p[0] == '-') continue;
        if (p[i] < '0' || p[i] > '9') {
          __redisReaderSetError(r, REDIS_ERR::PROTOCOL, "Bad bignum value");
          return REDIS_STATUS::ERR;
        }
      }
      if (r->fn && r->fn->createString)
        obj = r->fn->createString(cur, p, len);
      else
        obj = (void *)REDIS_REPLY_BIGNUM;
    } else {
      /* Type will be error or status. */
      for (int i = 0; i < len; i++) {
        if (p[i] == '\r' || p[i] == '\n') {
          __redisReaderSetError(r, REDIS_ERR::PROTOCOL,
                                "Bad simple string value");
          return REDIS_STATUS::ERR;
        }
      }
      if (r->fn && r->fn->createString)
        obj = r->fn->createString(cur, p, len);
      else
        obj = (void *)(size_t)(cur->type);
    }

    if (obj == NULL) {
      __redisReaderSetErrorOOM(r);
      return REDIS_STATUS::ERR;
    }

    /* Set reply if this is the root object. */
    if (r->ridx == 0) r->reply = obj;
    moveToNextTask(r);
    return REDIS_STATUS::OK;
  }

  if (likely(r->err == 0 && r->gather_builder._value.empty())) {
    // avoid an allocation in the common case
    r->gather_builder._value =
      sstring(r->gather_builder._start, r->buf + r->len);
  }

  return REDIS_STATUS::ERR;
}

static REDIS_STATUS processBulkItem(redisReader *r) {
  redisReadTask *cur = r->task[r->ridx];
  void *obj = NULL;
  char *p, *s;
  long long len;
  unsigned long bytelen;
  int success = 0;

  p = r->buf + r->pos;
  s = seekNewline(p, r->len - r->pos);
  if (s != NULL) {
    p = r->buf + r->pos;
    bytelen = s - (r->buf + r->pos) + 2; /* include \r\n */

    if (string2ll(p, bytelen - 2, &len) == REDIS_STATUS::ERR) {
      __redisReaderSetError(r, REDIS_ERR::PROTOCOL, "Bad bulk string length");
      return REDIS_STATUS::ERR;
    }

    if (len < -1 || (LLONG_MAX > SIZE_MAX && len > (long long)SIZE_MAX)) {
      __redisReaderSetError(r, REDIS_ERR::PROTOCOL,
                            "Bulk string length out of range");
      return REDIS_STATUS::ERR;
    }

    if (len == -1) {
      /* The nil object can always be created. */
      if (r->fn && r->fn->createNil)
        obj = r->fn->createNil(cur);
      else
        obj = (void *)REDIS_REPLY_NIL;
      success = 1;
    } else {
      /* Only continue when the buffer contains the entire bulk item. */
      bytelen += len + 2; /* include \r\n */
      if (r->pos + bytelen <= r->len) {
        if ((cur->type == REDIS_REPLY_VERB && len < 4) ||
            (cur->type == REDIS_REPLY_VERB && s[5] != ':')) {
          __redisReaderSetError(r, REDIS_ERR::PROTOCOL,
                                "Verbatim string 4 bytes of content type are "
                                "missing or incorrectly encoded.");
          return REDIS_STATUS::ERR;
        }
        if (r->fn && r->fn->createString)
          obj = r->fn->createString(cur, s + 2, len);
        else
          obj = (void *)(long)cur->type;
        success = 1;
      }
    }

    /* Proceed when obj was created. */
    if (success) {
      if (obj == NULL) {
        __redisReaderSetErrorOOM(r);
        return REDIS_STATUS::ERR;
      }

      r->pos += bytelen;

      /* Set reply if this is the root object. */
      if (r->ridx == 0) r->reply = obj;
      moveToNextTask(r);
      return REDIS_STATUS::OK;
    }
  }

  if (likely(r->err == 0 && len > 0 && !success &&
             r->gather_builder._value.empty())) {
    // avoid an allocation in the common case
    r->res_bulk_len = r->len - (r->pos + bytelen);
    r->gather_builder._value =
      sstring(r->gather_builder._start, r->buf + r->len);
  }

  return REDIS_STATUS::ERR;
}

static REDIS_STATUS redisReaderGrow(redisReader *r) {
  redisReadTask **aux;
  int newlen;

  /* Grow our stack size */
  newlen = r->tasks + REDIS_READER_STACK_SIZE;
  aux = static_cast<redisReadTask **>(
    hi_realloc(r->task, sizeof(*r->task) * newlen));
  if (aux == NULL) goto oom;

  r->task = aux;

  /* Allocate new tasks */
  for (; r->tasks < newlen; r->tasks++) {
    r->task[r->tasks] =
      static_cast<redisReadTask *>(hi_calloc(1, sizeof(**r->task)));
    if (r->task[r->tasks] == NULL) goto oom;
  }

  return REDIS_STATUS::OK;
oom:
  __redisReaderSetErrorOOM(r);
  return REDIS_STATUS::ERR;
}

/* Process the array, map and set types. */
static REDIS_STATUS processAggregateItem(redisReader *r) {
  redisReadTask *cur = r->task[r->ridx];
  void *obj;
  char *p;
  long long elements;
  int root = 0, len;

  if (r->ridx == r->tasks - 1) {
    if (redisReaderGrow(r) == REDIS_STATUS::ERR) return REDIS_STATUS::ERR;
  }

  if ((p = readLine(r, &len)) != NULL) {
    if (string2ll(p, len, &elements) == REDIS_STATUS::ERR) {
      __redisReaderSetError(r, REDIS_ERR::PROTOCOL, "Bad multi-bulk length");
      return REDIS_STATUS::ERR;
    }

    root = (r->ridx == 0);

    if (elements < -1 || (LLONG_MAX > SIZE_MAX && elements > SIZE_MAX) ||
        (r->maxelements > 0 && elements > r->maxelements)) {
      __redisReaderSetError(r, REDIS_ERR::PROTOCOL,
                            "Multi-bulk length out of range");
      return REDIS_STATUS::ERR;
    }

    if (elements == -1) {
      if (r->fn && r->fn->createNil)
        obj = r->fn->createNil(cur);
      else
        obj = (void *)REDIS_REPLY_NIL;

      if (obj == NULL) {
        __redisReaderSetErrorOOM(r);
        return REDIS_STATUS::ERR;
      }

      moveToNextTask(r);
    } else {
      if (cur->type == REDIS_REPLY_MAP) elements *= 2;

      if (r->fn && r->fn->createArray)
        obj = r->fn->createArray(cur, elements);
      else
        obj = (void *)(long)cur->type;

      if (obj == NULL) {
        __redisReaderSetErrorOOM(r);
        return REDIS_STATUS::ERR;
      }

      /* Modify task stack when there are more than 0 elements. */
      if (elements > 0) {
        cur->elements = elements;
        cur->obj = obj;
        r->ridx++;
        r->task[r->ridx]->type = -1;
        r->task[r->ridx]->elements = -1;
        r->task[r->ridx]->idx = 0;
        r->task[r->ridx]->obj = NULL;
        r->task[r->ridx]->parent = cur;
        r->task[r->ridx]->privdata = r->privdata;
      } else {
        moveToNextTask(r);
      }
    }

    /* Set reply if this is the root object. */
    if (root) r->reply = obj;
    return REDIS_STATUS::OK;
  }

  if (likely(r->err == 0 && r->gather_builder._value.empty())) {
    // avoid an allocation in the common case
    r->gather_builder._value =
      sstring(r->gather_builder._start, r->buf + r->len);
  }

  return REDIS_STATUS::ERR;
}

static REDIS_STATUS processItem(redisReader *r) {
  redisReadTask *cur = r->task[r->ridx];
  char *p;

  /* check if we need to read type */
  if (cur->type < 0) {
    if (likely((p = readBytes(r, 1)) != NULL)) {
      switch (p[0]) {
      case '-':
        cur->type = REDIS_REPLY_ERROR;
        break;
      case '+':
        cur->type = REDIS_REPLY_STATUS;
        break;
      case ':':
        cur->type = REDIS_REPLY_INTEGER;
        break;
      case ',':
        cur->type = REDIS_REPLY_DOUBLE;
        break;
      case '_':
        cur->type = REDIS_REPLY_NIL;
        break;
      case '$':
        cur->type = REDIS_REPLY_STRING;
        break;
      case '*':
        cur->type = REDIS_REPLY_ARRAY;
        break;
      case '%':
        cur->type = REDIS_REPLY_MAP;
        break;
      case '~':
        cur->type = REDIS_REPLY_SET;
        break;
      case '#':
        cur->type = REDIS_REPLY_BOOL;
        break;
      case '=':
        cur->type = REDIS_REPLY_VERB;
        break;
      case '>':
        cur->type = REDIS_REPLY_PUSH;
        break;
      case '(':
        cur->type = REDIS_REPLY_BIGNUM;
        break;
      default:
        __redisReaderSetErrorProtocolByte(r, *p);
        return REDIS_STATUS::ERR;
      }
    } else {
      /* could not consume 1 byte */
      return REDIS_STATUS::ERR;
    }
  }

  /* process typed item */
  switch (cur->type) {
  case REDIS_REPLY_ERROR:
  case REDIS_REPLY_STATUS:
  case REDIS_REPLY_INTEGER:
  case REDIS_REPLY_DOUBLE:
  case REDIS_REPLY_NIL:
  case REDIS_REPLY_BOOL:
  case REDIS_REPLY_BIGNUM:
    if (likely(r->gather_builder._value.empty())) {
      /* Record cursor when beginning for resuming */
      r->gather_builder._start = r->buf + r->pos;
    }
    return processLineItem(r);
  case REDIS_REPLY_STRING:
  case REDIS_REPLY_VERB:
    if (likely(r->gather_builder._value.empty())) {
      /* Record cursor when beginning for resuming */
      r->gather_builder._start = r->buf + r->pos;
    }
    return processBulkItem(r);
  case REDIS_REPLY_ARRAY:
  case REDIS_REPLY_MAP:
  case REDIS_REPLY_SET:
  case REDIS_REPLY_PUSH:
    if (likely(r->gather_builder._value.empty())) {
      /* Record cursor when beginning for resuming */
      r->gather_builder._start = r->buf + r->pos;
    }
    return processAggregateItem(r);
  default:
    assert(NULL);
    return REDIS_STATUS::ERR; /* Avoid warning. */
  }
}

redisReader *redisReaderCreateWithFunctions(redisReplyObjectFunctions *fn) {
  redisReader *r;

  r = static_cast<redisReader *>(hi_calloc(1, sizeof(redisReader)));
  if (r == NULL) return NULL;

  r->raw_buf = seastar::make_lw_shared<seastar::temporary_buffer<char>>();
  r->buf = NULL;

  r->task = static_cast<redisReadTask **>(
    hi_calloc(REDIS_READER_STACK_SIZE, sizeof(*r->task)));
  if (unlikely(r->task == NULL)) goto oom;

  for (; r->tasks < REDIS_READER_STACK_SIZE; r->tasks++) {
    r->task[r->tasks] =
      static_cast<redisReadTask *>(hi_calloc(1, sizeof(**r->task)));
    if (unlikely(r->task[r->tasks] == NULL)) goto oom;
  }

  r->fn = fn;
  r->maxelements = REDIS_READER_MAX_ARRAY_ELEMENTS;
  r->ridx = -1;

  return r;
oom:
  redisReaderFree(r);
  throw std::bad_alloc();
}

void redisReaderFree(redisReader *r) {
  if (r == NULL) return;

  if (r->reply != NULL && r->fn && r->fn->freeObject)
    r->fn->freeObject(r->reply);

  if (r->task) {
    /* We know r->task[i] is allocated if i < r->tasks */
    for (int i = 0; i < r->tasks; i++) {
      hi_free(r->task[i]);
    }

    hi_free(r->task);
  }

  r->buf = NULL;
  hi_free(r);
}

REDIS_STATUS redisReaderGetReply(redisReader *r, void **reply) {
  /* Default target pointer to NULL. */
  if (reply != NULL) *reply = NULL;

  /* Return early when this reader is in an erroneous state. */
  if (r->err) return REDIS_STATUS::ERR;

  /* When the buffer is empty, there will never be a reply. */
  if (r->len == 0) return REDIS_STATUS::OK;

  /* Set first item to process when the stack is empty. */
  if (r->ridx == -1) {
    r->task[0]->type = -1;
    r->task[0]->elements = -1;
    r->task[0]->idx = -1;
    r->task[0]->obj = NULL;
    r->task[0]->parent = NULL;
    r->task[0]->privdata = r->privdata;
    r->ridx = 0;
  }

  /* Process items in reply. */
  while (r->ridx >= 0)
    if (unlikely(processItem(r) != REDIS_OK)) break;

  /* Return ASAP when an error occurred. */
  if (r->err) return REDIS_STATUS::ERR;

  /* Emit a reply when there is one. */
  if (r->ridx == -1) {
    if (reply != NULL) {
      *reply = r->reply;
    } else if (r->reply != NULL && r->fn && r->fn->freeObject) {
      r->fn->freeObject(r->reply);
    }
    r->reply = NULL;
  }
  return REDIS_STATUS::OK;
}

}  // namespace Reader
}  // namespace ConnectionUnit
}  // namespace AsterHiredis