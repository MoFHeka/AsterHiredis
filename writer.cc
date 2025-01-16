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

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <cstring>

#include "sds.hh"
#include "writer.hh"

namespace AsterHiredis {
namespace ConnectionUnit {
namespace Writer {

/* Format a command according to the Redis protocol. This function
 * takes a format similar to printf:
 *
 * %s represents a C null terminated string you want to interpolate
 * %b represents a binary safe string
 *
 * When using %b you need to provide both the pointer to the string
 * and the length in bytes as a size_t. Examples:
 *
 * len = redisFormatCommand(target, "GET %s", mykey);
 * len = redisFormatCommand(target, "SET %s %b", mykey, myval, myvallen);
 */
int redisFormatCommand(redisWriter *w, const char *source, ...) {
  va_list ap;
  int len;

  va_start(ap, source);
  len = redisFormatCommandChar(&(w->target), source, ap);
  va_end(ap);
  w->len = len;

  /* The API says "-1" means bad result, but we now also return "-2" in some
   * cases.  Force the return value to always be -1. */
  if (likely(len >= 0)) {
    w->tmp_buf = seastar::make_lw_shared<seastar::temporary_buffer<char>>(
      w->target, w->len, std::move(seastar::make_free_deleter(w->target)));
  } else {
    len = -1;
  }

  return len;
}

/* Format a command according to the Redis protocol. This function takes the
 * number of arguments, an array with arguments and an array with their
 * lengths. If the latter is set to NULL, strlen will be used to compute the
 * argument lengths.
 */
int redisFormatCommand(redisWriter *w, int argc, const char **argv,
                       const size_t *argvlen) {
  int len;

  len = redisFormatCommandArgv(&(w->target), argc, argv, argvlen);
  w->len = len;

  if (likely(len >= 0)) {
    w->tmp_buf = seastar::make_lw_shared<seastar::temporary_buffer<char>>(
      w->target, w->len, std::move(seastar::make_free_deleter(w->target)));
  } else {
    len = -1;
  }

  return len;
}

/* Return the number of digits of 'v' when converted to string in radix 10.
 * Implementation borrowed from link in redis/src/util.c:string2ll(). */
static uint32_t countDigits(uint64_t v) {
  uint32_t result = 1;
  for (;;) {
    if (v < 10) return result;
    if (v < 100) return result + 1;
    if (v < 1000) return result + 2;
    if (v < 10000) return result + 3;
    v /= 10000U;
    result += 4;
  }
}

/* Helper that calculates the bulk length given a certain string length. */
static size_t bulklen(size_t len) { return 1 + countDigits(len) + 2 + len + 2; }

int redisFormatCommandChar(char **target, const char *source, va_list ap) {
  const char *c = source;
  char *cmd = NULL;   /* final command */
  int pos;            /* position in final command */
  sds curarg, newarg; /* current argument */
  int touched = 0;    /* was the current argument touched? */
  char **curargv = NULL, **newargv = NULL;
  int argc = 0;
  int totlen = 0;
  int error_type = 0; /* 0 = no error; -1 = memory error; -2 = format error */
  int j;

  /* Abort if there is not target to set */
  if (target == NULL) return -1;

  /* Build the command string accordingly to protocol */
  curarg = sdsempty();
  if (curarg == NULL) return -1;

  while (*c != '\0') {
    if (*c != '%' || c[1] == '\0') {
      if (*c == ' ') {
        if (touched) {
          newargv = static_cast<char **>(
            hi_realloc(curargv, sizeof(char *) * (argc + 1)));
          if (newargv == NULL) goto memory_err;
          curargv = newargv;
          curargv[argc++] = curarg;
          totlen += bulklen(sdslen(curarg));

          /* curarg is put in argv so it can be overwritten. */
          curarg = sdsempty();
          if (curarg == NULL) goto memory_err;
          touched = 0;
        }
      } else {
        newarg = sdscatlen(curarg, c, 1);
        if (newarg == NULL) goto memory_err;
        curarg = newarg;
        touched = 1;
      }
    } else {
      char *arg;
      size_t size;

      /* Set newarg so it can be checked even if it is not touched. */
      newarg = curarg;

      switch (c[1]) {
      case 's':
        arg = va_arg(ap, char *);
        size = strlen(arg);
        if (size > 0) newarg = sdscatlen(curarg, arg, size);
        break;
      case 'b':
        arg = va_arg(ap, char *);
        size = va_arg(ap, size_t);
        if (size > 0) newarg = sdscatlen(curarg, arg, size);
        break;
      case '%':
        newarg = sdscat(curarg, "%");
        break;
      default:
        /* Try to detect printf format */
        {
          static const char intfmts[] = "diouxX";
          static const char flags[] = "#0-+ ";
          char _format[16];
          const char *_p = c + 1;
          size_t _l = 0;
          va_list _cpy;

          /* Flags */
          while (*_p != '\0' && strchr(flags, *_p) != NULL)
            _p++;

          /* Field width */
          while (*_p != '\0' && isdigit(*_p))
            _p++;

          /* Precision */
          if (*_p == '.') {
            _p++;
            while (*_p != '\0' && isdigit(*_p))
              _p++;
          }

          /* Copy va_list before consuming with va_arg */
          va_copy(_cpy, ap);

          /* Integer conversion (without modifiers) */
          if (strchr(intfmts, *_p) != NULL) {
            va_arg(ap, int);
            goto fmt_valid;
          }

          /* Double conversion (without modifiers) */
          if (strchr("eEfFgGaA", *_p) != NULL) {
            va_arg(ap, double);
            goto fmt_valid;
          }

          /* Size: char */
          if (_p[0] == 'h' && _p[1] == 'h') {
            _p += 2;
            if (*_p != '\0' && strchr(intfmts, *_p) != NULL) {
              va_arg(ap, int); /* char gets promoted to int */
              goto fmt_valid;
            }
            goto fmt_invalid;
          }

          /* Size: short */
          if (_p[0] == 'h') {
            _p += 1;
            if (*_p != '\0' && strchr(intfmts, *_p) != NULL) {
              va_arg(ap, int); /* short gets promoted to int */
              goto fmt_valid;
            }
            goto fmt_invalid;
          }

          /* Size: long long */
          if (_p[0] == 'l' && _p[1] == 'l') {
            _p += 2;
            if (*_p != '\0' && strchr(intfmts, *_p) != NULL) {
              va_arg(ap, long long);
              goto fmt_valid;
            }
            goto fmt_invalid;
          }

          /* Size: long */
          if (_p[0] == 'l') {
            _p += 1;
            if (*_p != '\0' && strchr(intfmts, *_p) != NULL) {
              va_arg(ap, long);
              goto fmt_valid;
            }
            goto fmt_invalid;
          }

        fmt_invalid:
          va_end(_cpy);
          goto format_err;

        fmt_valid:
          _l = (_p + 1) - c;
          if (_l < sizeof(_format) - 2) {
            memcpy(_format, c, _l);
            _format[_l] = '\0';
            newarg = sdscatvprintf(curarg, _format, _cpy);

            /* Update current position (note: outer blocks
             * increment c twice so compensate here) */
            c = _p - 1;
          }

          va_end(_cpy);
          break;
        }
      }

      if (newarg == NULL) goto memory_err;
      curarg = newarg;

      touched = 1;
      c++;
    }
    c++;
  }

  /* Add the last argument if needed */
  if (touched) {
    newargv =
      static_cast<char **>(hi_realloc(curargv, sizeof(char *) * (argc + 1)));
    if (newargv == NULL) goto memory_err;
    curargv = newargv;
    curargv[argc++] = curarg;
    totlen += bulklen(sdslen(curarg));
  } else {
    sdsfree(curarg);
  }

  /* Clear curarg because it was put in curargv or was free'd. */
  curarg = NULL;

  /* Add bytes needed to hold multi bulk count */
  totlen += 1 + countDigits(argc) + 2;

  /* Build the command at protocol level */
  cmd = static_cast<char *>(hi_malloc(totlen + 1));
  if (cmd == NULL) goto memory_err;

  pos = sprintf(cmd, "*%d\r\n", argc);
  for (j = 0; j < argc; j++) {
    pos += sprintf(cmd + pos, "$%zu\r\n", sdslen(curargv[j]));
    memcpy(cmd + pos, curargv[j], sdslen(curargv[j]));
    pos += sdslen(curargv[j]);
    sdsfree(curargv[j]);
    cmd[pos++] = '\r';
    cmd[pos++] = '\n';
  }
  assert(pos == totlen);
  cmd[pos] = '\0';

  hi_free(curargv);
  *target = cmd;
  return totlen;

format_err:
  error_type = -2;
  goto cleanup;

memory_err:
  error_type = -1;
  goto cleanup;

cleanup:
  if (curargv) {
    while (argc--)
      sdsfree(curargv[argc]);
    hi_free(curargv);
  }

  sdsfree(curarg);
  hi_free(cmd);

  return error_type;
}

int redisFormatCommandArgv(char **target, int argc, const char **argv,
                           const size_t *argvlen) {
  char *cmd = NULL; /* final command */
  int pos;          /* position in final command */
  size_t len;
  int totlen, j;

  /* Abort on a NULL target */
  if (target == NULL) return -1;

  /* Calculate number of bytes needed for the command */
  totlen = 1 + countDigits(argc) + 2;
  for (j = 0; j < argc; j++) {
    len = argvlen ? argvlen[j] : strlen(argv[j]);
    totlen += bulklen(len);
  }

  /* Build the command at protocol level */
  cmd = static_cast<char *>(hi_malloc(totlen + 1));
  if (cmd == NULL) return -1;

  pos = sprintf(cmd, "*%d\r\n", argc);
  for (j = 0; j < argc; j++) {
    len = argvlen ? argvlen[j] : strlen(argv[j]);
    pos += sprintf(cmd + pos, "$%zu\r\n", len);
    memcpy(cmd + pos, argv[j], len);
    pos += len;
    cmd[pos++] = '\r';
    cmd[pos++] = '\n';
  }
  assert(pos == totlen);
  cmd[pos] = '\0';

  *target = cmd;
  return totlen;
}

}  // namespace Writer
}  // namespace ConnectionUnit
}  // namespace AsterHiredis