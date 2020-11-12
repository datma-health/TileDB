/**
 * @file   error.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018,2020 Omics Data Automation, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION Common Error Handling
 *
 */

#ifndef TILEDB_ERROR_H
#define TILEDB_ERROR_H

#include <sys/types.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <string>

#ifdef TILEDB_VERBOSE
#  define PRINT_ERROR(x) std::cerr << x << std::endl
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif

#define SYSTEM_ERROR(PREFIX, MSG, PATH, TILEDB_MSG)                  \
  do {                                                               \
    std::string errmsg = PREFIX + "(" + __func__ + ") " + MSG;       \
    std::string errpath = PATH;                                      \
    if (errpath.length() > 0) {                                      \
      errmsg +=  " path=" + errpath;                                \
    }                                                                \
    if (errno > 0) {                                                 \
      errmsg += " errno=" + std::to_string(errno) + "(" + std::string(std::strerror(errno)) + ")"; \
    }                                                                \
    PRINT_ERROR(errmsg);                                             \
    TILEDB_MSG = errmsg;                                             \
  } while (false)

#define TILEDB_ERROR(PREFIX, MSG, TILEDB_MSG)                        \
  do {                                                               \
    std::string errmsg = PREFIX + "(" + __func__ + ") " + MSG;       \
    PRINT_ERROR(errmsg);                                             \
    TILEDB_MSG = errmsg;                                             \
  } while (false)

void reset_errno();

#endif /* TILEDB_ERROR_H */
