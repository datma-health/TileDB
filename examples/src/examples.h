/**
 * @file   examples.h
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2018 Omics Data Automation Inc.
 * @copyright Copyright (C) 2023 dātma, inc™
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
 * @section DESCRIPTION
 *
 * Examples Header File handling errors of TileDB api
 */

// TODO: move all examples to do error checking

#ifndef TILEDB_EXAMPLES_H
#define TILEDB_EXAMPLES_H

#include "tiledb.h"
#include "tiledb_storage.h"
#include <cstdio>
#include <string.h>

#define CHECK_RC(...)                                      \
do {                                                       \
  int rc = __VA_ARGS__;                                    \
  if (rc) {                                                \
    printf("%s", &tiledb_errmsg[0]);                       \
    printf("[Examples::%s] Runtime Error.\n", __FILE__);   \
    return rc;                                             \
  }                                                        \
} while (false)

#endif /* TILEDB_EXAMPLES_H */
