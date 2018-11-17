/**
 * @file   test_buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 Omics Data Automation, Inc.
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
 * Tests for the Buffer class
 */

#include "catch.h"
#include "buffer.h"

void alloc(void **bytes, int64_t size, int value) {
  *bytes = malloc(size);
  memset(*bytes, value, size);
}


TEST_CASE("Test Buffer Management", "[buffer]") {
  void *bytes;
  int64_t size = 50;
  alloc(&bytes, size, 0);

  void *read_bytes = NULL;
  
  Buffer buffer;
  CHECK(buffer.get_buffer() == NULL);
  CHECK(buffer.get_buffer_size() == 0);
  CHECK(buffer.read_buffer(0, NULL, 0) == TILEDB_BF_ERR);
  CHECK(buffer.read_buffer(0, read_bytes, 0) == TILEDB_BF_ERR);
  CHECK(buffer.append_buffer(bytes, 0) == TILEDB_BF_OK);
  CHECK(buffer.append_buffer(NULL, size) == TILEDB_BF_ERR);
  CHECK(buffer.append_buffer(bytes, size) == TILEDB_BF_OK);
  CHECK(buffer.get_buffer() != NULL);
  CHECK(buffer.get_buffer_size() == size);
  CHECK(buffer.append_buffer(bytes, size) == TILEDB_BF_OK);
  CHECK(buffer.get_buffer() != NULL);
  CHECK(buffer.get_buffer_size() == 2*size);
  buffer.free_buffer();
  free(bytes);

  // Test Constructor with bytes specified
  size = 10;
  alloc(&bytes, size, 2);

  Buffer buffer1(bytes, size);

  CHECK(buffer1.get_buffer() == bytes);
  CHECK(buffer1.get_buffer_size() == size);
  CHECK(buffer1.read_buffer(0, NULL, 0) == TILEDB_BF_ERR);

  read_bytes = malloc(11);
  CHECK(buffer1.read_buffer(0, read_bytes, 0) == TILEDB_BF_OK);
  CHECK(buffer1.read_buffer(0, read_bytes, 11) == TILEDB_BF_ERR);
  CHECK(buffer1.read_buffer(0, read_bytes, 5) == TILEDB_BF_OK);
  CHECK(*reinterpret_cast<char *>(read_bytes) == 2);
  CHECK(buffer1.read_buffer(5, read_bytes, 5) == TILEDB_BF_OK);
  CHECK(*reinterpret_cast<char *>(read_bytes) == 2);
  CHECK(buffer1.read_buffer(10, read_bytes, 5) == TILEDB_BF_ERR);

  CHECK(buffer1.append_buffer(bytes, size) == TILEDB_BF_ERR);
  buffer1.free_buffer();
}



