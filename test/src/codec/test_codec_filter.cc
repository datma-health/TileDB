/**
 * @file   test_codec_filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 Omics Data Automation, Inc.
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
 * Test pre-post compression filters
 */

#include "catch.h"
#include "codec_filter_delta_encode.h"
#include "tiledb_constants.h"

TEST_CASE("Test codec filter basic", "[codec_filter_basic]") {
  CodecFilter* codec_filter = new CodecFilter();
  CHECK(codec_filter->stride() == 1);
  CHECK(codec_filter->in_place());
  CHECK(codec_filter->name() == "");
  delete codec_filter;

  codec_filter = new CodecFilter(3);
  CHECK(codec_filter->stride() == 3);
  CHECK(codec_filter->in_place());
  CHECK(codec_filter->name() == "");
  delete codec_filter;

  codec_filter = new CodecFilter(5, false);
  CHECK(codec_filter->stride() == 5);
  CHECK(!codec_filter->in_place());
  CHECK(codec_filter->name() == "");
  size_t sz = 0;
  CHECK(codec_filter->code(NULL, 0, NULL, sz));
  CHECK(codec_filter->code(NULL, 0));
  CHECK(codec_filter->decode(NULL, 0, NULL, sz));
  CHECK(codec_filter->decode(NULL, 0));
  delete codec_filter;
}

TEST_CASE("Test delta encoding", "[codec_filter_delta_encoding]") {
  CodecDeltaEncode* codec_filter = new CodecDeltaEncode();
  CHECK(codec_filter->name() == "Delta Encoding");
  CHECK(codec_filter->in_place());
  CHECK(codec_filter->stride() == 1);
  CHECK(codec_filter->type() == TILEDB_UINT64);

  // Fill vector with 1's
  std::vector<uint64_t> vector(10, 1);
  CHECK(codec_filter->code(reinterpret_cast<unsigned char*>(vector.data()), 10*sizeof(int64_t)) == TILEDB_CDF_OK);
  CHECK(vector[0] == 1);
  for (auto i=1u; i<10; i++) {
    CHECK(vector[i] == 0);
  }
  CHECK(codec_filter->decode(reinterpret_cast<unsigned char*>(vector.data()), 10*sizeof(int64_t)) == TILEDB_CDF_OK);
  for (auto i=0u; i<10; i++) {
    CHECK(vector[i] == 1); // After decode they should match the original vector
  }

  // Fill vector with 1,2,3,4...
  for (auto i=0u; i<10; i++) {
    vector[i] = i+1;
  }
  CHECK(codec_filter->code(reinterpret_cast<unsigned char*>(vector.data()), 10*sizeof(int64_t)) == TILEDB_CDF_OK);
  for (auto i=0u; i<10; i++) {
    CHECK(vector[i] == 1);
  }
   CHECK(codec_filter->decode(reinterpret_cast<unsigned char*>(vector.data()), 10*sizeof(int64_t)) == TILEDB_CDF_OK);
  for (auto i=0u; i<10; i++) {
    CHECK(vector[i] == i+1);
  }
  delete codec_filter;

  codec_filter = new CodecDeltaEncode(TILEDB_UINT64, 3);
  CHECK(codec_filter->type() == TILEDB_UINT64);
  CHECK(codec_filter->stride() == 3);
  // This should fail as 10 is not divisible by 3;
  CHECK(codec_filter->code(reinterpret_cast<unsigned char*>(vector.data()), 10*sizeof(int64_t)) == TILEDB_CDF_ERR);
  delete codec_filter;

  codec_filter = new CodecDeltaEncode(TILEDB_INT32, 2);
  CHECK(codec_filter->stride() == 2);

  std::vector<int32_t> vector1(10);
  // Fill vector with 1,2,3,4...
  for (auto i=0u; i<10; i++) {
    vector1[i] = i+1;
  }
  CHECK(codec_filter->code(reinterpret_cast<unsigned char*>(vector1.data()), 10*sizeof(int64_t)) == TILEDB_CDF_OK);
  CHECK(vector1[0] == 1);
  for (auto i=1u; i<10; i++) {
    CHECK(vector1[i] == 2);
  }
  CHECK(codec_filter->decode(reinterpret_cast<unsigned char*>(vector1.data()), 10*sizeof(int64_t)) == TILEDB_CDF_OK);
  for (auto i=0u; i<10; i++) {
    CHECK(vector1[i] == (unsigned char)(i+1));
  }
  delete codec_filter;

  codec_filter = new CodecDeltaEncode(TILEDB_INT64, 1);
  CHECK(codec_filter->type() == TILEDB_INT64);
  delete codec_filter;

  codec_filter = new CodecDeltaEncode(TILEDB_UINT32, 1);
  CHECK(codec_filter->type() == TILEDB_UINT32);
  delete codec_filter;
  
}
