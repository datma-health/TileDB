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

class TestDeltaEncoding {
 public:
  template<typename T>
  size_t create_buffer(T** buffer, int stride, int length) {
    size_t sz = sizeof(T)*length;
    *buffer = reinterpret_cast<T *>(malloc(sz));
    for (auto i=0l; i<length/stride; i++) {
      for (auto j=0l; j<stride; j++) {
        (*buffer)[i*stride+j] = i;
      }
    }
    return sz;
  }

  template<typename T>
  void check_buffer(T* buffer, int stride, int length, bool is_encode) {
    for (auto i=0l; i<length/stride; i++) {
      for (auto j=0l; j<stride; j++) {
        if (is_encode && i>0) {
          CHECK(buffer[i*stride+j] == 1);
        } else {
          CHECK(buffer[i*stride+j] == i);
        }
      }
    }
  }

  template<typename T>
  void test_filter(CodecDeltaEncode* codec_filter, T** buffer, int stride, int length) {
    size_t sz = create_buffer(buffer, stride, length); 
    CHECK(sz == sizeof(T)*length);
    CHECK(codec_filter->code(reinterpret_cast<unsigned char*>(*buffer), sz) == 0);
    check_buffer(*buffer, stride, length, true);
    CHECK(codec_filter->decode(reinterpret_cast<unsigned char*>(*buffer), sz) == 0);
    check_buffer(*buffer, stride, length, false);

    // code/decode should fail if length is not divisible by stride
    if ((sz-1) % stride != 0) {
      CHECK(codec_filter->code(reinterpret_cast<unsigned char*>(*buffer), sz-1) != 0);
      CHECK(codec_filter->decode(reinterpret_cast<unsigned char*>(*buffer), sz-1) != 0);
    }
  }

  void test_filter(int type, int stride, int length) {
    CodecDeltaEncode* codec_filter = new CodecDeltaEncode(type, stride);
    CHECK(codec_filter->name() == "Delta Encoding");
    CHECK(codec_filter->in_place());
    CHECK(codec_filter->stride() == stride);
    CHECK(codec_filter->type() == type);

    unsigned char* buffer = 0;
    switch (type) {
      case TILEDB_INT32:
        test_filter(codec_filter, reinterpret_cast<int32_t **>(&buffer), stride, length);
        break;
      case TILEDB_INT64:
        test_filter(codec_filter, reinterpret_cast<int64_t **>(&buffer), stride, length);
        break;
      case TILEDB_UINT32:
        test_filter(codec_filter, reinterpret_cast<uint32_t **>(&buffer), stride, length);
        break;
      case TILEDB_UINT64:
        test_filter(codec_filter, reinterpret_cast<uint64_t **>(&buffer), stride, length);
        break;
    }

    free(buffer);
    delete codec_filter;
  }


  TestDeltaEncoding() {};
  
};
  
TEST_CASE_METHOD(TestDeltaEncoding, "Test delta encoding", "[codec_filter_delta_encoding]") {
  test_filter(TILEDB_INT64, 1, 10);
  test_filter(TILEDB_INT32, 2, 20);
  test_filter(TILEDB_UINT32, 3, 9);
  test_filter(TILEDB_UINT64, 4, 16);
}

