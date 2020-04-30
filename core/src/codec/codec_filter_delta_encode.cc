/**
 * @file   codec_filter_delta_encode.cc
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
 * This file implements the Delta Encoder Pre-Compression Filter.
 */

#include "codec_filter_delta_encode.h"
#include "tiledb_constants.h"

#include <vector>

template<typename T>
int do_code(T* tile, size_t tile_size_in_bytes, CodecFilter* filter) {
  int stride = filter->stride();
  size_t length = tile_size_in_bytes/sizeof(T)/stride;
  if (tile_size_in_bytes/sizeof(T)%stride != 0) {
    return filter->print_errmsg("Only tiles that are divisible by stride supported");
  }
  std::vector<T> last(stride, 0);
  for (size_t i=0; i<length; i++) {
    for (int j=0; j<stride; j++) {
      size_t index = i*stride+j;
      T current = tile[index];
      tile[index] = current - last[j];
      last[j] = current;
    }
  }
  return TILEDB_CDF_OK;
}

int CodecDeltaEncode::code(unsigned char* tile, size_t tile_size) {
  switch (type_) {
    case TILEDB_INT32:
      return do_code(reinterpret_cast<int32_t *>(tile), tile_size, this);
    case TILEDB_INT64:
      return do_code(reinterpret_cast<int64_t *>(tile), tile_size, this);
    case TILEDB_UINT32:
      return do_code(reinterpret_cast<uint32_t *>(tile), tile_size, this);
    case TILEDB_UINT64:
      return do_code(reinterpret_cast<uint64_t *>(tile), tile_size, this);
    default:
      return print_errmsg("CodecDeltaEncode not implemented for type");
  }
}

template<typename T>
int do_decode(T* tile, size_t tile_size_in_bytes, CodecFilter* filter) {
  int stride = filter->stride();
  size_t length = tile_size_in_bytes/sizeof(T)/stride;
  if (tile_size_in_bytes/sizeof(T)%stride != 0) {
    return filter->print_errmsg("Only tiles that are divisible by stride supported");
  }
  std::vector<T> last(stride, 0);
  for (size_t i = 0; i < length; i++) {
    for (int j=0; j<stride; j++) {
      size_t index = i*stride+j;
      T delta = tile[index];
      tile[index] = delta + last[j];
      last[j] = tile[index];
    }
  }
  return TILEDB_CDF_OK;
}

int CodecDeltaEncode::decode(unsigned char* tile, size_t tile_size) {
  switch (type_) {
    case TILEDB_INT32:
      return do_decode(reinterpret_cast<int32_t *>(tile), tile_size, this);
    case TILEDB_INT64:
      return do_decode(reinterpret_cast<int64_t *>(tile), tile_size, this);
    case TILEDB_UINT32:
      return do_decode(reinterpret_cast<uint32_t *>(tile), tile_size, this);
    case TILEDB_UINT64:
      return do_decode(reinterpret_cast<uint64_t *>(tile), tile_size, this);
    default:
      return print_errmsg("CodecDeltaEncode not implemented for type");
  }
}
