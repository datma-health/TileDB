/**
 * @file   codec_filter_bit_shuffle.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 Omics Data Automation, Inc.
 * @copyright Copyright (c) 2023 dātma, inc™
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
 * This file implements the Bit Shuffle Pre-Compression Filter.
 */

#include "bitshuffle_core.h"
#include "codec_filter_bit_shuffle.h"
#include "tiledb_constants.h"

const std::string err_msg(int rc) {
  switch (rc) {
  case -1:
    return "Fail to allocate memory";
  case -11:
    return "Missing SSE";
  case -12:
    return "Missing AVX";
  case -80:
    return "Input size not a multiple of 8";
  case -81:
    return "Block Size not a multiple of 8";
  case -91:
    return "Decompression error, wrong number of bytes processed";
  default:
    return "Internal error";
  }
}

template<typename T>
int do_code(T* tile, size_t tile_size_in_bytes, CodecFilter* filter) {
  if (tile_size_in_bytes%sizeof(T) != 0) {
    return filter->print_errmsg("Tile size to pre-compression filter " + filter->name() + " should be a multiple of sizeof type");
  }

  if (filter->allocate_buffer(tile_size_in_bytes) == TILEDB_CDF_ERR) {
    return filter->print_errmsg("OOM while tring to allocate memory for filter " + filter->name());
  }
  
  int rc = bshuf_bitshuffle(tile, filter->buffer(), tile_size_in_bytes/sizeof(T), sizeof(T), 0);
  if (rc < 0) {
    return filter->print_errmsg("Bit shuffle error: " + err_msg(rc));
  }
      
  return TILEDB_CDF_OK;
}

int CodecBitShuffle::code(unsigned char* tile, size_t tile_size) {
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
      return print_errmsg("CodecBitShuffle not implemented for type");
  }
}

template<typename T>
int do_decode(T* tile, size_t tile_size_in_bytes, CodecFilter* filter) {
  if (tile_size_in_bytes%sizeof(T) != 0) {
    return filter->print_errmsg("Tile size to pre-compression filter " + filter->name() + " should be a multiple of sizeof type");
  }
  
  int rc = bshuf_bitunshuffle(filter->buffer(), tile, tile_size_in_bytes/sizeof(T), sizeof(T), 0);
  if (rc < 0) {
    return filter->print_errmsg("Bit unshuffle error: " + err_msg(rc));
  }
 
  return TILEDB_CDF_OK;
}

int CodecBitShuffle::decode(unsigned char* tile, size_t tile_size) {
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
      return print_errmsg("CodecBitShuffle not implemented for type");
  }
}
