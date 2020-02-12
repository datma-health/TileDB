/**
 * @file codec_lz4.cc
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
 * This file implements codec for LZ4 for compression and decompression.
 */

#ifdef ENABLE_LZ4

#define LZ4_EXTERN_DECL extern
#include "codec_lz4.h"

int CodecLZ4::compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size, bool delta_encode) {
    // Allocate space to store the compressed tile
  size_t compress_bound = LZ4_compressBound(tile_size);
  if(tile_compressed_ == NULL) {
    tile_compressed_allocated_size_ = compress_bound; 
    tile_compressed_ = malloc(compress_bound); 
  }

  // Expand comnpressed tile if necessary
  if(compress_bound > tile_compressed_allocated_size_) {
    tile_compressed_allocated_size_ = compress_bound; 
    tile_compressed_ = realloc(tile_compressed_, compress_bound);
  }

  // Compress tile
  int lz4_size = LZ4_compress_default(
          (const char*) tile,
          (char*) tile_compressed_, 
          tile_size,
          compress_bound);
  if(lz4_size < 0) {
    return print_errmsg("Failed compressing with LZ4");
  }

  *tile_compressed = tile_compressed_;
  tile_compressed_size = lz4_size;

  // Success
  return TILEDB_CD_OK;
}

int CodecLZ4::decompress_tile(unsigned char* tile_compressed,  size_t tile_compressed_size, unsigned char* tile, size_t tile_sizee, bool delta_decode) {
    // Decompress tile 
  if(LZ4_decompress_safe(
         (const char*) tile_compressed, 
         (char*) tile,
         tile_compressed_size,
         tile_size) < 0) {
    return print_errmsg("LZ4 decompression failed");
  }

  // Success
  return TILEDB_CD_OK;
}

#endif
