/**
 * @file codec_zstd.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2020 Omics Data Automation, Inc.
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
 * This file implements codec for Zstandard for compression and decompression.
 */

#ifdef ENABLE_ZSTD

#define ZSTD_EXTERN_DECL extern
#include "codec_zstd.h"

int CodecZStandard::do_compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size) {
   // Allocate space to store the compressed tile
  size_t compress_bound = ZSTD_compressBound(tile_size);
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
  size_t zstd_size = 
      ZSTD_compress(
           static_cast<unsigned char*>(tile_compressed_), 
          tile_compressed_allocated_size_,
          tile, 
          tile_size,
          compression_level_);
  if(ZSTD_isError(zstd_size)) {
    return print_errmsg("Failed compressing with Zstandard");
  }

  *tile_compressed = tile_compressed_;
  tile_compressed_size = zstd_size;

  // Success
  return TILEDB_CD_OK;
}

int CodecZStandard::do_decompress_tile(unsigned char* tile_compressed,  size_t tile_compressed_size, unsigned char* tile, size_t tile_size) {
    // Decompress tile 
  size_t zstd_size = 
      ZSTD_decompress(
          tile,
          tile_size,
          tile_compressed, 
          tile_compressed_size);
  if(ZSTD_isError(zstd_size)) {
    return print_errmsg("Zstandard decompression failed");
  }

  // Success
  return TILEDB_CD_OK;
}

#else

#endif /* ENABLE_ZSTD */
