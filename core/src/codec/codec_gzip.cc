/**
 * @file codec_gzip.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 Omics Data Automation, Inc.
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
 * This file implements codec for gzip for compression and decompression.
 */

#include "codec_gzip.h"
#include "utils.h"

#include <math.h>

int CodecGzip::do_compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size) {
  // Allocate space to store the compressed tile
  if(tile_compressed_ == NULL) {
    tile_compressed_allocated_size_ = 
        tile_size + 6 + 5*(ceil(tile_size/16834.0));
    tile_compressed_ = malloc(tile_compressed_allocated_size_); 
  }

  // Expand compressed tile if necessary
  if(tile_size + 6 + 5*(ceil(tile_size/16834.0)) > 
     tile_compressed_allocated_size_) {
    tile_compressed_allocated_size_ = 
        tile_size + 6 + 5*(ceil(tile_size/16834.0));
    tile_compressed_ = 
        realloc(tile_compressed_, tile_compressed_allocated_size_);
  }

  if (tile_compressed_ == NULL) {
    return print_errmsg("OOM while trying to allocate memory for compress using " + name());
  }

  // Compress tile
  ssize_t gzip_size = 
    gzip(tile, tile_size, (unsigned char*)tile_compressed_, tile_compressed_allocated_size_, compression_level_);
  if(gzip_size == static_cast<ssize_t>(TILEDB_UT_ERR)) {
    tiledb_cd_errmsg = tiledb_ut_errmsg;
    return TILEDB_CD_ERR;
  }

  *tile_compressed = tile_compressed_;
  tile_compressed_size = (size_t) gzip_size;

  // Success
  return TILEDB_CD_OK;
}

int CodecGzip::do_decompress_tile(unsigned char* tile_compressed,  size_t tile_compressed_size, unsigned char* tile, size_t tile_size) {
  // Decompress tile 
  size_t gunzip_out_size;
  if(gunzip(
         tile_compressed, 
         tile_compressed_size, 
         tile,
         tile_size,
         gunzip_out_size) != TILEDB_UT_OK) {
    tiledb_cd_errmsg = tiledb_ut_errmsg;
    return TILEDB_CD_ERR;
  }

  // Success
  return TILEDB_CD_OK;
}
