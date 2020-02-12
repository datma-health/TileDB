/**
 * @file codec_blosc.cc
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
 * This file implements codec for Blosc for compression and decompression.
 */

#ifdef ENABLE_BLOSC

#define BLOSC_EXTERN_DECL extern
#include "codec_blosc.h"

int CodecBlosc::compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size, bool delta_encode) {
  // Allocate space to store the compressed tile
#define BLOSC_MAX_OVERHEAD 16
  size_t compress_bound = tile_size + BLOSC_MAX_OVERHEAD;
  if(tile_compressed_ == NULL) {
    tile_compressed_allocated_size_ = compress_bound; 
    tile_compressed_ = malloc(compress_bound); 
  }

  // Expand comnpressed tile if necessary
  if(compress_bound > tile_compressed_allocated_size_) {
    tile_compressed_allocated_size_ = compress_bound; 
    tile_compressed_ = realloc(tile_compressed_, compress_bound);
  }

  // Initialize Blosc
  blosc_init();

  // Set the appropriate compressor
  if(blosc_set_compressor(compressor_.c_str()) < 0) {
    blosc_destroy();
    return print_errmsg("Failed to set Blosc compressor");
  } 

  // Compress tile
  int blosc_size = 
      blosc_compress(
          compression_level_, //clevel
          1, //doshuffle
          type_size_, //typesize
          tile_size, //nbytes
          tile, //src
          static_cast<unsigned char*>(tile_compressed_), //dest 
          tile_compressed_allocated_size_ //dest_size
                     );
  if(blosc_size < 0) {
    blosc_destroy();
    return print_errmsg("Failed compressing with Blosc");
  }

  *tile_compressed = tile_compressed_;
  tile_compressed_size = blosc_size;

  // Clean up
  blosc_destroy();

  // Success
  return TILEDB_CD_OK;
}

int CodecBlosc::decompress_tile(unsigned char* tile_compressed,  size_t tile_compressed_size, unsigned char* tile, size_t tile_size, bool delta_decode = false) {
  // Initialization
  blosc_init();

  // Decompress tile 
  if(blosc_decompress(
         (const char*) tile_compressed, 
         (char*) tile,
         tile_size) < 0) { 
    blosc_destroy();
    return print_errmsg("Blosc decompression failed");
  }

  // Clean up
  blosc_destroy();

  // Success
  return TILEDB_CD_OK;
}
#endif
