/**
 * @file codec_rle.cc
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
 * This file implements codec for RLE for compression and decompression.
 */

#include "codec_rle.h"
#include "utils.h"

int CodecRLE::do_compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size) {
  // Allocate space to store the compressed tile
  size_t compress_bound;
  if(!is_coords_)
    compress_bound = RLE_compress_bound(tile_size, value_size_);
  else
    compress_bound = RLE_compress_bound_coords(tile_size, value_size_, dim_num_);
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
  int64_t rle_size;
  if(!is_coords_) { 
    rle_size = RLE_compress(
                  tile, 
                  tile_size,
                  (unsigned char*) tile_compressed_, 
                  tile_compressed_allocated_size_,
                  value_size_);
  } else {
    if(cell_order_ == TILEDB_ROW_MAJOR) {
        rle_size = RLE_compress_coords_row(
                       tile, 
                       tile_size,
                       (unsigned char*) tile_compressed_, 
                       tile_compressed_allocated_size_,
                       value_size_,
                       dim_num_);
    } else if(cell_order_ == TILEDB_COL_MAJOR) {
        rle_size = RLE_compress_coords_col(
                       tile, 
                       tile_size,
                       (unsigned char*) tile_compressed_, 
                       tile_compressed_allocated_size_,
                       value_size_,
                       dim_num_);
    } else { // Error
      return print_errmsg("Failed compressing with RLE; unsupported cell order");
    }
  }

  // Handle error
  if(rle_size == TILEDB_UT_ERR) {
    tiledb_cd_errmsg = tiledb_ut_errmsg;
    return TILEDB_CD_ERR;
  }

  // Set output
  *tile_compressed = tile_compressed_;
  tile_compressed_size = (size_t) rle_size;

  // Success
  return TILEDB_CD_OK;
}

int CodecRLE::do_decompress_tile(unsigned char* tile_compressed,  size_t tile_compressed_size, unsigned char* tile, size_t tile_size) {
   // Decompress tile
  int rc;
  if(!is_coords_) { 
    rc = RLE_decompress(
             (unsigned char*) tile_compressed,
             tile_compressed_size,
             tile, 
             tile_size,
             value_size_);
  } else {
    if(cell_order_ == TILEDB_ROW_MAJOR) {
      rc = RLE_decompress_coords_row(
               (unsigned char*) tile_compressed,
               tile_compressed_size,
               tile, 
               tile_size,
               value_size_,
               dim_num_);
    } else if(cell_order_ == TILEDB_COL_MAJOR) {
      rc = RLE_compress_coords_col(
               (unsigned char*) tile_compressed, 
               tile_compressed_size,
               tile, 
               tile_size,
               value_size_,
               dim_num_);
    } else { // Error
      return print_errmsg("Failed decompressing with RLE; unsupported cell order");
    }
  }

  // Handle error
  if(rc != TILEDB_UT_OK) {
    tiledb_cd_errmsg = tiledb_ut_errmsg;
    return TILEDB_CD_ERR;
  }

  // Success
  return TILEDB_CD_OK;
}
