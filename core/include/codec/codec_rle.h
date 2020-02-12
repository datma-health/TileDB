/**
 * @file codec_rle.h
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
 * CodecGzip derived from Codec for RLE support
 *
 */

#ifndef __CODEC_RLE_H__
#define  __CODEC_RLE_H__

#include "codec.h"

class CodecRLE : public Codec {
 public:
  CodecRLE(int attribute_num, int dim_num, int cell_order, bool is_coords, size_t value_size):Codec(0){
    attribute_num_ = attribute_num;
    dim_num_ = dim_num;
    cell_order_ = cell_order;
    is_coords_ = is_coords;
    value_size_ = value_size;
  }
  
  int compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size, bool delta_encode = false);

  int decompress_tile(unsigned char* tile_compressed,  size_t tile_compressed_size, unsigned char* tile, size_t tile_size, bool delta_decode = false);

 private:
  int attribute_num_;
  int dim_num_;
  int cell_order_;
  bool is_coords_;
  size_t value_size_;
  
};

#endif /*__CODEC_RLE_H__*/
