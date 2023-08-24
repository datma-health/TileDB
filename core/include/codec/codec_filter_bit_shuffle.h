/**
 * @file   codec_filter_bit_shuffle.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 Omics Data Automation, Inc.
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
 * This file defines the Bit Shuffle Pre-Compression Filter
 */

#ifndef __CODEC_BIT_SHUFFLE_H__
#define  __CODEC_BIT_SHUFFLE_H__

#include "codec_filter.h"
#include "tiledb_constants.h"

class CodecBitShuffle : public CodecFilter {
 public:
  using CodecFilter::CodecFilter;

  CodecBitShuffle(int type): CodecFilter(type, false) {
    filter_name_ = "Bit Shuffle";
    type_ = type;
  }
      
  /**
   * @return TILEDB_CDF_OK on success and TILEDB_CDF_ERR on error.
   */
  int code(unsigned char* tile, size_t tile_size) override;

  /**
   * @return TILEDB_CDF_OK on success and TILEDB_CDF_ERR on error.
   */
  int decode(unsigned char* tile_coded,  size_t tile_coded_size) override;
  
};

#endif /*  __CODEC_BIT_SHUFFLE_H__ */
