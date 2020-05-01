/**
 * @file   codec_filter_delta_encode.h
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
 * This file defines the Delta Encoder Pre-Compression Filter
 */

#ifndef __CODEC_DELTA_ENCODING_H__
#define  __CODEC_DELTA_ENCODING_H__

#include "codec_filter.h"
#include "tiledb_constants.h"

class CodecDeltaEncode : public CodecFilter {
 public:
  using CodecFilter::CodecFilter;

  CodecDeltaEncode(int type=TILEDB_UINT64, int filter_stride=1): CodecFilter(filter_stride, true) {
    filter_name_ = "Delta Encoding";
    type_ = type;
  }

  int type() {
    return type_;
  }
      
  /**
   * @return TILEDB_CD_OK on success and TILEDB_CD_ERR on error.
   */
  int code(unsigned char* tile, size_t tile_size) override;

  /**
   * @return TILEDB_CD_OK on success and TILEDB_CD_ERR on error.
   */
  int decode(unsigned char* tile_coded,  size_t tile_coded_size) override;

 private:
  int type_;
  
};

#endif /*  __CODEC_DELTA_ENCODING_H__ */
