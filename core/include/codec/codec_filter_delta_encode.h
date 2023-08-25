/**
 * @file   codec_filter_delta_encode.h
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
 * This file defines the Delta Encoder Pre-Compression Filter
 */

#ifndef __CODEC_DELTA_ENCODING_H__
#define  __CODEC_DELTA_ENCODING_H__

#include "codec_filter.h"
#include "tiledb_constants.h"

class CodecDeltaEncode : public CodecFilter {
 public:
  using CodecFilter::CodecFilter;

  CodecDeltaEncode(int type, int stride=1): CodecFilter(type, true) {
    filter_name_ = "Delta Encoding";
    stride_ = stride;
  }

  const int stride() {
    return stride_;
  }
      
  /**
   * @return TILEDB_CDF_OK on success and TILEDB_CDF_ERR on error.
   */
  int code(unsigned char* tile, size_t tile_size) override;

  /**
   * @return TILEDB_CDF_OK on success and TILEDB_CDF_ERR on error.
   */
  int decode(unsigned char* tile,  size_t tile_size) override;

 private:
  int stride_;
  
};

#endif /*  __CODEC_DELTA_ENCODING_H__ */
