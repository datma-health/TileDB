/**
 * @file   codec_filter.h
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
 * This file defines the base class for pre and post compression filters
 */

#ifndef __CODEC_FILTER_H__
#define __CODEC_FILTER_H__

#include <cassert>
#include <iostream>
#include <string>

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_CDF_OK        0
#define TILEDB_CDF_ERR      -1
/**@}*/

/* ****************************** */
/*             MACROS             */
/* ****************************** */

/** Default error message. */
#define TILEDB_CDF_ERRMSG std::string("[TileDB::CodecFilter] Error: ")

class CodecFilter {
 public:
   /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor. 
   *
   * @param compression_level
   */
  CodecFilter(int type, bool filter_in_place=true) {
    type_ = type;
    filter_in_place_ = filter_in_place;
  }
  
  virtual ~CodecFilter() {
    if (filter_buffer_ != NULL) {
      free(filter_buffer_);
    }
  }

  /**
   */
  const std::string& name() {
    return filter_name_;
  }

  const bool in_place() {
    return filter_in_place_;
  }

  int allocate_buffer(size_t size) {
    if (filter_buffer_ == NULL) {
      assert(filter_buffer_allocated_size_ == 0);
      filter_buffer_ = malloc(size);
      filter_buffer_allocated_size_ = size;
    } else if (filter_buffer_allocated_size_ < size) {
      filter_buffer_ = realloc(filter_buffer_, size);
      filter_buffer_allocated_size_ = size;
    }
    if (filter_buffer_ == NULL) {
      return TILEDB_CDF_ERR;
    } else {
      return TILEDB_CDF_OK;
    }
  }

  int type() {
    return type_;
  }

  unsigned char* buffer() {
    return reinterpret_cast<unsigned char*>(filter_buffer_);
  }

  virtual int code(unsigned char* tile, size_t tile_size, unsigned char* tile_coded, size_t& tile_coded_size) {
    return print_errmsg("virtual method should be overridden");
  }

  virtual int code(unsigned char *tile, size_t tile_size) {
    return print_errmsg("virtual method should be overridden");
  }

  virtual int decode(unsigned char* tile_coded,  size_t tile_coded_size, void** tile, size_t& tile_size) {
    return print_errmsg("virtual method should be overridden");
  }

  virtual int decode(unsigned char* tile_coded, size_t tile_coded_size) {
    return print_errmsg("virtual method should be overridden");
  }

  int print_errmsg(const std::string& msg);

 protected:
  std::string filter_name_ = "";
  bool filter_in_place_;

  int type_;

  /**
   * Internal buffer malloc'ed if necessary
   * Support only for pre compression filters for now
   */
  void* filter_buffer_ = NULL;
  size_t filter_buffer_allocated_size_ = 0;
};

#endif /* __CODEC_FILTER_H_ */
