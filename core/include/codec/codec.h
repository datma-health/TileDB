/**
 * @file   codec.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 Omics Data Automation, Inc.
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
 * This file defines the base class for compression and decompression.
 */

#ifndef __CODEC_H__
#define __CODEC_H__

#include "array_schema.h"
#include "codec_filter.h"

#include <errno.h>
#include <dlfcn.h>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <system_error>

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_CD_OK        0
#define TILEDB_CD_ERR      -1
/**@}*/

/** Default error message. */
#define TILEDB_CD_ERRMSG std::string("[TileDB::Codec] Error: ")

/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Stores potential error messages. */
extern std::string tiledb_cd_errmsg;

/** Stores the state necessary when writing cells to a fragment. */
class Codec {
 public:

  /* ****************************** */
  /*        STATIC METHODS          */
  /* ****************************** */

  typedef std::function<Codec*(const ArraySchema*, const int, const bool)> create_fn_t;

  static int register_codec(int compression_type, Codec::create_fn_t create_fn);

  static bool is_registered_codec(int compression_type);

  static Codec::create_fn_t get_registered_codec(int compression_type);

  static Codec* create(const ArraySchema* array_schema, const int attribute_id, const bool is_offsets_compression=false);

  // Generalized non-TileDB codec usage
  static int create(void **handle, int compression_type, int compression_level);

  static int get_default_level(const int compression_type);

  static int print_errmsg(const std::string& msg);
  
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor. 
   *
   * @param compression_level
   */
  Codec(int compression_level) {
    compression_level_ = compression_level;
  }

  /**
   * Destructor
   */
  virtual ~Codec() {
    if (tile_compressed_ != NULL) {
      free(tile_compressed_);
    }    if (pre_compression_filter_) {
      delete pre_compression_filter_;
    }
    if (post_compression_filter_) {
      delete post_compression_filter_;
    }
  }
  
  /**
   */
  const std::string& name() {
    return name_;
  }

  int compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size);

  int decompress_tile(unsigned char* tile_compressed,  size_t tile_compressed_size, unsigned char* tile, size_t tile_size);

  /**
   * @return TILEDB_CD_OK on success and TILEDB_CD_ERR on error.
   */
  virtual int do_compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size) = 0;

  /**
   * @return TILEDB_CD_OK on success and TILEDB_CD_ERR on error.
   */
  virtual int do_decompress_tile(unsigned char* tile_compressed, size_t tile_compressed_size, unsigned char* tile, size_t tile_size) = 0;

  void set_pre_compression(CodecFilter* filter) {
    pre_compression_filter_ = filter;
  };

  void set_post_compression(CodecFilter* filter) {
    post_compression_filter_ = filter;
  };

  /* ********************************* */
  /*         PROTECTED ATTRIBUTES      */
  /* ********************************* */
 protected:
  std::string name_ = "";
  int compression_level_;
  CodecFilter* pre_compression_filter_ = NULL;
  CodecFilter* post_compression_filter_ = NULL;

  /** Internal buffer used in the case of compression. */
  void* tile_compressed_ = NULL;
  /** Allocated size for internal buffer used in the case of compression. */
  size_t tile_compressed_allocated_size_ = 0;

};

#endif /*__CODEC_H__*/
