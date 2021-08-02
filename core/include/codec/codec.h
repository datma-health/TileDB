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
  
  /* ********************************* */
  /*              MUTATORS             */
  /* ********************************* */

  // Clears old error conditions
  void clear_dlerror() {
    dl_error_ = std::string("");
    dlerror();
  }

  void set_dlerror() {
    char *errmsg = dlerror();
    if (errmsg) {
      dl_error_.empty()?(dl_error_=errmsg):(dl_error_+=std::string("\n")+errmsg);
    }
  }

  std::string& get_dlerror() {
    return dl_error_;
  }

 void *get_dlopen_handle(const std::string& name) {
    return get_dlopen_handle(name, "");
  }

  void *get_dlopen_handle(const std::string& name, const std::string& version) {
    void *handle;
    std::string prefix("lib");
#ifdef __APPLE__
    std::string suffix(".dylib");
#elif __linux__
    std::string suffix(".so");
#else
#  error Platform not supported
#endif
    
    clear_dlerror();
    for (std::string dl_path : dl_paths_) {
      std::string path = dl_path+prefix+name;
      if (version.empty()) {
        handle = dlopen((path+suffix).c_str(), RTLD_GLOBAL|RTLD_NOW);
      } else {
#ifdef __APPLE__
        handle = dlopen((path+"."+version+suffix).c_str(), RTLD_GLOBAL|RTLD_NOW);
#else
        handle = dlopen((path+suffix+"."+version).c_str(), RTLD_GLOBAL|RTLD_NOW);
#endif
      }
      if (handle) {
	clear_dlerror();
        return handle;
      } else {
	set_dlerror();
      }
    }

    return handle;
  }

#define BIND_SYMBOL(H, X, Y, Z)  \
  do {                           \
    clear_dlerror();             \
    X = Z dlsym(H, Y);           \
    if (!X) {                    \
      set_dlerror();             \
      throw std::system_error(ECANCELED, std::generic_category(), dl_error_); \
    }                            \
  } while (false)

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

  std::string dl_error_;
#ifdef __APPLE__
  std::vector<std::string> dl_paths_ = {"/usr/local/Cellar/lib/", "/usr/local/lib/", "/usr/lib/", ""};
#elif __linux__
  std::vector<std::string> dl_paths_ = {"/usr/lib64/", "/usr/lib/", ""};
#else
#  error Platform not supported
#endif
  
};

#endif /*__CODEC_H__*/
