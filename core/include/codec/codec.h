/**
 * @file   codec.h
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
 * This file defines the base class for compression and decompression.
 */

#ifndef __CODEC_H__
#define __CODEC_H__

#include "array_schema.h"

#include <errno.h>
#include <dlfcn.h>
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
  /*        STATIC METHODS         */
  /* ****************************** */
  
  static Codec* create(const ArraySchema* array_schema, const int attribute_id);

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
    if(tile_compressed_ != NULL) {
      free(tile_compressed_);
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
    dl_error_ = dlerror();
  }

  void *get_dlopen_handle(const std::string& name) {
    clear_dlerror();

    const char *lib_name;
#ifdef __APPLE__    
    lib_name = std::string("lib"+name+".dylib").c_str();
#elif __linux__
    lib_name = std::string("lib"+name+".so").c_str();
#else
#  error "This platform is currently not supported"
#endif
    void *handle = dlopen(lib_name, RTLD_NOLOAD|RTLD_NOW);
    if (!handle) {
      clear_dlerror();
      handle = dlopen(lib_name, RTLD_LOCAL|RTLD_NOW);
    }
    if (!handle) {
      dl_error_ = std::string(dlerror());
    }
    return handle;
  }

#define BIND_SYMBOL(H, X, Y, Z)  clear_dlerror(); X = Z dlsym(H, Y); if (!X) { set_dlerror(); throw std::system_error(ECANCELED, std::generic_category(), dl_error_); }

  /**
   * @return TILEDB_CD_OK on success and TILEDB_CD_ERR on error.
   */
  virtual int compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size) = 0;

  /**
   * @return TILEDB_CD_OK on success and TILEDB_CD_ERR on error.
   */
  virtual int decompress_tile(unsigned char* tile_compressed,  size_t tile_compressed_size, unsigned char* tile, size_t tile_size) = 0;

  /* ********************************* */
  /*         PROTECTED ATTRIBUTES      */
  /* ********************************* */
 protected:
  int compression_level_;

  /** Internal buffer used in the case of compression. */
  void* tile_compressed_ = NULL;
  /** Allocated size for internal buffer used in the case of compression. */
  size_t tile_compressed_allocated_size_ = 0;
  void *dl_handle_ = NULL;
  std::string dl_error_;

};

#endif /*__CODEC_H__*/
