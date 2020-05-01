/**
 * @file codec_zstd.h
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
 * CodecZstd derived from Codec for Zstandard support
 *
 */

#ifndef __CODEC_ZSTD_H__
#define  __CODEC_ZSTD_H__

#include "codec.h"

// Function Pointers for ZStd
#if !defined(ZSTD_EXTERN_DECL)
#  define ZSTD_EXTERN_DECL
#endif

ZSTD_EXTERN_DECL size_t(*ZSTD_compressBound)(size_t);
ZSTD_EXTERN_DECL int(*ZSTD_isError)(size_t);
ZSTD_EXTERN_DECL int(*ZSTD_maxCLevel)(void);
ZSTD_EXTERN_DECL size_t(*ZSTD_compress)(void *, size_t, const void *, size_t, int);
ZSTD_EXTERN_DECL size_t(*ZSTD_decompress)(void *, size_t, const void *, size_t);

class CodecZStandard : public Codec {
 public:

  CodecZStandard(int compression_level):Codec(compression_level) {
    static bool loaded = false;
    static std::mutex loading;

     if (!loaded) {
      loading.lock();

      if (!loaded) {
        dl_handle_ = get_dlopen_handle("zstd");
        if (dl_handle_ != NULL) {
	  BIND_SYMBOL(dl_handle_, ZSTD_compressBound, "ZSTD_compressBound", (size_t(*)(size_t)));
	  BIND_SYMBOL(dl_handle_, ZSTD_isError, "ZSTD_isError", (int(*)(size_t)));
	  BIND_SYMBOL(dl_handle_, ZSTD_maxCLevel, "ZSTD_maxCLevel", (int(*)(void)));
	  BIND_SYMBOL(dl_handle_, ZSTD_compress, "ZSTD_compress", (size_t(*)(void *, size_t, const void *, size_t, int)));
	  BIND_SYMBOL(dl_handle_, ZSTD_decompress, "ZSTD_decompress", (size_t(*)(void *, size_t, const void *, size_t)));
	  loaded = true;
	}
      }

      loading.unlock();

      if (dl_handle_ == NULL || !loaded) {
	  if (dl_handle_ == NULL) {
	    char *error = dlerror();
	    if (error) {
	      std::cerr << dlerror() << std::endl << std::flush;
	    }
	  }
	  throw std::system_error(ECANCELED, std::generic_category(), "ZStd library not found. Install ZStandard and setup library paths.");
        }
     }
  }
  
  int do_compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size) override;

  int do_decompress_tile(unsigned char* tile_compressed,  size_t tile_compressed_size, unsigned char* tile, size_t tile_size) override;
  
};

#endif /*__CODEC_ZSTD_H__*/
