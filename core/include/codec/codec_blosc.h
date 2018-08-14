/**
 * @file codec_blosc.h
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
 * CodecBlosc derived from Codec for Blosc support
 *
 */

#ifndef __CODEC_BLOSC_H__
#define  __CODEC_BLOSC_H__

#include "codec.h"

// Function Pointers for blosc
#if !defined(BLOSC_EXTERN_DECL)
#  define BLOSC_EXTERN_DECL
#endif

BLOSC_EXTERN_DECL void (*blosc_init)();
BLOSC_EXTERN_DECL void (*blosc_destroy)();
BLOSC_EXTERN_DECL int (*blosc_set_compressor)(const char *);
BLOSC_EXTERN_DECL int (*blosc_compress)(int, int, size_t, size_t, const void *, void *, size_t);
BLOSC_EXTERN_DECL int (*blosc_decompress)(const void *, void *, size_t);

class CodecBlosc : public Codec {
 public:

  CodecBlosc(int compression_level, std::string compressor, size_t type_size):Codec(compression_level) {
    static bool loaded = false;
    static std::mutex loading;
    
    compressor_ = compressor;
    type_size_ = type_size;

    if (!loaded) {
      loading.lock();

      if (!loaded) {
        dl_handle_ = get_dlopen_handle("blosc");
        if (dl_handle_ == NULL) {
          throw std::system_error(ECANCELED, std::generic_category(), "Blosc library not found. Install Blosc and setup library paths.");
        }

        BIND_SYMBOL(dl_handle_, blosc_init, "blosc_init", (void (*)()));
        BIND_SYMBOL(dl_handle_, blosc_destroy, "blosc_destroy", (void (*)()));
        BIND_SYMBOL(dl_handle_, blosc_set_compressor, "blosc_set_compressor", (int (*)(const char *)));
        BIND_SYMBOL(dl_handle_, blosc_compress, "blosc_compress", (int (*)(int, int, size_t, size_t, const void *, void *, size_t)));
        BIND_SYMBOL(dl_handle_, blosc_decompress, "blosc_decompress", (int (*)(const void *, void *, size_t)));
        
        loading.unlock();
        loaded = true;
      } else {
        // TODO: throw exception
      }
    }
  }
  
  int compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size);

  int decompress_tile(unsigned char* tile_compressed,  size_t tile_compressed_size, unsigned char* tile, size_t tile_size);

 private:
  std::string compressor_;
  size_t type_size_;
  
};

#endif /*__CODEC_BLOSC_H__*/
