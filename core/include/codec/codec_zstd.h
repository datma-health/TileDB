/**
 * @file codec_zstd.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 Omics Data Automation, Inc.
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
ZSTD_EXTERN_DECL char *(*ZSTD_getErrorName)(size_t);
ZSTD_EXTERN_DECL int(*ZSTD_maxCLevel)(void);
ZSTD_EXTERN_DECL size_t(*ZSTD_compress)(void *, size_t, const void *, size_t, int);
ZSTD_EXTERN_DECL size_t(*ZSTD_decompress)(void *, size_t, const void *, size_t);

ZSTD_EXTERN_DECL char *(*ZSTD_createCCtx)(void);
ZSTD_EXTERN_DECL size_t(*ZSTD_freeCCtx)(char *);
ZSTD_EXTERN_DECL size_t(*ZSTD_compressCCtx)(char *, void *, size_t, const void *, size_t, int);

ZSTD_EXTERN_DECL char *(*ZSTD_createDCtx)(void);
ZSTD_EXTERN_DECL size_t(*ZSTD_freeDCtx)(char *);
ZSTD_EXTERN_DECL size_t(*ZSTD_decompressDCtx)(char *, void *, size_t, const void *, size_t);

class CodecZStandard : public Codec {
 public:

  CodecZStandard(int compression_level):Codec(compression_level) {
    static std::once_flag loaded;
    static void *dl_handle = NULL;

    std::call_once(loaded, [this]() {
        dl_handle = get_dlopen_handle("zstd", "1");
        if (dl_handle) {
          BIND_SYMBOL(dl_handle, ZSTD_compressBound, "ZSTD_compressBound", (size_t(*)(size_t)));
          BIND_SYMBOL(dl_handle, ZSTD_isError, "ZSTD_isError", (int(*)(size_t)));
          BIND_SYMBOL(dl_handle, ZSTD_getErrorName, "ZSTD_getErrorName", (char *(*)(size_t)));
          BIND_SYMBOL(dl_handle, ZSTD_maxCLevel, "ZSTD_maxCLevel", (int(*)(void)));
          BIND_SYMBOL(dl_handle, ZSTD_compress, "ZSTD_compress", (size_t(*)(void *, size_t, const void *, size_t, int)));
          BIND_SYMBOL(dl_handle, ZSTD_decompress, "ZSTD_decompress", (size_t(*)(void *, size_t, const void *, size_t)));

          BIND_SYMBOL(dl_handle, ZSTD_createCCtx, "ZSTD_createCCtx", (char*(*)(void)));
          BIND_SYMBOL(dl_handle, ZSTD_freeCCtx, "ZSTD_freeCCtx", (size_t(*)(char*)));
          BIND_SYMBOL(dl_handle, ZSTD_compressCCtx, "ZSTD_compressCCtx", (size_t(*)(char *, void *, size_t, const void *, size_t, int)));

          BIND_SYMBOL(dl_handle, ZSTD_createDCtx, "ZSTD_createDCtx", (char*(*)(void)));
          BIND_SYMBOL(dl_handle, ZSTD_freeDCtx, "ZSTD_freeDCtx", (size_t(*)(char*)));
          BIND_SYMBOL(dl_handle, ZSTD_decompressDCtx, "ZSTD_decompressDCtx", (size_t(*)(char *, void *, size_t, const void *, size_t)));
        } else {
          throw std::system_error(ECANCELED, std::generic_category(), dl_error_ + " ZStd library not found. Install ZStandard and/or setup library paths.");
        }
      });

    name_ = "ZSTD";
  }

  int do_compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size) override;

  int do_decompress_tile(unsigned char* tile_compressed,  size_t tile_compressed_size, unsigned char* tile, size_t tile_size) override;

};

#endif /*__CODEC_ZSTD_H__*/
