/**
 * @file codec_jpeg2K.h
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
 * CodecJPEG2000 derived from Codec for JPEG2000 support
 *
 */

#ifndef __CODEC_JPEG2000_H__
#define  __CODEC_JPEG2000_H__

#include "codec.h"

/* OpenJPEG 2000 includes */
#include "opj_apps_config.h"
#include "openjpeg.h"
#include "opj_getopt.h"
#include "convert.h"
#include "index.h"

#include "event.h"
#include "color.h"
#include "format_defs.h"
#include "opj_string.h"

// Function Pointers for JPEG2000
#if !defined(JPEG2000_EXTERN_DECL)
#  define JPEG2000_EXTERN_DECL
#endif

/*
JPEG2000_EXTERN_DECL int(*JPEG2000_compressBound)(int);
JPEG2000_EXTERN_DECL size_t(*JPEG2000_compress_default)(const char *, char *, int, int);
JPEG2000_EXTERN_DECL size_t(*JPEG2000_decompress_safe)(const char *, char *, int, int);
*/

class CodecJPEG2000 : public Codec {
 public:
  
  CodecJPEG2000(int compression_level):Codec(compression_level) {
    static bool loaded = false;
    static std::mutex loading;
    
    if (!loaded) {
      loading.lock();
      
      if (!loaded) {
        dl_handle_ = get_dlopen_handle("openjp2");
        if (dl_handle_ != NULL) {
/*
	  BIND_SYMBOL(dl_handle_, LZ4_compressBound, "LZ4_compressBound", (int(*)(int)));
	  BIND_SYMBOL(dl_handle_, LZ4_compress_default, "LZ4_compress_default", (size_t(*)(const char *, char *, int, int)));
	  BIND_SYMBOL(dl_handle_, LZ4_decompress_safe, "LZ4_decompress_safe", (size_t(*)(const char *, char *, int, int)));
*/
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
	throw std::system_error(ECANCELED, std::generic_category(), "OpenJPEG 2000 library not found. Install OpenJPEG 2000 and setup library paths.");
      }
    }
  }
  
  int compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size);

  int decompress_tile(unsigned char* tile_compressed,  size_t tile_compressed_size, unsigned char* tile, size_t tile_size);
  
};

/**********************************
 ** Memory (sub)stream structure **
 **********************************/
typedef OPJ_BOOL (* mem_stream_resize_fn )( void *m_buffer);

typedef struct mem_stream {

/** The holder of actual data; can be increased with mem_resize_fn **/
    OPJ_BYTE *mem_data;

/** Index value of current element of mem_data **/
    OPJ_UINT64 mem_curidx;

/** Current size of mem_data array **/
    OPJ_UINT64 mem_cursize;

/** Resize function to increase the current array size in mem_data **/
    mem_stream_resize_fn  mem_resize_fn;

} mem_stream_t;


#endif /*__CODEC_JPEG200_H__*/
