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
 * CodecJPEG2K derived from Codec for JPEG2000 support
 *
 */

#ifndef __CODEC_JPEG2K_H__
#define  __CODEC_JPEG2K_H__

#include "codec.h"

/* OpenJPEG 2000 include for types and struct definitions */
#include "openjpeg-2.3/opj_types.h"
#include "string.h"
//#include "openjpeg-2.3/openjpeg.h"
#include "openjpeg-2.3/format_defs.h"

// Function Pointers for JPEG2000
#if !defined(JPEG2K_EXTERN_DECL)
#  define JPEG2K_EXTERN_DECL 
#endif

JPEG2K_EXTERN_DECL void(*opj_set_default_encoder_parameters)(opj_cparameters_t *);
JPEG2K_EXTERN_DECL opj_codec_t*(*opj_create_compress)(OPJ_CODEC_FORMAT);
JPEG2K_EXTERN_DECL opj_image_t*(*opj_image_create)(OPJ_UINT32, opj_image_cmptparm_t *, OPJ_COLOR_SPACE);
JPEG2K_EXTERN_DECL OPJ_BOOL(*opj_setup_encoder)(opj_codec_t *, opj_cparameters_t *, opj_image_t *);
JPEG2K_EXTERN_DECL opj_stream_t*(*opj_stream_create_default_memory_stream)(OPJ_BOOL);
JPEG2K_EXTERN_DECL OPJ_BOOL(*opj_start_compress)(opj_codec_t *, opj_image_t *, opj_stream_t *);
JPEG2K_EXTERN_DECL OPJ_BOOL(*opj_encode)(opj_codec_t *, opj_stream_t *);
JPEG2K_EXTERN_DECL OPJ_BOOL(*opj_end_compress)(opj_codec_t *, opj_stream_t *);
JPEG2K_EXTERN_DECL OPJ_BYTE*(*opj_mem_stream_copy)(opj_stream_t *, size_t*);

JPEG2K_EXTERN_DECL void (*opj_set_default_decoder_parameters)(opj_dparameters_t *);
JPEG2K_EXTERN_DECL opj_codec_t*(*opj_create_decompress)(OPJ_CODEC_FORMAT);
JPEG2K_EXTERN_DECL opj_stream_t* (*opj_stream_create_memory_stream)(void *, OPJ_SIZE_T, OPJ_BOOL );
JPEG2K_EXTERN_DECL OPJ_BOOL (*opj_setup_decoder)(opj_codec_t *, opj_dparameters_t *);
JPEG2K_EXTERN_DECL OPJ_BOOL (*opj_read_header)(opj_stream_t *, opj_codec_t *, opj_image_t **);
JPEG2K_EXTERN_DECL OPJ_BOOL (*opj_set_decode_area)(opj_codec_t *, opj_image_t *, int, int, int, int);
JPEG2K_EXTERN_DECL OPJ_BOOL (*opj_read_tile_header)(opj_codec_t *, opj_stream_t *, OPJ_UINT32 *, OPJ_UINT32 *, OPJ_INT32 *, OPJ_INT32 *, OPJ_INT32 *, OPJ_INT32 *, OPJ_UINT32 *, OPJ_BOOL *);
JPEG2K_EXTERN_DECL OPJ_BOOL (*opj_decode_tile_data)(opj_codec_t *, OPJ_UINT32, OPJ_BYTE *, OPJ_UINT32, opj_stream_t *);
JPEG2K_EXTERN_DECL OPJ_BOOL (*opj_end_decompress)(opj_codec_t *, opj_stream_t *);
JPEG2K_EXTERN_DECL void (*opj_destroy_image)(opj_image_t *);
JPEG2K_EXTERN_DECL void (*opj_stream_destroy)(opj_stream_t *);
JPEG2K_EXTERN_DECL void (*opj_destroy_codec)(opj_codec_t *);

// Maximum number of components within an image allowed
#define NUM_COMPS_MAX 4

class CodecJPEG2K : public Codec {
 public:
  
  CodecJPEG2K(int compression_level):Codec(compression_level) {
    static bool loaded = false;
    static std::mutex loading;
    
    if (!loaded) {
      loading.lock();
      
      if (!loaded) {
        dl_handle_ = get_dlopen_handle("openjp2");
        if (dl_handle_ != NULL) {

/** compress_tile functions **/
	  BIND_SYMBOL(dl_handle_, opj_set_default_encoder_parameters, "opj_set_default_encoder_parameters", (void (*)(opj_cparameters_t *)));
	  BIND_SYMBOL(dl_handle_, opj_create_compress, "opj_create_compress", (opj_codec_t*(*)(OPJ_CODEC_FORMAT)));
	  BIND_SYMBOL(dl_handle_, opj_image_create, "opj_image_create", (opj_image_t*(*)(OPJ_UINT32, opj_image_cmptparm_t *, OPJ_COLOR_SPACE)));
	  BIND_SYMBOL(dl_handle_, opj_setup_encoder, "opj_setup_encoder", (OPJ_BOOL(*)(opj_codec_t *, opj_cparameters_t *, opj_image_t *)));
	  BIND_SYMBOL(dl_handle_, opj_stream_create_default_memory_stream, "opj_stream_create_default_memory_stream", (opj_stream_t*(*)(OPJ_BOOL)));
	  BIND_SYMBOL(dl_handle_, opj_start_compress, "opj_start_compress", (OPJ_BOOL(*)(opj_codec_t *, opj_image_t *, opj_stream_t *)));
	  BIND_SYMBOL(dl_handle_, opj_encode, "opj_encode", (OPJ_BOOL(*)(opj_codec_t *, opj_stream_t *)));
	  BIND_SYMBOL(dl_handle_, opj_end_compress, "opj_end_compress", (OPJ_BOOL(*)(opj_codec_t *, opj_stream_t *)));
	  BIND_SYMBOL(dl_handle_, opj_mem_stream_copy, "opj_mem_stream_copy", (OPJ_BYTE*(*)(opj_stream_t *, size_t*)));

/** decompress_tile functions **/
	  BIND_SYMBOL(dl_handle_, opj_set_default_decoder_parameters, "opj_set_default_decoder_parameters", (void(*)(opj_dparameters_t *)));
	  BIND_SYMBOL(dl_handle_, opj_create_decompress, "opj_create_decompress", (opj_codec_t*(*)(OPJ_CODEC_FORMAT)));
	  BIND_SYMBOL(dl_handle_, opj_stream_create_memory_stream, "opj_stream_create_memory_stream", (opj_stream_t*(*)(void *, OPJ_SIZE_T, OPJ_BOOL )));
	  BIND_SYMBOL(dl_handle_, opj_setup_decoder, "opj_setup_decoder", (OPJ_BOOL(*)(opj_codec_t *, opj_dparameters_t *)));
	  BIND_SYMBOL(dl_handle_, opj_read_header, "opj_read_header", (OPJ_BOOL(*)(opj_stream_t *, opj_codec_t *, opj_image_t **)));
	  BIND_SYMBOL(dl_handle_, opj_set_decode_area, "opj_set_decode_area", (OPJ_BOOL(*)(opj_codec_t *, opj_image_t *, int, int, int, int)));
	  BIND_SYMBOL(dl_handle_, opj_read_tile_header, "opj_read_tile_header", (OPJ_BOOL(*)(opj_codec_t *, opj_stream_t *, OPJ_UINT32 *, OPJ_UINT32 *, OPJ_INT32 *, OPJ_INT32 *, OPJ_INT32 *, OPJ_INT32 *, OPJ_UINT32 *, OPJ_BOOL *)));
	  BIND_SYMBOL(dl_handle_, opj_decode_tile_data, "opj_decode_tile_data", (OPJ_BOOL(*)(opj_codec_t *, OPJ_UINT32, OPJ_BYTE *, OPJ_UINT32, opj_stream_t *)));
	  BIND_SYMBOL(dl_handle_, opj_end_decompress, "opj_end_decompress", (OPJ_BOOL(*)(opj_codec_t *, opj_stream_t *)));

/** Common functions **/
	  BIND_SYMBOL(dl_handle_, opj_stream_destroy, "opj_stream_destroy", (void(*)(opj_stream_t *)));
	  BIND_SYMBOL(dl_handle_, opj_destroy_codec, "opj_destroy_codec", (void(*)(opj_codec_t *)));
	  BIND_SYMBOL(dl_handle_, opj_destroy_image, "opj_image_destroy", (void(*)(opj_image_t *)));

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

#endif /*__CODEC_JPEG200_H__*/
