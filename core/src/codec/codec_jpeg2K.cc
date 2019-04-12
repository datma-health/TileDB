/**
 * @file codec_jpeg2K.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 Omics Data Automation, Inc.
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
 * This file implements codec for OpenJPEG 2000 compression and decompression.
 */

#ifdef ENABLE_JPEG2K

#define JPEG2000_EXTERN_DECL extern
#include "codec_jpeg2K.h"

void image_copy_out(opj_stream_t *s, OPJ_BYTE** return_buff, size_t* size_out)
{
   opj_stream_private_t* p = (opj_stream_private_t*) s;
   //size_t img_bytes = p->m_bytes_in_buffer;
   mem_stream_t* m = (mem_stream_t*) p->m_user_data;

   size_t img_bytes = m->mem_curidx;

   *return_buff = (char *) malloc(img_bytes * sizeof(char));
   if (!(*return_buff)) {
     printf("Fail to allocate %d bytes: image_copy_out\n", img_bytes);
     return;
   }
   memcpy(*return_buff, &(m->mem_data[0]), img_bytes);
   *size_out = img_bytes;

/*
   printf ("Compare First 16 bytes in compressed buffer with buff_out:\n");
   int i;
   for (i = 0; i < 16 ; ++i) {
      printf("%3d: %02x  ",i, ((OPJ_BYTE*)(p->m_stored_data[i])));
      printf("%02x  ",m->mem_data[i]);
      printf("%02x \n",(*return_buff)[i]);
   }
   printf("\n");
*/
}


int CodecLZ4::compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size) {
   opj_cparameters_t parameters;   /* compression parameters */
   opj_image_t *image = (opj_image_t *) tile;

   opj_stream_t *l_stream = 00;
   opj_codec_t* l_codec = 00;
   OPJ_BOOL bSuccess;

   /* set encoding parameters to default values */
   opj_set_default_encoder_parameters(&parameters);

   parameters.decod_format = TIF_DFMT; // hard-code to TIF format
   parameters.cod_format = 1; // hard-code JP2 format for compression
   parameters.tcp_numlayers = 1;
   parameters.cp_disto_alloc = 1; // hardwired to match example compress
   parameters.cp_fixed_alloc = 0;
   parameters.cp_fixed_quality = 0;
   if (image->numcomps > 1) {
      parameters.tcp_mct = 1;
   }

   /* JPEG-2000 compressed image data */
   /* Get a decoder handle */
   //l_codec = opj_create_compress(OPJ_CODEC_J2K);
   l_codec = opj_create_compress(OPJ_CODEC_JP2);

   if (! opj_setup_encoder(l_codec, &parameters, image)) {
      opj_destroy_codec(l_codec);
      //opj_image_destroy(image);
      return print_errmsg("Failed to encode with JPEG 2000: opj_setup_encoder");
   }

/* open a byte stream for writing and allocate memory for all tiles */
   l_stream = (opj_stream_t *) opj_stream_create_default_memory(OPJ_FALSE);
   if (! l_stream) { 
      return print_errmsg("Failed to open memory byte stream JPEG 2000: opj_stream_create_default_memory");
   }

/* encode the image */
   bSuccess = opj_start_compress(l_codec, image, l_stream);
   if (!bSuccess)  {
      return print_errmsg("Failed compressing with JPEG 2000: opj_start_compress");
   }

   bSuccess = opj_encode(l_codec, l_stream);
   if (!bSuccess)  {
      return print_errmsg("Failed compressing with JPEG 2000: opj_encode");
   }

   bSuccess = opj_end_compress(l_codec, l_stream);
   if (!bSuccess)  {
      return print_errmsg("Failed compressing with JPEG 2000: opj_end_compress");
   }
   
   image_copy_out(l_stream, tile_compressed, tile_compressed_size);
   
   /* close and free the byte stream */
   opj_stream_destroy(l_stream);
   
   /* free remaining compression structures */
   opj_destroy_codec(l_codec);
   
   /* free image data */
   //opj_image_destroy(image); // Don't need to free input parameter
 
   // Success
   return TILEDB_CD_OK;
}

int CodecLZ4::decompress_tile(unsigned char* tile_compressed,  size_t tile_compressed_size, unsigned char* tile, size_t tile_size) {
    // Decompress tile 
   opj_decompress_parameters parameters;   /* decompression parameters */

   opj_stream_t *l_stream = 00;
   opj_codec_t* l_codec = 00;
   opj_image_t *image = NULL;
   OPJ_BOOL bSuccess;

   /* set decoding parameters to default values */
   //set_default_parameters(&parameters);
   if (parameters) {
      memset(parameters, 0, sizeof(opj_decompress_parameters));

      /* default decoding parameters (command line specific) */
      parameters->decod_format = -1;
      parameters->cod_format = -1;

      /* default decoding parameters (core) */
      opj_set_default_decoder_parameters(&(parameters->core));
   }

   // NEED TO FIX -- How can the decode format be sent into this function?
   // Is this really needed/used if result of this operation is image_t
   parameters.decod_format = TIF_DFMT; // hard-code to TIF format

   /* create stream in memory and set m_stored_data pointer */
   /* ----------------------------------------------------- */
   l_stream = (opj_stream_t *) opj_stream_create_default_memory(OPJ_TRUE);
   if (!l_stream) {
      return print_errmsg("Failed to create stream for decompress: opj_stream_create_default_memory");
   }
   opj_stream_mem_set_user_data(l_stream, (void*)tile_compressed, tile_compressed_size, NULL);

   opj_stream_private_t *p_stream = (opj_stream_private_t *) l_stream;
   mem_stream_t * m_stream = (mem_stream_t *) p_stream->m_user_data;

   /* decode the JPEG2000 stream */
   /* Get a decoder handle */
   //l_codec = opj_create_decompress(OPJ_CODEC_J2K);
   l_codec = opj_create_decompress(OPJ_CODEC_JP2);

   /* catch events using our callbacks and give a local context */
    //opj_set_info_handler(l_codec, info_callback, 00);
    //opj_set_warning_handler(l_codec, warning_callback, 00);
    //opj_set_error_handler(l_codec, error_callback, 00);

   /* Setup the decoder decoding parameters using user parameters */
   if (!opj_setup_decoder(l_codec, &(parameters.core))) {
      opj_stream_destroy(l_stream);
      opj_destroy_codec(l_codec);
      return print_errmsg("Failed to setup the decoder: opj_setup_decoder");
   }

   /* Read the main header of the codestream and if necessary the JP2 boxes*/
   if (! opj_read_header(l_stream, l_codec, &image)) {
      opj_stream_destroy(l_stream);
      opj_destroy_codec(l_codec);
      opj_image_destroy(image);
      return print_errmsg("Failed to read the header: opj_read_header");
   }

   if (parameters.numcomps) {
       if (! opj_set_decoded_components(l_codec,
                                        parameters.numcomps,
                                        parameters.comps_indices,
                                        OPJ_FALSE)) {
           opj_destroy_codec(l_codec);
           opj_stream_destroy(l_stream);
           opj_image_destroy(image);
           return print_errmsg("Failed to set the component indices: opj_set_decoded_components");
           return EXIT_FAILURE;
       }
   }

   /* decode the entire image */
   if (!opj_set_decode_area(l_codec, image, (OPJ_INT32)parameters.DA_x0,
                                            (OPJ_INT32)parameters.DA_y0,
                                            (OPJ_INT32)parameters.DA_x1,
                                            (OPJ_INT32)parameters.DA_y1)) {
       opj_stream_destroy(l_stream);
       opj_destroy_codec(l_codec);
       opj_image_destroy(image);
       return print_errmsg("Failed to set the decoded area: opj_set_decode_area");
       return EXIT_FAILURE;
   }

   /* Get the decoded image */
   if (!(opj_decode(l_codec, l_stream, image) &&
         opj_end_decompress(l_codec,   l_stream))) {
      opj_destroy_codec(l_codec);
      opj_stream_destroy(l_stream);
      opj_image_destroy(image);
      return print_errmsg("Failed to decode the image: opj_decode && opj_end_decompress");
   }

   if (image->comps[0].data == NULL) {
      fprintf(stderr, "ERROR -> opj_decompress: no image data!\n");
      opj_destroy_codec(l_codec);
      opj_stream_destroy(l_stream);
      opj_image_destroy(image);
      return print_errmsg("ERROR -> decompress_tile: no image data generated");
   }

   /** Assume RGB output is desired **/
   image->color_space = OPJ_CLRSPC_SRGB:

   /* Close the byte stream */
   opj_stream_destroy(l_stream);

   /** From opj_decompress.c **/
/*
   if (image->color_space != OPJ_CLRSPC_SYCC
           && image->numcomps == 3 && image->comps[0].dx == image->comps[0].dy
           && image->comps[1].dx != 1) {
       image->color_space = OPJ_CLRSPC_SYCC;
   }
   else if (image->numcomps <= 2) {
       image->color_space = OPJ_CLRSPC_GRAY;
   }

   if (image->color_space == OPJ_CLRSPC_SYCC) {
       color_sycc_to_rgb(image);
   }
   else if ((image->color_space == OPJ_CLRSPC_CMYK) &&
              (parameters.cod_format != TIF_DFMT)) {
       color_cmyk_to_rgb(image);
   }
   else if (image->color_space == OPJ_CLRSPC_EYCC) {
       color_esycc_to_rgb(image);
   }

   if (image->icc_profile_buf) {
#if defined(OPJ_HAVE_LIBLCMS1) || defined(OPJ_HAVE_LIBLCMS2)
     if (image->icc_profile_len) {
         color_apply_icc_profile(image);
     }
     else {
         color_cielab_to_rgb(image);
     }
#endif
     free(image->icc_profile_buf);
     image->icc_profile_buf = NULL;
     image->icc_profile_len = 0;
   }
   // Force RGB output //
   // ---------------- //
   if (parameters.force_rgb) {
      switch (image->color_space) {
      case OPJ_CLRSPC_SRGB:
           break;
      case OPJ_CLRSPC_GRAY:
           image = convert_gray_to_rgb(image);
           break;
      default:
           //"ERROR -> opj_decompress: don't know how to convert image to RGB colorspace!"
           opj_image_destroy(image);
           image = NULL;
           break;
      }

      if (image == NULL) {
         opj_destroy_codec(l_codec);
         return print_errmsg("ERROR -> opj_decompress: failed to convert to RGB image!");
      }
   }

*/
   /* return image data */
   *tile = (char *)image;
   // ESTIMATE size of decompressed image
   //   Number of bytes is computed from number of components and 
   //   size of the data arrays holding each color component.
   *tile_size = (size_t) image->numcomps * (image->comps->w * image->comps->h);

   /* free remaining compression structures */
   opj_destroy_codec(l_codec);

  // Success
  return TILEDB_CD_OK;
}

#endif
