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

#define JPEG2K_EXTERN_DECL extern
#include "codec_jpeg2K.h"

void cleanup(void *l_data, opj_stream_t *l_stream, opj_codec_t *l_codec, opj_image_t *l_image)
{
   if (l_data)   free(l_data);
   if (l_stream) opj_stream_destroy(l_stream);
   if (l_image)  opj_destroy_image(l_image);
   if (l_codec)  opj_destroy_codec(l_codec);
}

void get_header_values(unsigned char *tile_in,
                       OPJ_UINT32 *nc,
                       int *ih, int *iw)
{
   int *buffer = (int *) tile_in;

   *nc = buffer[0];       // number of color components
   *ih = buffer[1];       // image height
   *iw = buffer[2];       // image width

/** Do we need these? **
   *c_space = buffer[5];  // color space ASSUME set from num_comps
   *t_num = buffer[6];    // tile number ASSUME the number makes no difference
**/

   return;
}

int CodecJPEG2K::compress_tile(unsigned char* tile_in, size_t tile_size_in, void** tile_compressed, size_t& tile_compressed_size) 
{
   opj_cparameters_t l_param;
   opj_codec_t * l_codec;
   opj_image_t * l_image;
   opj_image_cmptparm_t l_image_params[NUM_COMPS_MAX];
   opj_stream_t * l_stream;
   //OPJ_UINT32 l_nb_tiles_width, l_nb_tiles_height, l_nb_tiles;
   //OPJ_UINT32 l_data_size;
   //size_t len;

   opj_image_cmptparm_t * l_current_param_ptr;
   OPJ_UINT32 i;
   OPJ_BYTE *l_data;

   OPJ_UINT32 num_comps;
   int image_width;
   int image_height;
   int tile_width;
   int tile_height;

// Fixed parameter values. May need to amend if different image types are used
   int comp_prec = 8;
   int irreversible = 1;
   int cblockw_init = 64;
   int cblockh_init = 64;
   int numresolution = 6;
   OPJ_UINT32 offsetx = 0;
   OPJ_UINT32 offsety = 0;
   int quality_loss = 0; // lossless

   get_header_values(tile_in, &num_comps, &image_height, &image_width);

// Use whole tile as single "image"; tiling set in TileDB level
   tile_height = image_height;
   tile_width = image_width;

   opj_set_default_encoder_parameters(&l_param); // Default parameters

   if (num_comps > NUM_COMPS_MAX) {
      char msg[100];
      sprintf(msg, "ERROR -> j2k_compress: num_comps %d > NUM_COMPS_MAX %d\n", num_comps, NUM_COMPS_MAX);
      return print_errmsg(msg);
   }

//  Set data pointer to the first pixel byte beyond the header values
   OPJ_UINT32 header_offset = 3 * sizeof(int);
   l_data = (OPJ_BYTE*) &tile_in[header_offset];
   OPJ_UINT32 tilesize = tile_size_in - header_offset;

   /** number of quality layers in the stream */
   if (quality_loss) {
      l_param.tcp_numlayers = 1;
      l_param.cp_fixed_quality = 1;
      l_param.tcp_distoratio[0] = 20;
   }

   /* tile definitions parameters */
   /* position of the tile grid aligned with the image */
   l_param.cp_tx0 = 0;
   l_param.cp_ty0 = 0;
   /* tile size, we are using tile based encoding */
   l_param.tile_size_on = OPJ_TRUE;
   l_param.cp_tdx = tile_width;
   l_param.cp_tdy = tile_height;

   /* code block size */
   l_param.cblockw_init = cblockw_init;
   l_param.cblockh_init = cblockh_init;

   /* use irreversible encoding ?*/
   l_param.irreversible = irreversible;

   /** number of resolutions */
   l_param.numresolution = numresolution;

   /** progression order to use*/
   l_param.prog_order = OPJ_LRCP; // default

   /* image definition */
   l_current_param_ptr = l_image_params;
   for (i = 0; i < num_comps; ++i) {
       /* do not bother bpp useless */
       /*l_current_param_ptr->bpp = COMP_PREC;*/
       l_current_param_ptr->dx = 1;
       l_current_param_ptr->dy = 1;

       l_current_param_ptr->h = (OPJ_UINT32)image_height;
       l_current_param_ptr->w = (OPJ_UINT32)image_width;

       l_current_param_ptr->sgnd = 0;
       l_current_param_ptr->prec = (OPJ_UINT32)comp_prec;

       l_current_param_ptr->x0 = offsetx;
       l_current_param_ptr->y0 = offsety;

       ++l_current_param_ptr;
   }

   /* should we do j2k or jp2 ?*/
   l_codec = opj_create_compress(OPJ_CODEC_JP2);
   // l_codec = opj_create_compress(OPJ_CODEC_J2K);

   if (! l_codec) {
      char msg[100];
      sprintf(msg, "ERROR -> j2k_compress: failed to setup codec!\n");
      return print_errmsg(msg);
   }

   if (num_comps == 3)
      l_image = opj_image_tile_create(num_comps, l_image_params, OPJ_CLRSPC_SRGB);
   else if (num_comps == 1)
      l_image = opj_image_tile_create(num_comps, l_image_params, OPJ_CLRSPC_GRAY);
   else
      l_image = opj_image_tile_create(num_comps, l_image_params, OPJ_CLRSPC_UNKNOWN);
   if (! l_image) {
      cleanup(NULL, NULL, l_codec, NULL);
      char msg[100];
      sprintf(msg, "ERROR -> j2k_compress: failed to setup tile image!\n");
      return print_errmsg(msg);
   }

   l_image->x0 = offsetx;
   l_image->y0 = offsety;
   l_image->x1 = offsetx + (OPJ_UINT32)image_width;
   l_image->y1 = offsety + (OPJ_UINT32)image_height;
/** should be set in opj_image_tile_create() **
   if (num_comps == 3)
      l_image->color_space = OPJ_CLRSPC_SRGB;
   else if (num_comps == 1)
      l_image->color_space = OPJ_CLRSPC_GRAY;
   else
      l_image->color_space = OPJ_CLRSPC_UNKNOWN;
**/

printf("JPEG200 compresion\n");
   if (! opj_setup_encoder(l_codec, &l_param, l_image)) {
      cleanup(NULL, NULL, l_codec, l_image);
      char msg[100];
      sprintf(msg, "ERROR -> j2k_compress: failed to setup the codec!\n");
      return print_errmsg(msg);
   }

   l_stream = (opj_stream_t *) opj_stream_create_default_memory_stream(OPJ_FALSE);
   if (! l_stream) {
      cleanup(NULL, NULL, l_codec, l_image);
      char msg[100];
      sprintf(msg, "ERROR -> j2k_compress: failed to create the memory stream!\n");
      return print_errmsg(msg);
   }

   if (! opj_start_compress(l_codec, l_image, l_stream)) {
      cleanup(NULL, l_stream, l_codec, l_image);
      char msg[100];
      sprintf(msg, "ERROR -> j2k_compress: failed to start compress!\n");
      return print_errmsg(msg);
   }

  /** Always write tile 0 **/
   if (! opj_write_tile(l_codec, 0, l_data, tilesize, l_stream)) {
      cleanup(NULL, l_stream, l_codec, l_image);
      char msg[100];
      sprintf(msg, "ERROR -> j2k_compress: failed to write the tile %d!\n", i);
      return print_errmsg(msg);
   }

   if (! opj_end_compress(l_codec, l_stream)) {
      cleanup(NULL, l_stream, l_codec, l_image);
      char msg[100];
      sprintf(msg, "ERROR -> j2k_compress: failed to end compress!\n");
      return print_errmsg(msg);
   }

   *tile_compressed = opj_mem_stream_copy(l_stream, &tile_compressed_size);

/* Can the image_buffer be output as a binary file recognized as JP2? */
   char JP2fileName[22] = "encode_image_TDB.jp2";
   FILE *write_ptr;
   write_ptr = fopen(JP2fileName,"wb");  // w for write, b for binary
   fwrite(*tile_compressed, tile_compressed_size, 1, write_ptr);
   fclose(write_ptr);

   cleanup(NULL, l_stream, l_codec, l_image);

   // Success
   return TILEDB_CD_OK;
}


int CodecJPEG2K::decompress_tile(unsigned char* tile_compressed, size_t tile_compressed_size, unsigned char* tile_out, size_t size_out) 
{
   opj_dparameters_t l_param;
   opj_codec_t * l_codec;
   opj_image_t * l_image;
   opj_stream_t * l_stream;
   OPJ_UINT32 l_data_size;
   OPJ_UINT32 l_max_data_size = 1000;
   OPJ_BYTE * l_data = (OPJ_BYTE *) malloc(1000);

   OPJ_UINT32 l_tile_index;
   OPJ_INT32 l_current_tile_x0;
   OPJ_INT32 l_current_tile_y0;
   OPJ_INT32 l_current_tile_x1;
   OPJ_INT32 l_current_tile_y1;
   OPJ_UINT32 l_nb_comps=0;
   OPJ_BOOL l_go_on = OPJ_TRUE;

   OPJ_UINT32 image_height;
   OPJ_UINT32 image_width;
   OPJ_UINT32 numcomps;

   /* Set the default decoding parameters */
   opj_set_default_decoder_parameters(&l_param);
   l_param.decod_format = JP2_CFMT;

   /** you may here add custom decoding parameters */
   /* do not use layer decoding limitations */
   l_param.cp_layer = 0;

   /* do not use resolutions reductions */
   l_param.cp_reduce = 0;

   /** Fixed to match Compress format **/
   l_codec = opj_create_decompress(OPJ_CODEC_JP2);
   if (!l_codec){
      cleanup(l_data, NULL, NULL, NULL);
      char msg[100];
      sprintf(msg, "ERROR -> j2k_decompress: failed to create decompress codec\n");
      return print_errmsg(msg);
   }

   /** Setup stream with user data into stream **/
   l_stream = opj_stream_create_memory_stream((void *)tile_compressed, tile_compressed_size, OPJ_TRUE);
   if (!l_stream){
      cleanup(l_data, NULL, l_codec, NULL);
      char msg[100];
      sprintf(msg, "ERROR -> j2k_decompress: failed to create the memory stream\n");
      return print_errmsg(msg);
   }

   /* Setup the decoder decoding parameters using user parameters */
   if (! opj_setup_decoder(l_codec, &l_param)) {
      cleanup(l_data, l_stream, l_codec, NULL);
      char msg[100];
      sprintf(msg, "ERROR -> j2k_decompress: failed to setup the decoder\n");
      return print_errmsg(msg);
   }

   /* Read the main header of the codestream and if necessary the JP2 boxes*/
   if (! opj_read_header(l_stream, l_codec, &l_image)) {
      cleanup(l_data, l_stream, l_codec, NULL);
      char msg[100];
      sprintf(msg, "ERROR -> j2k_decompress: failed to read the header\n");
      return print_errmsg(msg);
   }

   // PULL OUT numcomps, image_height, image_width from  l_stream
   image_height = l_image->comps[0].h;
   image_width = l_image->comps[0].w;
   numcomps = l_image->numcomps;

   if (!opj_set_decode_area(l_codec, l_image, 0, 0, 0, 0)){ // whole image
      cleanup(l_data, l_stream, l_codec, l_image);
      char msg[100];
      sprintf(msg, "ERROR -> j2k_decompress: failed to set the decoded area\n");
      return print_errmsg(msg);
   }

   if (! opj_read_tile_header( l_codec, l_stream, &l_tile_index, &l_data_size,
                               &l_current_tile_x0, &l_current_tile_y0,
                               &l_current_tile_x1, &l_current_tile_y1,
                               &l_nb_comps, &l_go_on)) {
      cleanup(l_data, l_stream, l_codec, l_image);
      char msg[100];
      sprintf(msg, "ERROR -> j2k_decompress: failed to read_tile_header\n");
      return print_errmsg(msg);
   }

   if (l_go_on) {
      if (l_data_size > l_max_data_size) {
         OPJ_BYTE *l_new_data = (OPJ_BYTE *) realloc(l_data, l_data_size+12);
         if (! l_new_data) {
            cleanup(l_data, l_stream, l_codec, l_image);
            char msg[100];
            sprintf(msg, "ERROR -> j2k_decompress: failed to realloc new data of size %d\n", l_data_size+12);
            return print_errmsg(msg);
         }
         l_data = l_new_data;
         l_max_data_size = l_data_size;
      }

      if (! opj_decode_tile_data(l_codec,l_tile_index,l_data,l_data_size,l_stream)) {
         cleanup(l_data, l_stream, l_codec, l_image);
         char msg[100];
         sprintf(msg, "ERROR -> j2k_decompress: unable to decode_tile_data\n");
         return print_errmsg(msg);
      }
   }  // if (l_go_on) 
   else { i              // Problem in the opj_read_tile_header() 
       cleanup(l_data, l_stream, l_codec, l_image);
       char msg[100];
       sprintf(msg, "ERROR -> j2k_decompress: Current tile number and total number of tiles problem\n");
       return print_errmsg(msg);
   }

   if (! opj_end_decompress(l_codec,l_stream)) {
      cleanup(l_data, l_stream, l_codec, l_image);
      char msg[100];
      sprintf(msg, "ERROR -> j2k_decompress: Failed to end_decompress correctly\n");
      return print_errmsg(msg);
   }

   // Double check space in tile for decompressed data 
   size_t image_bytes = 3*sizeof(OPJ_UINT32) + l_data_size;
   if (image_bytes > size_out) {
      char msg[100];
      sprintf(msg, "Tile decompresss size (%lu) greater than expectef tile size (%lu)\n", image_bytes, size_out);
      return print_errmsg(msg);
   }

   size_t copy_bytes = size_out - 3*sizeof(OPJ_UINT32);

   // Add header portion -- write ints into char array
   OPJ_UINT32 *buffer = (OPJ_UINT32 *)tile_out;
   buffer[0] = numcomps;      // number of color components
   buffer[1] = image_height;  // image height
   buffer[2] = image_width;   // image width

   OPJ_BYTE *ob = tile_out;
   ob += 3*sizeof(OPJ_UINT32);

   // Copy pixel data to output buffer
   memcpy(ob, l_data, copy_bytes);

   /* Free memory */
   cleanup(l_data, l_stream, l_codec, l_image);

   // Success
   return TILEDB_CD_OK;
}

#endif
