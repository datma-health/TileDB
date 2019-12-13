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

void print_POC(opj_poc_t POC)
{

/**
 * Progression order changes
 *
 */
    printf("      Res num start, Cmpnt num start: %d %d\n", POC.resno0, POC.compno0);
    printf("      Lyr num end, Res num end, Cmpnt num end: %d %d %d\n", POC.layno1, POC.resno1, POC.compno1);
    printf("      Lyr num start, POCrnct num start, POCrnct num end: %d %d %d\n", POC.layno0, POC.precno0, POC.precno1);
    printf("      Progression order enum: %d %d\n", POC.prg1, POC.prg);
    printf("      Progression order string: %s\n", POC.progorder);
    printf("      Tile number: %u\n", POC.tile);
    printf("      Start and end values for Tile width and height: %d %d  %d %d\n", POC.tx0, POC.tx1, POC.ty0, POC.ty1);
    printf("      Start value, initialised in pi_initialise_encode: %d %d %d %d\n", POC.layS, POC.resS, POC.compS, POC.prcS);
    printf("      End value, initialised in pi_initialise_encode: %d %d %d %d\n", POC.layE, POC.resE, POC.compE, POC.prcE);
    printf("      Start and end values of Tile width and height: %d %d  %d %d  %d %d\n", POC.txS, POC.txE, POC.tyS, POC.tyE, POC.dx, POC.dy);
    printf("      Temporary values for Tile parts: %u %u %u %u %u %u\n", POC.lay_t, POC.res_t, POC.comp_t, POC.prc_t, POC.tx0_t, POC.ty0_t);

}

void print_parameters(opj_cparameters_t P)
{
   int i;

   printf("-- Compression Parameters: ------------------------------------------------------\n");
   printf("  Tile Size On? : %d\n", P.tile_size_on);
   printf("  XTOsiz, YTOsiz, XTsiz, YTsiz: %d %d %d %d\n", P.cp_tx0, P.cp_ty0, P.cp_tdx, P.cp_tdy);
   printf("  Allocation by rate/distortion: %d\n", P.cp_disto_alloc);
   printf("  Allocation by fixed layer: %d\n", P.cp_fixed_alloc);
   printf("  Add fixed_quality: %d\n", P.cp_fixed_quality);
   printf("  Fixed layer (cp_matrice array)\n");
   if (! P.cp_matrice)
      printf("     NULL\n");
   else {
      printf("     ");
      for (i = 0; i < 4; ++i)
         printf("%d ", P.cp_matrice[i]);
      printf("\n");
   }
   printf("  Comment for coding: %s\n", P.cp_comment);
   printf("  csty : Coding style: %d\n", P.csty);
   printf("  Progression order (default OPJ_LRCP): %d\n", P.prog_order);
   printf("  Number of POC, default to 0: %u\n", P.numpocs);
   if (P.numpocs > 0) {
      for (i = 0; i < P.numpocs; ++i) {
         printf("    Progression order change[%d]:\n", i);
         print_POC(P.POC[i]);
      }
   }
   printf("  Number of layers: %d\n", P.tcp_numlayers);
   if (P.tcp_numlayers > 0) {
      for (i = 0; i < P.tcp_numlayers; ++i)
         printf("    TCP Rates[%d]:  %f\n", i, P.tcp_rates[i]);
   }
   if (P.tcp_numlayers > 0) {
      for (i = 0; i < P.tcp_numlayers; ++i)
         printf("    TCP Distoratio[%d]:  %f\n", i, P.tcp_distoratio[i]);
   }
   printf("\n  Number of resolutions: %d\n", P.numresolution);
   printf("  Initial code block width, height (default 64):  %d  %d\n", P.cblockw_init, P.cblockh_init);
   printf("  Mode switch (cblk_style): %d\n", P.mode);
   printf("  Irreversible (0: lossless): %d\n", P.irreversible);
   printf("  Region of interest (-1 means no ROI): %d\n", P.roi_compno);
   printf("  Region of interest, upshift value: %d\n", P.roi_shift);
   printf("  Number of precinct size specifications: %d\n", P.res_spec);
   if (P.res_spec > 0) {
      for (i = 0; i < P.res_spec; ++i)
         printf("    Precinct width, height:  %d  %d\n", P.prcw_init[i], P.prch_init[i]);
   }
   printf("\n  Command line encoder parameters (not used inside the library)\n");
   printf("    Input file name: %s\n", P.infile);
   printf("    Output file name: %s\n", P.outfile);
   printf("    DEPRECATED. Index generation: %d\n", P.index_on);
   printf("    DEPRECATED. Index string: %s\n", P.index);
   printf("    Subimage encoding: origin offset x, y: %d  %d\n", P.image_offset_x0, P.image_offset_y0);
   printf("    Subsampling value for dx, dy: %d  %d\n", P.subsampling_dx, P.subsampling_dy);
   printf("    Input file format 0: PGX, 1: PxM, 2: BMP 3:TIF : %d\n", P.decod_format);
   printf("    Output file format 0: J2K, 1: JP2, 2: JPT : %d\n", P.cod_format);

   printf("\n  UniPG>> NOT YET USED IN THE V2 VERSION OF OPENJPEG\n");
    ///**@name JPWL encoding parameters */
    ///*@{*/
    ///** enables writing of EPC in MH, thus activating JPWL */
    //OPJ_BOOL jpwl_epc_on;
    ///** error protection method for MH (0,1,16,32,37-128) */
    //int jpwl_hprot_MH;
    ///** tile number of header protection specification (>=0) */
    //int jpwl_hprot_TPH_tileno[JPWL_MAX_NO_TILESPECS];
    ///** error protection methods for TPHs (0,1,16,32,37-128) */
    //int jpwl_hprot_TPH[JPWL_MAX_NO_TILESPECS];
    ///** tile number of packet protection specification (>=0) */
    //int jpwl_pprot_tileno[JPWL_MAX_NO_PACKSPECS];
    ///** packet number of packet protection specification (>=0) */
    //int jpwl_pprot_packno[JPWL_MAX_NO_PACKSPECS];
    ///** error protection methods for packets (0,1,16,32,37-128) */
    //int jpwl_pprot[JPWL_MAX_NO_PACKSPECS];
    ///** enables writing of ESD, (0=no/1/2 bytes) */
    //int jpwl_sens_size;
    ///** sensitivity addressing size (0=auto/2/4 bytes) */
    //int jpwl_sens_addr;
    ///** sensitivity range (0-3) */
    //int jpwl_sens_range;
    ///** sensitivity method for MH (-1=no,0-7) */
    //int jpwl_sens_MH;
    ///** tile number of sensitivity specification (>=0) */
    //int jpwl_sens_TPH_tileno[JPWL_MAX_NO_TILESPECS];
    ///** sensitivity methods for TPHs (-1=no,0-7) */
    //int jpwl_sens_TPH[JPWL_MAX_NO_TILESPECS];
    ///*@}*/
    ///* <<UniPG */

   printf("\n  DEPRECATED Digital Cinema compliance 0-not , 1-yes : %d\n", P.cp_cinema);
   printf("  Maximum size (in bytes) for each component: %d\n", P.max_comp_size);
   printf("  DEPRECATED: RSIZ_CAPABILITIES: %d\n", P.cp_rsiz);
   printf("  Tile part generation: >%o<\n",(int)P.tp_on);
   printf("  Flag for Tile part generation: >%o<\n", (int)P.tp_flag);
   printf("  MCT (multiple component transform): >%o<\n", (int)P.tcp_mct);
   printf("  Enable JPIP indexing? : %d\n", P.jpip_on);
   printf("  Naive implementation of MCT: ");
   if (! P.mct_data) printf("NULL\n");
   else printf("%x\n", P.mct_data);
   printf("  Maximum size (in bytes) for the whole codestream: %d\n", P.max_cs_size);
   printf("  RSIZ value: %d\n", P.rsiz);

   printf("------------------------------------------------------------------------\n");
}

void print_image_comp_summary(opj_image_comp_t C)
{
   size_t i, num_pixels;
   printf("\n    IMAGE COMPONENT:\n");
   printf("    XRsiz, YRsiz: %u %u\n", C.dx, C.dy);
   printf("    Data width, height: %u %u\n", C.w, C.h);
   printf("    x, y component offset: %u %u\n", C.x0, C.y0);
   printf("    Precision: %u\n", C.prec);
   printf("    Image depth in bits: %u\n", C.bpp);
   printf("    Signed (1) / unsigned (0): %u\n", C.sgnd);
   printf("    Number of decoded resolution: %u\n", C.resno_decoded);
   printf("    Factor: %u\n", C.factor);
   printf("    Image component data:");
   num_pixels = C.w * C.h;
   for (i = 0; i < 102; ++i)
   {
      if (i % 17 == 0) printf("\n       ");
      printf("%4d", C.data[i]);
   }
   printf("\n");
   printf("    Alpha channel: %u\n", C.alpha);
}

void print_image_info (opj_image_t *I)
{
   size_t i;
   printf(" -- Image struct: ------------------------------------------------------------\n");
   printf("  XOsiz, YOsiz: %u %u\n", I->x0, I->y0);
   printf("  Xsiz, Ysiz (width, height): %u %u\n", I->x1, I->y1);
   printf("  Color space: sRGB, Greyscale or YUV: %d\n", I->color_space);
   printf("  Number of components in the image: %u\n", I->numcomps);
   /** image components */
   for (i = 0; i < I->numcomps; ++i)
      print_image_comp_summary(I->comps[i]);

   printf("  Size of ICC profile: %u\n",  I->icc_profile_len);
   printf("  'restricted' ICC profile:");
   if (I->icc_profile_len == 0) printf(" NULL\n\n");
   else {
      printf("\n   >");
      for (i = 0; i < I->icc_profile_len; ++i)
         printf("%c", I->icc_profile_buf[i]);
      printf("<\n\n");
   }
}



void cleanup(void *l_data, opj_stream_t *l_stream, opj_codec_t *l_codec, opj_image_t *l_image)
{
   if (l_data)   free(l_data);
   if (l_stream) opj_stream_destroy(l_stream);
   if (l_image)  opj_destroy_image(l_image);
   if (l_codec)  opj_destroy_codec(l_codec);
}

void get_header_values(unsigned char *tile_in,
                       OPJ_UINT32 *nc,
                       int *iw, int *ih)
{
   int *buffer = (int *) tile_in;

   *nc = buffer[0];       // number of color components
   *iw = buffer[1];       // image width
   *ih = buffer[2];       // image height

/** Do we need these? **
   *bpp = buffer[3];  // Bits per pixel ASSUME default = 8
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
   opj_image_cmptparm_t l_image_params;   // only need one component
   opj_stream_t * l_stream;
   //OPJ_UINT32 l_nb_tiles_width, l_nb_tiles_height, l_nb_tiles;
   //OPJ_UINT32 l_data_size;
   //size_t len;

   //opj_image_cmptparm_t * l_current_param_ptr;
   OPJ_UINT32 i;
   OPJ_BYTE *l_data;

   OPJ_UINT32 num_comps = 1;
   OPJ_UINT32 compno;
   //int image_width;
   //int image_height;
   //int tile_width;
   //int tile_height;

// Fixed parameter values. May need to amend if different image types are used
   int comp_prec = 8;
   int irreversible = 0;            // 0 = lossless
   int cblockw_init = 64;
   int cblockh_init = 64;
   int numresolution = 6;
   OPJ_UINT32 offsetx = 0;
   OPJ_UINT32 offsety = 0;
   int quality_loss = 0;            // lossless

// Use whole tile as single "image"; tiling set in TileDB level

   opj_set_default_encoder_parameters(&l_param); // Default parameters

//  Set data pointer to the first pixel byte beyond the header values
   l_data = (OPJ_BYTE*) tile_in;
   //OPJ_UINT32 tilesize = tile_size_in - header_offset;

   /** number of quality layers in the stream */
   if (!quality_loss) { // Set for lossless compression
      l_param.cp_disto_alloc = 1;
      l_param.cp_fixed_alloc = 0;
      l_param.tcp_numlayers = 1;
      l_param.tcp_rates[0] = 0.0;
      l_param.tcp_distoratio[0] = 0.0;
   }
   else { // could be set from compression_level in array_schema
      l_param.tcp_numlayers = 1;
      l_param.cp_fixed_quality = 1;
      l_param.tcp_distoratio[0] = 20;
   }

   /* tile definitions parameters */
   /* position of the tile grid aligned with the image */
   l_param.cp_tx0 = 0;
   l_param.cp_ty0 = 0;

   l_param.tile_size_on = OPJ_FALSE;
   l_param.cp_tdx = 0;
   l_param.cp_tdy = 0;

   /* code block size */
   l_param.cblockw_init = cblockw_init;
   l_param.cblockh_init = cblockh_init;

   /* use irreversible encoding ?*/
   l_param.irreversible = irreversible;

   /** number of resolutions */
   l_param.numresolution = numresolution;

   /** progression order to use*/
   l_param.prog_order = OPJ_LRCP; // default

    /** Multiple component transform? **/
    if (num_comps == 1)
       l_param.tcp_mct = 0;
    else
       l_param.tcp_mct = 1; /* multiple components in image  UNUSED */

   /* image definition */
   l_image_params.bpp = 8;     // Does this need to be parameter?
   l_image_params.dx = 1;
   l_image_params.dy = 1;

   l_image_params.w = (OPJ_UINT32)tile_image_width_;
   l_image_params.h = (OPJ_UINT32)tile_image_height_;

   l_image_params.sgnd = 0;
   l_image_params.prec = (OPJ_UINT32)comp_prec;

   l_image_params.x0 = offsetx;
   l_image_params.y0 = offsety;

   /* should we do j2k or jp2 ?*/
   l_codec = opj_create_compress(OPJ_CODEC_JP2);
   // l_codec = opj_create_compress(OPJ_CODEC_J2K);

   if (! l_codec) {
      return print_errmsg("jpeg2k_compress: failed to setup codec");
   }

   l_image = opj_image_create(num_comps, &l_image_params, OPJ_CLRSPC_GRAY);

   if (! l_image) {
      cleanup(NULL, NULL, l_codec, NULL);
      return print_errmsg("jpeg2k_compress: failed to setup tile image");
   }

   l_image->x0 = offsetx;
   l_image->y0 = offsety;
   l_image->x1 = offsetx + (OPJ_UINT32)tile_image_width_;
   l_image->y1 = offsety + (OPJ_UINT32)tile_image_height_;

// Copy pixels to l_image component data from l_data (tile_in) 
// Can this be done by pointer copy??

   size_t num_bytes = tile_image_width_ * tile_image_height_ * sizeof(OPJ_INT32);
   memcpy(l_image->comps[0].data, l_data, num_bytes);

// CPB - to debug by comparing image information and parameters from good run
//print_image_info(l_image);
//print_parameters(l_param);
// CPB end

   if (! opj_setup_encoder(l_codec, &l_param, l_image)) {
      cleanup(NULL, NULL, l_codec, l_image);
      return print_errmsg("jpeg2k_compress: failed to setup the codec");
   }

   l_stream = (opj_stream_t *) opj_stream_create_default_memory_stream(OPJ_FALSE);
   if (! l_stream) {
      cleanup(NULL, NULL, l_codec, l_image);
      return print_errmsg("jpeg2k_compress: failed to create the memory stream");
   }

   if (! opj_start_compress(l_codec, l_image, l_stream)) {
      cleanup(NULL, l_stream, l_codec, l_image);
      return print_errmsg("jpeg2k_compress: failed to start compress");
   }

   if (! opj_encode(l_codec, l_stream)) {
      cleanup(NULL, l_stream, l_codec, l_image);
      return print_errmsg("jpeg2k_compress: failed to encode the tile");
   }

   if (! opj_end_compress(l_codec, l_stream)) {
      cleanup(NULL, l_stream, l_codec, l_image);
      return print_errmsg("jpeg2k_compress: failed to end compress");
   }

   *tile_compressed = opj_mem_stream_copy(l_stream, &tile_compressed_size);

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
      return print_errmsg("jpeg2k_decompress: failed to create decompress codec");
   }

   /** Setup stream with user data into stream **/
   l_stream = opj_stream_create_memory_stream((void *)tile_compressed, tile_compressed_size, OPJ_TRUE);
   if (!l_stream){
      cleanup(l_data, NULL, l_codec, NULL);
      return print_errmsg("jpeg2k_decompress: failed to create the memory stream");
   }

   /* Setup the decoder decoding parameters using user parameters */
   if (! opj_setup_decoder(l_codec, &l_param)) {
      cleanup(l_data, l_stream, l_codec, NULL);
      return print_errmsg("jpeg2k_decompress: failed to setup the decoder");
   }

   /* Read the main header of the codestream and if necessary the JP2 boxes*/
   if (! opj_read_header(l_stream, l_codec, &l_image)) {
      cleanup(l_data, l_stream, l_codec, NULL);
      return print_errmsg("jpeg2k_decompress: failed to read the header");
   }

   // PULL OUT numcomps, image_height, image_width from  l_stream
   image_height = l_image->comps[0].h;
   image_width = l_image->comps[0].w;
   numcomps = l_image->numcomps;

   if (!opj_set_decode_area(l_codec, l_image, 0, 0, 0, 0)){ // whole image
      cleanup(l_data, l_stream, l_codec, l_image);
      return print_errmsg("jpeg2k_decompress: failed to set the decoded area");
   }

   if (! opj_read_tile_header( l_codec, l_stream, &l_tile_index, &l_data_size,
                               &l_current_tile_x0, &l_current_tile_y0,
                               &l_current_tile_x1, &l_current_tile_y1,
                               &l_nb_comps, &l_go_on)) {
      cleanup(l_data, l_stream, l_codec, l_image);
      return print_errmsg("jpeg2k_decompress: failed to read_tile_header");
   }

   if (l_go_on) {
      if (l_data_size > l_max_data_size) {
         OPJ_BYTE *l_new_data = (OPJ_BYTE *) realloc(l_data, l_data_size);
         if (! l_new_data) {
            cleanup(l_data, l_stream, l_codec, l_image);
            char msg[64];
            sprintf(msg, "jpeg2k_decompress: failed to realloc new data of size %d", l_data_size);
            return print_errmsg(msg);
         }
         l_data = l_new_data;
         l_max_data_size = l_data_size;
      }

      if (! opj_decode_tile_data(l_codec,l_tile_index,l_data,l_data_size,l_stream)) {
         cleanup(l_data, l_stream, l_codec, l_image);
         return print_errmsg("jpeg2k_decompress: unable to decode_tile_data");
      }
   }  // if (l_go_on) 
   else {               // Problem in the opj_read_tile_header() 
       cleanup(l_data, l_stream, l_codec, l_image);
       return print_errmsg("jpeg2k_decompress: Current tile number and total number of tiles problem");
   }

   if (! opj_end_decompress(l_codec,l_stream)) {
      cleanup(l_data, l_stream, l_codec, l_image);
      return print_errmsg("jpeg2k_decompress: Failed to end_decompress correctly");
   }

   // Double check space in tile for decompressed data 
   size_t image_bytes = l_data_size;
   if (image_bytes > size_out) {
      char msg[100];
      sprintf(msg, "Tile decompresss size (%lu) greater than expected image size (%lu)", image_bytes, size_out);
      return print_errmsg(msg);
   }


   // Add header portion -- write ints into char array
   OPJ_UINT32 *buffer = (OPJ_UINT32 *)tile_out;
   OPJ_BYTE *ob = l_data;

   // Copy pixel data to output buffer
   size_t i;
   for (i = 0; i < l_data_size; ++i) {
      buffer[i] = (OPJ_UINT32) (0x000000FF & (*ob));
      ++ob;
   }

   //memcpy(ob, l_data, copy_bytes);

   /* Free memory */
   cleanup(l_data, l_stream, l_codec, l_image);

   // Success
   return TILEDB_CD_OK;
}

#endif
