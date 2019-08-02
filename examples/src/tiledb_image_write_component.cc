/**
 * @file   tiledb_image_write_component.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Example to write dense array to hold 300x300 pixel image of
 *    3x3 color palette divided into component "planes"
 *
 */

#include "examples.h"

char *build_image(size_t num_comps, size_t height, size_t width)
{
   size_t component_size = height * width + 12;
   size_t buffer_size = num_comps * component_size;
   char *image_buffer = (char *)malloc(buffer_size);
   char *l_data = image_buffer;

   char R[10], G[10], B[10];
//        Black,              Red,                Orange
   R[0] = char(  0);   R[1] = char(201);   R[2] = char(234);
   G[0] = char(  0);   G[1] = char( 23);   G[2] = char( 85);
   B[0] = char(  0);   B[1] = char( 30);   B[2] = char(  6);

//        Pink,               White,              Yellow
   R[3] = char(233);   R[4] = char(255);   R[5] = char(255);
   G[3] = char( 82);   G[4] = char(255);   G[5] = char(234);
   B[3] = char(149);   B[4] = char(255);   B[5] = char(  0);

//        Purple,             Blue,               Green
   R[6] = char(101);   R[7] = char( 12);   R[8] = char(  0);
   G[6] = char( 49);   G[7] = char(  2);   G[8] = char( 85);
   B[6] = char(142);   B[7] = char(196);   B[8] = char( 46);

//         Grey (unused)
   R[9] = char(130);
   G[9] = char(130);
   B[9] = char(130);

   // Insert Red component "header" info into image buffer
   int *header = (int *)l_data;
   header[0] = 1;       l_data += sizeof(int);
   header[1] = height;  l_data += sizeof(int);
   header[2] = width;   l_data += sizeof(int);
   // copy R component   
   for (int c = 0; c < 9; c += 3) {
      for (int i = 0; i < 100; ++i) {
         for (int j = c; j < c+3; ++j) {
            for (int k = 0; k < 100; ++k) {
               *l_data = R[j]; l_data++;
            }
         }
      }
   }

   // Insert Green compoent "header" info into image buffer
   header = (int *)l_data;
   header[0] = 1;       l_data += sizeof(int);
   header[1] = height;  l_data += sizeof(int);
   header[2] = width;   l_data += sizeof(int);
   // copy G component   
   for (int c = 0; c < 9; c += 3) {
      for (int i = 0; i < 100; ++i) {
         for (int j = c; j < c+3; ++j) {
            for (int k = 0; k < 100; ++k) {
               *l_data = G[j]; l_data++;
            }
         }
      }
   }

   // Insert Blue compoent "header" info into image buffer
   header = (int *)l_data;
   header[0] = 1;       l_data += sizeof(int);
   header[1] = height;  l_data += sizeof(int);
   header[2] = width;   l_data += sizeof(int);
   // copy B component   
   for (int c = 0; c < 9; c += 3) {
      for (int i = 0; i < 100; ++i) {
         for (int j = c; j < c+3; ++j) {
            for (int k = 0; k < 100; ++k) {
               *l_data = B[j]; l_data++;
            }
         }
      }
   }

   return image_buffer;
}

int main(int argc, char *argv[]) {
   // Initialize context with home dir if specified in command line, else
   // initialize with the default configuration parameters
   TileDB_CTX* tiledb_ctx;
   if (argc > 1) {
      TileDB_Config tiledb_config;
      tiledb_config.home_ = argv[1];
      CHECK_RC(tiledb_ctx_init(&tiledb_ctx, &tiledb_config));
   } 
   else {
      CHECK_RC(tiledb_ctx_init(&tiledb_ctx, NULL));
   }

   // Initialize array
   TileDB_Array* tiledb_array;
   CHECK_RC(tiledb_array_init(
      tiledb_ctx,                                // Context 
      &tiledb_array,                             // Array object
      "my_workspace/image_arrays/comp_image",    // Array name
      TILEDB_ARRAY_WRITE,                        // Mode
      NULL,                                      // Entire domain
      NULL,                                      // All attributes
      0));                                        // Number of attributes

   // Prepare cell buffer
   size_t num_comps = 3;
   size_t height = 300;
   size_t width  = 300;
   size_t image_bytes = num_comps * (12 + height * width);
 
   char * buffer_image = build_image(num_comps, height, width);
   
   const void* buffers[] = { buffer_image };
   size_t buffer_sizes[] = 
   { 
       image_bytes        // sizeof( buffer_image )  
   };
 
   // Write to array
   CHECK_RC(tiledb_array_write(tiledb_array, buffers, buffer_sizes));
 
   // Finalize array
   CHECK_RC(tiledb_array_finalize(tiledb_array));
 
   // Finalize context
   CHECK_RC(tiledb_ctx_finalize(tiledb_ctx));

   return 0;
}
