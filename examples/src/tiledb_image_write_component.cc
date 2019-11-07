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

int *build_image(size_t num_comps, size_t width, size_t height)
{
   size_t component_size = height * width + 3;
   size_t buffer_size = (num_comps * component_size) * sizeof(int);
   int *image_buffer = (int *)malloc(buffer_size);
   int *l_data = image_buffer;
   size_t c, i, j, k;

   int R[10], G[10], B[10];
//       Black,        Red,          Orange
   R[0] =   0;   R[1] = 201;   R[2] = 234;
   G[0] =   0;   G[1] =  23;   G[2] =  85;
   B[0] =   0;   B[1] =  30;   B[2] =   6;

//       Pink,         White,        Yellow
   R[3] = 233;   R[4] = 255;   R[5] = 255;
   G[3] =  82;   G[4] = 255;   G[5] = 234;
   B[3] = 149;   B[4] = 255;   B[5] =   0;

//       Purple,       Blue,         Green
   R[6] = 101;   R[7] =  12;   R[8] =   0;
   G[6] =  49;   G[7] =   2;   G[8] =  85;
   B[6] = 142;   B[7] = 196;   B[8] =  46;

//       Grey (unused)
   R[9] = 130;
   G[9] = 130;
   B[9] = 130;

   // Insert Red component "header" info into image buffer
   *l_data = 1;       ++l_data;
   *l_data = width;   ++l_data;
   *l_data = height;  ++l_data;
    size_t panel_width  = width / 3;
    size_t panel_height = height / 3;
   
   // copy R component   
   for (c = 0; c < 9; c += 3) {
      for (i = 0; i < panel_height; ++i) {
         for (j = c; j < c+3; ++j) {
            for (k = 0; k < panel_width; ++k) {
               *l_data = R[j]; ++l_data;
            }
         }
      }
   }

   // Insert Green component "header" info into image buffer
   *l_data = 1;       ++l_data;
   *l_data = width;   ++l_data;
   *l_data = height;  ++l_data;
  
   // copy G component   
   for (c = 0; c < 9; c += 3) {
      for (i = 0; i < panel_height; ++i) {
         for (j = c; j < c+3; ++j) {
            for (k = 0; k < panel_width; ++k) {
               *l_data = G[j]; ++l_data;
            }
         }
      }
   }

   // Insert Blue component "header" info into image buffer
   *l_data = 1;       ++l_data;
   *l_data = width;   ++l_data;
   *l_data = height;  ++l_data;
  
   // copy B component   
   for (c = 0; c < 9; c += 3) {
      for (i = 0; i < panel_height; ++i) {
         for (j = c; j < c+3; ++j) {
            for (k = 0; k < panel_width; ++k) {
               *l_data = B[j]; l_data++;
            }
         }
      }
   }

   return image_buffer;
}

int main(int argc, char *argv[]) {
#ifdef ENABLE_JPEG2K
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
   size_t num_component_elements = width * height + 3;
   size_t image_bytes = (num_comps * num_component_elements) * sizeof(int);
 
   int * buffer_image = build_image(num_comps, width, height);
   
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

#else
  printf("*** %s test unable to run; \n*** Enable JPEG2K library to execute\n", argv[0]);
#endif
   return 0;
}
