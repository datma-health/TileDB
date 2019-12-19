/**
 * @file   tiledb_image_read_panel_RGB.cc
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
 * Example to read dense array holding 300x300 pixel image of
 *    3x3 color palette as 100x100 panels using TILEDB_JPEG2K_RGB compression
 */

#include "examples.h"
#include <stdlib.h>
#define HEADER_SIZE 3

void check_results(int *buffer_image)
{

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

//       Grey (unused}
   R[9] = 130;
   G[9] = 130;
   B[9] = 130;

   int *l_data = buffer_image;
   int num_comps = l_data[0];
   int width = l_data[1];
   int height = l_data[2];
   int num_panels = 9;

   size_t i, j;
   int Rerrs[9] = {0,0,0,0,0,0,0,0,0};
   int Gerrs[9] = {0,0,0,0,0,0,0,0,0};
   int Berrs[9] = {0,0,0,0,0,0,0,0,0};


   printf("Expected Image Palette RGB values:\n");
   printf("----------------------------\n");
   for (i = 0; i < 9; i+=3) {
     printf("| R: %3u | R: %3u | R: %3u |\n",
                 R[i],    R[i+1],  R[i+2]);
     printf("| G: %3u | G: %3u | G: %3u |\n",
                 G[i],    G[i+1],  G[i+2]);
     printf("| B: %3u | B: %3u | B: %3u |\n",
                 B[i],    B[i+1],  B[i+2]);
     printf("----------------------------\n");
   }

   int errors = 0;
   for (i = 0; i < num_panels; ++i) {
      l_data += 3;   // skip over panel header
      for (j = 0; j < width*height; ++j) {
         if (*l_data != R[i]) { ++Rerrs[i]; ++errors; }
         ++l_data;
      }
      for (j = 0; j < width*height; ++j) {
         if (*l_data != G[i]) { ++Gerrs[i]; ++errors; }
         ++l_data;
      }
      for (j = 0; j < width*height; ++j) {
         if (*l_data != B[i]) { ++Berrs[i]; ++errors; }
         ++l_data;
      }
   }

   if (!errors) printf("\nCheck SUCCESSFUL\n");
   else {
      printf("\nERRORS found; Counts: \n");
      for (i = 0; i < 9; ++i) {
         if (Rerrs[i] + Gerrs[i] + Berrs[i]) {
            printf("   Panel %lu errors: ", i);
            if (Rerrs[i]) printf("R - %d  ", Rerrs[i]);
            if (Gerrs[i]) printf("G - %d  ", Gerrs[i]);
            if (Berrs[i]) printf("B - %d  ", Berrs[i]);
            printf("\n");
         }
      }
   }
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
      tiledb_ctx,                                  // Context
      &tiledb_array,                               // Array object
      "my_workspace/image_arrays/panelimage_RGB",  // Array name
      TILEDB_ARRAY_READ,                           // Mode
      NULL,                                        // Whole domain
      NULL,                                        // All attributes
      0));                                         // Number of attributes

   // Prepare cell buffer
   size_t num_comps = 3;
   size_t panel_width  = 100;  // per panel
   size_t panel_height = 100;  // per panel
   size_t num_panels = 9;
   size_t num_panel_pixels = num_comps * panel_width * panel_height;
   size_t buffer_bytes = num_panels * ((num_panel_pixels + HEADER_SIZE) * sizeof(int));
 
   int* buffer_image = (int*)malloc(buffer_bytes);
 
   void* buffers[] = 
   { 
     buffer_image         // whole image
   };

   size_t buffer_sizes[] =
   {
       buffer_bytes      // whole image plus headers
   };

   // Read from array
   CHECK_RC(tiledb_array_read(tiledb_array, buffers, buffer_sizes)); 

   check_results(buffer_image); 

   // Finalize the array
   CHECK_RC(tiledb_array_finalize(tiledb_array));
 
   /* Finalize context. */
   CHECK_RC(tiledb_ctx_finalize(tiledb_ctx));
 
   return 0;
}
