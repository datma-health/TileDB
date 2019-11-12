/**
 * @file   tiledb_image_read_whole.cc
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
 * Example to read dense array holding whole 300x300 pixel image of
 *    3x3 color palette
 */

#include "examples.h"

void check_results(int *buffer_image, int* image_info)
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

   // Print midpoint RGB value of each palette block and check
   int *l_data = buffer_image;
   int num_comps, height, width;
   int num_parts, tileW1, tileH1, tileW2, tileH2;

   num_comps = image_info[0];
   width = image_info[1];
   height = image_info[2];
   num_parts = image_info[3];
   tileW1 = image_info[4];
   tileH1 = image_info[5];
   tileW2 = image_info[6];
   tileH2 = image_info[7];

   l_data += 3; // skip header

   size_t c, i, j, k;
   int Rerrs[9] = {0,0,0,0,0,0,0,0,0};
   int Gerrs[9] = {0,0,0,0,0,0,0,0,0};
   int Berrs[9] = {0,0,0,0,0,0,0,0,0};

printf("Tiling attributes:\n  Original comp, w, h : %d  %d x %d\n", num_comps, width, height);
printf("  Number of partitions: %d\n", num_parts);
   if (num_parts > 1) {
      printf("        %3d x %3d     %3d x %3d\n        %3d x %3d     %3d x %3d\n\n", tileW1, tileH1, tileW2, tileH1, tileW1, tileH2, tileW2, tileH2);
   }
   else {
      printf("        %3d x %3d                \n                               \n\n", tileW2, tileH2);
   }

   printf("Expected Image Palette RGB values: %d components\n", num_comps);
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

   int p = 0, q =  width*height, r = 2*width*height;
   int errors = 0;
   for (c = 0; c < 9; c+=3) {
      for (i = 0; i < width/3; ++i) {
         for (j = c; j < c+3; ++j) {
            for (k = 0; k < height/3; ++k) {
               if (l_data[p++] != R[j]) { ++Rerrs[j]; ++errors; }
               if (l_data[q++] != G[j]) { ++Gerrs[j]; ++errors; }
               if (l_data[r++] != B[j]) { ++Berrs[j]; ++errors; }
            }
         }
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
   } else {
       CHECK_RC(tiledb_ctx_init(&tiledb_ctx, NULL));
   }

   // Initialize array 
   TileDB_Array* tiledb_array;
   CHECK_RC(tiledb_array_init(
      tiledb_ctx,                                       // Context
      &tiledb_array,                                    // Array object
      "my_workspace/image_arrays/wholeimage_attr",      // Array name
      TILEDB_ARRAY_READ,                                // Mode
      NULL,                                             // Whole domain
      NULL,                                             // All attributes
      0));                                              // Number of attributes

   // Prepare cell buffer 
   int num_comps = 3;
   int width  = 300;
   int height = 300;
   int num_parts = 1;
   int tileW1 = 300;
   int tileH1 = 300;
   int tileW2 = 300;
   int tileH2 = 300;

   size_t image_bytes = (num_comps * width * height + 3) * sizeof(int);
   size_t info_bytes = 8 * sizeof(int);
 
   int *buffer_image = (int*)malloc(image_bytes);
   int image_info[8];
   void* buffers[] = { buffer_image, image_info };
   size_t buffer_sizes[] = 
   { 
       image_bytes,        // sizeof(buffer_image)  
       info_bytes          // sizeof( image_info )

   };

   // Read from array
   CHECK_RC(tiledb_array_read(tiledb_array, buffers, buffer_sizes)); 

   check_results(buffer_image, image_info); 

   // Finalize the array
   CHECK_RC(tiledb_array_finalize(tiledb_array));
 
   /* Finalize context. */
   CHECK_RC(tiledb_ctx_finalize(tiledb_ctx));

   return 0;
}
