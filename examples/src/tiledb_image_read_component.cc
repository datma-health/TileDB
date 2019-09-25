/**
 * @file   tiledb_image_read_component.cc
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
 *    3x3 color palette divided into component "planes"
 */

#include "examples.h"

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
   size_t num_comps, width, height;
   int *header = (int*) buffer_image;
   num_comps = header[0];
   width =     header[1];
   height =    header[2];

   size_t panel_width  = width / 3;
   size_t panel_height = height / 3;

   size_t c, i, j, k;
   int Rerrs[9] = {0,0,0,0,0,0,0,0,0};
   int Gerrs[9] = {0,0,0,0,0,0,0,0,0};
   int Berrs[9] = {0,0,0,0,0,0,0,0,0};

   printf("\nExpected Image Palette Red component values: %lu components\n", num_comps);
   printf("----------------------------\n");
   for (i = 0; i < 9; i+=3) {
     printf("| R: %3u | R: %3u | R: %3u |\n",
                 R[i],    R[i+1],  R[i+2]);
     printf("----------------------------\n");
   }


   l_data += 3;  // skip header

   int errors = 0;
   // Check R component
   for (c = 0; c < 9; c += 3) {
      for (i = 0; i < panel_height; ++i) {
         for (j = c; j < c+3; ++j) {
            for (k = 0; k < panel_width; ++k) {
               if (*l_data != R[j]) {
                  ++Rerrs[j];
                  ++errors;
               }
               ++l_data;
            }
         }
      }
   }
   if (!errors) printf("Red component Check SUCCESSFUL\n");
   else {
      printf("Red Component ERRORS found; Counts: \n");
      for (i = 0; i < 9; ++i) {
         if (Rerrs[i]) {
            printf("   Panel %lu errors: ", i);
            printf("R - %d  \n", Rerrs[i]);
         }
      }
   }

   printf("\nExpected Image Palette Green component values: %lu components\n", num_comps);
   printf("----------------------------\n");
   for (i = 0; i < 9; i+=3) {
     printf("| G: %3u | G: %3u | G: %3u |\n",
                 G[i],    G[i+1],  G[i+2]);
     printf("----------------------------\n");
   }
   l_data += 3;  // skip header

   errors = 0;
   // check G component
   for (c = 0; c < 9; c += 3) {
      for (i = 0; i < panel_height; ++i) {
         for (j = c; j < c+3; ++j) {
            for (k = 0; k < panel_width; ++k) {
               if (*l_data != G[j]) {
                  ++Gerrs[j];
                  ++errors;
               }
               ++l_data;
            }
         }
      }
   }
   if (!errors) printf("Green component Check SUCCESSFUL\n");
   else {
      printf("Green Component ERRORS found; Counts: \n");
      for (i = 0; i < 9; ++i) {
         if (Gerrs[i]) {
            printf("   Panel %lu errors: ", i);
            printf("G - %d  \n", Gerrs[i]);
         }
      }
   }

   printf("\nExpected Image Palette Blue component values: %lu components\n", num_comps);
   printf("----------------------------\n");
   for (i = 0; i < 9; i+=3) {
     printf("| B: %3u | B: %3u | B: %3u |\n",
                 B[i],    B[i+1],  B[i+2]);
     printf("----------------------------\n");
   }
   l_data += 3;  // skip header

   errors = 0;
   // check B component
   for (c = 0; c < 9; c += 3) {
      for (i = 0; i < panel_height; ++i) {
         for (j = c; j < c+3; ++j) {
            for (k = 0; k < panel_width; ++k) {
               if (*l_data != B[j]) {
                  ++Berrs[j];
                  ++errors;
               }
               ++l_data;
            }
         }
      }
   }
   if (!errors) printf("Blue component Check SUCCESSFUL\n");
   else {
      printf("Blue Component ERRORS found; Counts: \n");
      for (i = 0; i < 9; ++i) {
         if (Berrs[i]) {
            printf("   Panel %lu errors: ", i);
            printf("B - %d  \n", Berrs[i]);
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
      tiledb_ctx,                                       // Context
      &tiledb_array,                                    // Array object
      "my_workspace/image_arrays/comp_image",           // Array name
      TILEDB_ARRAY_READ,                                // Mode
      NULL,                                             // Whole domain
      NULL,                                             // All attributes
      0));                                              // Number of attributes

   // Prepare cell buffer 
   size_t num_comps = 3;
   size_t width  = 300; 
   size_t height = 300;  
   size_t num_component_elements = width * height + 3;
   size_t image_bytes = (num_comps * num_component_elements) * sizeof(int);
 
   int *buffer_image = (int*)malloc(image_bytes);
   void* buffers[] = { buffer_image };
   size_t buffer_sizes[] = 
   { 
       image_bytes             // sizeof(buffer_image)  
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
