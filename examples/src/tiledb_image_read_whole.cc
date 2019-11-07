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

void check_results(int *buffer_image)
{

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
   
   // Print midpoint RGB value of each palette block and check
   int *l_data = buffer_image;
   size_t num_comps, height, width;
   int *header = (int*) buffer_image;
   num_comps = header[0];
   height = header[1];
   width = header[2];
   l_data += 12; // skip header

   int i, j;
   char Rvalue[9];
   char Gvalue[9];
   char Bvalue[9];
   size_t Roffset = 0*height*width;
   size_t Goffset = 1*height*width;
   size_t Boffset = 2*height*width;

   int t = 0;
   for (i = 50; i < 300; i+=100) {
      for (j = 50; j < 300; j+=100) {
         Rvalue[t] = l_data[Roffset + i * width + j];
         Gvalue[t] = l_data[Goffset + i * width + j];
         Bvalue[t] = l_data[Boffset + i * width + j];
         ++t;
      }
   }

   printf("Image Palette RGB values: %lu components\n", num_comps);
   printf("----------------------------\n");
   for (i = 0; i < 9; i+=3) {
     printf("| R: %3u | R: %3u | R: %3u |\n",
             0x000000FF & Rvalue[i],
             0x000000FF & Rvalue[i+1],
             0x000000FF & Rvalue[i+2]);
     printf("| G: %3u | G: %3u | G: %3u |\n",
             0x000000FF & Gvalue[i],
             0x000000FF & Gvalue[i+1],
             0x000000FF & Gvalue[i+2]);
     printf("| B: %3u | B: %3u | B: %3u |\n",
             0x000000FF & Bvalue[i],
             0x000000FF & Bvalue[i+1],
             0x000000FF & Bvalue[i+2]);
     printf("----------------------------\n");
   }

   int errors = 0;
   for (i = 0; i < 9; ++i) {
      if (Rvalue[i] != R[i]) {
         printf("ERROR: Red[%d] should be %3lu\n",i,(size_t)R[i]);
         ++errors;
      }
      if (Gvalue[i] != G[i]) {
         printf("ERROR: Green[%d] should be %3lu\n",i,(size_t)G[i]);
         ++errors;
      }
      if (Bvalue[i] != B[i]) {
         printf("ERROR: Blue[%d] should be %3lu\n",i,(size_t)B[i]);
         ++errors;
      }
   }
   if (!errors) printf("\nCheck SUCCESSFUL\n");

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
   } else {
       CHECK_RC(tiledb_ctx_init(&tiledb_ctx, NULL));
   }

   // Initialize array 
   TileDB_Array* tiledb_array;
   CHECK_RC(tiledb_array_init(
      tiledb_ctx,                                       // Context
      &tiledb_array,                                    // Array object
      "my_workspace/image_arrays/wholeimage",           // Array name
      TILEDB_ARRAY_READ,                                // Mode
      NULL,                                             // Whole domain
      NULL,                                             // All attributes
      0));                                              // Number of attributes

   // Prepare cell buffer 
   size_t num_comps = 3;
   size_t width  = 300;
   size_t height = 300;
   size_t image_bytes = num_comps * width * height * sizeof(int) + 12;
 
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
 
#else
  printf("*** %s test unable to run; \n*** Enable JPEG2K library to execute\n", argv[0]);
#endif
   return 0;
}
