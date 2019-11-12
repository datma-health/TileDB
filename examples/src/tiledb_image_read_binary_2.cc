/**
 * @file   tiledb_image_read_binary_2.cc
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
 * Example to read dense array holding whole 165x150 pixel image
 *
 */

#include "examples.h"

void check_results(char *buffer_image, size_t num_bytes)
{
   size_t i, errors = 0;
   std::string filename = std::string(TILEDB_EXAMPLE_DIR)+"/data/tissue165x150.bin";
   char *original_image = (char *)malloc(num_bytes);
   FILE *infile;
   infile = fopen(filename.c_str(), "rb"); // r for read, b for binary
   fread(original_image, num_bytes, 1, infile);
   fclose(infile);

   FILE *errfile;
   errfile = fopen("t165x150_err", "w");

   unsigned int b, o;
   for (i = 0; i < num_bytes; ++i) {
      b = buffer_image[i];
      o = original_image[i];
      if (b != o) {
         fprintf(errfile, "%8lu: %4u  %4u   %2dc\n", i, b, o, abs(b - o));
         ++errors;
      }
   }
   fclose(errfile);

   if (!errors)
      printf("\nCheck SUCCESSFUL\n");
   else
      printf("\nCheck FAILED with %lu errors (%5.2f%%)\n",
              errors, 100*((float)errors/num_bytes));
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
      "my_workspace/image_arrays/tissue165150",         // Array name
      TILEDB_ARRAY_READ,                                // Mode
      NULL,                                             // Whole domain
      NULL,                                             // All attributes
      0));                                              // Number of attributes

   // Prepare cell buffer 
   size_t num_comps = 3;
   size_t width  = 165;
   size_t height = 150;
   size_t image_bytes = (num_comps * width * height + 3) * sizeof(int);
 
   int *buffer_image = (int*)malloc(image_bytes);
   void* buffers[] = { buffer_image };
   size_t buffer_sizes[] = 
   { 
       image_bytes             // sizeof(buffer_image)  
   };

   // Read from array
   CHECK_RC(tiledb_array_read(tiledb_array, buffers, buffer_sizes)); 

   check_results((char*)buffer_image, image_bytes); 

   // Finalize the array
   CHECK_RC(tiledb_array_finalize(tiledb_array));
 
   /* Finalize context. */
   CHECK_RC(tiledb_ctx_finalize(tiledb_ctx));

   FILE *bin_ptr;
   bin_ptr = fopen("tissue_decode.bin","wb");
   fwrite(buffer_image, image_bytes, 1, bin_ptr);
   fclose(bin_ptr);

   return 0;
}
