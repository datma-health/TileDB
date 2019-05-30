/**
 * @file   tiledb_array_write_whole.cc
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
 * Example to write dense array to hold whole 300x300 pixel image of
 *    3x3 color palette
 */

#include "examples.h"

// Black, Red, Orange, Pink, White, Yellow, Purple, Blue, Green, Grey
   char R[10] = {  0, 201, 234, 233, 255, 255, 101,  12,   0, 130};
   char G[10] = {  0,  23,  85,  82, 255, 234,  49,   2,  85, 130};
   char B[10] = {  0,  30,   6, 149, 255,   0, 142, 196,  46, 130};

char *build_image(size_t num_comps, size_t height, size_t width)
{
   size_t buffer_size = num_comps * height * width + 12;
   char *image_buffer = (char *)malloc(buffer_size);
   char *l_data = image_buffer;

   // Insert "header" info into image buffer
   int *header = (int *)image_buffer; 
   header[0] = num_comps; l_data += sizeof(int);
   header[1] = height;    l_data += sizeof(int);
   header[2] = width;     l_data += sizeof(int);

   int p = 0, q = width*height, r = 2*width*height;
   for (int c = 0; c < 9; c += 3) {
      for (int i = 0; i < 100; ++i) {
         for (int j = c; j < c+3; ++j) {
            for (int k = 0; k < 100; ++k) {
               l_data[p++] = R[j];
               l_data[q++] = G[j];
               l_data[r++] = B[j];
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
  } else {
    CHECK_RC(tiledb_ctx_init(&tiledb_ctx, NULL));
  }

  // Initialize array
  TileDB_Array* tiledb_array;
  CHECK_RC(tiledb_array_init(
      tiledb_ctx,                                // Context 
      &tiledb_array,                             // Array object
      "my_workspace/image_arrays/wholeimage",    // Array name
      TILEDB_ARRAY_WRITE,                        // Mode
      NULL,                                      // Entire domain
      NULL,                                      // All attributes
      0));                                        // Number of attributes

  // Prepare cell buffer
  size_t num_comps = 3;
  size_t height = 300;
  size_t width  = 300;
  size_t image_bytes = num_comps * height * width + 12;

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
