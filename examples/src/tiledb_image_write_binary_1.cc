/**
 * @file   tiledb_image_write_binary_1.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 * @copyright Copyright (c) 2019-2020 Omics Data Automation, Inc.
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
 * Example to write dense array to hold whole 150x165 pixel image
 *
 */

#include <iostream>
#include <fstream>
#include "examples.h"
#include <stdlib.h>

using namespace std;

uint8_t* read_image(const char* filename, size_t image_pixels)
{
   size_t file_image_bytes = image_pixels * sizeof(int);
   size_t image_bytes = image_pixels * sizeof(uint8_t);
   int *read_image_buffer = (int *)malloc(file_image_bytes); // number of file bytes
   uint8_t *image_buffer = (uint8_t *)malloc(image_bytes);   // number of pixel bytes
   FILE *infile;
   infile = fopen(filename, "rb"); // r for read, b for binary
   fread(read_image_buffer, file_image_bytes, 1, infile);
   fclose(infile);

   for (size_t i = 0; i < image_pixels; ++i)
       image_buffer[i] = read_image_buffer[i];
      
   free(read_image_buffer);
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
      "my_workspace/image_arrays/tissue150165",  // Array name
      TILEDB_ARRAY_WRITE,                        // Mode
      NULL,                                      // Entire domain
      NULL,                                      // All attributes
      0));                                       // Number of attributes

  // Prepare cell buffer
  size_t num_comps = 3;
  size_t width  = 150;
  size_t height = 165;
  size_t full_image_pixels = num_comps * width * height;
  size_t buffer_image_bytes = (width * height) * sizeof(uint8_t);

  std::string filename = std::string(TILEDB_EXAMPLE_DIR)+"/data/tissue150x165.bin";

  uint8_t* buffer_image = read_image(filename.c_str(), full_image_pixels);

  const void* buffers[] = 
  { 
      &buffer_image[0*width*height],    // R buffer
      &buffer_image[1*width*height],    // G buffer
      &buffer_image[2*width*height]     // B buffer
  };

  size_t buffer_sizes[] = 
  { 
      buffer_image_bytes,       // sizeof( R buffer_image )  
      buffer_image_bytes,       // sizeof( G buffer_image )  
      buffer_image_bytes        // sizeof( B buffer_image )  
  };

  // Write to array
  CHECK_RC(tiledb_array_write(tiledb_array, buffers, buffer_sizes));

  // Finalize array
  CHECK_RC(tiledb_array_finalize(tiledb_array));

  // Finalize context
  CHECK_RC(tiledb_ctx_finalize(tiledb_ctx));

  return 0;
}
