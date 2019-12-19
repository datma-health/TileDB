/**
 * @file   tiledb_image_write_binary_2.cc
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
 * Example to write dense array to hold whole 165x150 pixel image using
 *   TILEDB_JPEG2K_RGB compression
 *
 */

#include <iostream>
#include <fstream>
#include "examples.h"
#include <stdlib.h>

#define HEADER_SIZE 3

using namespace std;

int *read_image(const char* filename, size_t num_bytes)
{
/**
 *  Tissue image file doesn't have header bytes, so those will be added
 * here to ensure the image buffer is configured for TILEDB_JPEG2K_RGB 
 * compression.
**/
   int *image_buffer = (int *)malloc(num_bytes);
   size_t bytes_to_read = num_bytes - (HEADER_SIZE * sizeof(int));
   int *l_buffer = image_buffer;
   *l_buffer = 3;   ++l_buffer;  // Add number of components
   *l_buffer = 165; ++l_buffer;  // Add number of width
   *l_buffer = 150; ++l_buffer;  // Add number of height
   FILE *infile;
   infile = fopen(filename, "rb"); // r for read, b for binary
   fread(l_buffer, bytes_to_read, 1, infile);
   fclose(infile);

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
      "my_workspace/image_arrays/tissue165150",  // Array name
      TILEDB_ARRAY_WRITE,                        // Mode
      NULL,                                      // Entire domain
      NULL,                                      // All attributes
      0));                                       // Number of attributes

  // Prepare cell buffer
  size_t num_comps = 3;
  size_t width  = 165;
  size_t height = 150;
  size_t full_image_bytes = (num_comps * width * height + HEADER_SIZE) * sizeof(int);

  std::string filename = std::string(TILEDB_EXAMPLE_DIR)+"/data/tissue165x150.bin";

  int * buffer_image = read_image(filename.c_str(), full_image_bytes);

  const void* buffers[] =
  {
      buffer_image             // whole image (plus header)
  };

  size_t buffer_sizes[] =
  {
      full_image_bytes        // buffer size 
  };

  // Write to array
  CHECK_RC(tiledb_array_write(tiledb_array, buffers, buffer_sizes));

  // Finalize array
  CHECK_RC(tiledb_array_finalize(tiledb_array));

  // Finalize context
  CHECK_RC(tiledb_ctx_finalize(tiledb_ctx));

  return 0;
}
