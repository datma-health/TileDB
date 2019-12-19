/**
 * @file   tiledb_image_create_panels_RGB.cc
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
 * Example to create dense array to hold 300x300 pixel image of 
 *   3x3 color palette divided into 100x100 panels using TILEDB_JPEG2K_RGB
 *   compression
 */

#include "examples.h"

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

  // Prepare parameters for array schema
  const char* array_name = "my_workspace/image_arrays/panelimage_RGB";
  const char* attributes[] = { "imageRGB" };          // one image
  const char* dimensions[] = { "width", "height" };   // 2-D image dimensions
  int64_t domain[] = 
  { 
      0, 2,                       // width
      0, 2                        // height
  };                
  const int cell_val_num[] = 
  { 
     30003                       // integer pixels in panel plus header
                                 // 3 x (100 x 100) + 3
  };
  const int compression[] = 
  { 
#ifdef ENABLE_JPEG2K_RGB
        TILEDB_JPEG2K_RGB,        // image 
#else
        TILEDB_NO_COMPRESSION,    // image as raw pixels
#endif

        TILEDB_NO_COMPRESSION     // coordinates
  };
  int64_t tile_extents[] = 
  { 
      1,                          // width
      1                           // height
  };               
  const int types[] = 
  { 
      TILEDB_INT32,               // image
      TILEDB_INT64                // coordinates
  };
  
  // Set array schema
  TileDB_ArraySchema array_schema;
  CHECK_RC(tiledb_array_set_schema(
      &array_schema,              // Array schema struct 
      array_name,                 // Array name 
      attributes,                 // Attributes 
      1,                          // Number of attributes 
      1,                          // Capacity 
      TILEDB_ROW_MAJOR,           // Cell order 
      cell_val_num,               // Number of cell values per attribute  
      compression,                // Compression
      NULL,                       // Compression level, use defaults
      1,                          // Dense array
      dimensions,                 // Dimensions
      2,                          // Number of dimensions
      domain,                     // Domain
      4*sizeof(int64_t),          // Domain length in bytes
      tile_extents,               // Tile extents
      2*sizeof(int64_t),          // Tile extents length in bytes 
      TILEDB_ROW_MAJOR,           // Tile order
      types                       // Types
				   ));

  // Create array
  CHECK_RC(tiledb_array_create(tiledb_ctx, &array_schema));

  // Free array schema
  CHECK_RC(tiledb_array_free_schema(&array_schema));

  /* Finalize context. */
  CHECK_RC(tiledb_ctx_finalize(tiledb_ctx));

  return 0;
}
