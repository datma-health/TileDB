/**
 * @file   tiledb_array_3d.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 * @copyright Copyright (c) 2021 Omics Data Automation, Inc.
 * @copyright Copyright (c) 2023 dātma, inc™
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
 * It shows how to create, write, and read a sparse 3d array.
 */

#include "examples.h"

int main(int argc, char *argv[]) {

  // ================================== ARRAY CREATE ======================

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
  const char* array_name = "my_workspace/sparse_arrays/my_3d_array";

  // Delete the array if it exists
  if (is_dir(tiledb_ctx, array_name)) {
    CHECK_RC(tiledb_delete(tiledb_ctx, array_name));
  }

  const char* attributes[] = { "a1", "a2" };        // Two attributes
  const char* dimensions[] = { "d1", "d2", "d3" };  // Three dimensions
  int64_t domain[] = 
  { 
      0, 4,                       // d1
      0, 4,                       // d2
      0, 4                        // d3
  };                
  const int cell_val_num[] = 
  { 
      1,                          // a1
      TILEDB_VAR_NUM              // a2 
  };
  const int compression[] =
  { 
        TILEDB_GZIP
        +TILEDB_BIT_SHUFFLE,      // a1
        TILEDB_GZIP,              // a2
        TILEDB_GZIP
        +TILEDB_DELTA_ENCODE      // coordinates
  };
  const int offsets_compression[] =
  {
        0,                        // a1 - DON'T CARE
        TILEDB_GZIP
        +TILEDB_DELTA_ENCODE,     // a2
  };

  int64_t tile_extents[] = 
  { 
      1,                          // d1
      1,                          // d2
      1                           // d3
  };               
  const int types[] = 
  { 
      TILEDB_INT32,               // a1 
      TILEDB_CHAR,                // a2
      TILEDB_INT64                // coordinates
  };

  // Set array schema
  TileDB_ArraySchema array_schema;
  tiledb_array_set_schema( 
      &array_schema,              // Array schema struct 
      array_name,                 // Array name 
      attributes,                 // Attributes 
      2,                          // Number of attributes 
      5,                          // Capacity 
      TILEDB_ROW_MAJOR,           // Cell order 
      cell_val_num,               // Number of cell values per attribute  
      compression,                // Compression
      NULL,                       // Compression level - Use defaults
      offsets_compression,        // Offsets compression
      NULL,                       // Offsets compression level
      0,                          // Sparse array
      dimensions,                 // Dimensions
      3,                          // Number of dimensions
      domain,                     // Domain
      6*sizeof(int64_t),          // Domain length in bytes
      tile_extents,               // Tile extents
      3*sizeof(int64_t),          // Tile extents length in bytes 
      TILEDB_ROW_MAJOR,           // Tile order 
      types                       // Types
  );

  // Create array
  CHECK_RC(tiledb_array_create(tiledb_ctx, &array_schema));

  // Free array schema
  CHECK_RC(tiledb_array_free_schema(&array_schema));

  /* Finalize context. */
  CHECK_RC(tiledb_ctx_finalize(tiledb_ctx));


  // ================================== ARRAY WRITE ======================

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
      array_name,                                // Array name
      TILEDB_ARRAY_WRITE,                        // Mode
      NULL,                                      // Entire domain
      NULL,                                      // All attributes
      0));                                       // Number of attributes

  // Prepare cell buffers
  int buffer_a1[] = { 0, 1, 2, 3 };
  size_t buffer_a2[] = { 0, 5, 11, 16 };
  const char buffer_var_a2[] = "firstsecondthirdfourth";
  int64_t buffer_coords[] = { 0, 0, 0,  0, 0, 1,  0, 2, 3,  2, 1, 1 };
  const void* buffers[] =
      { buffer_a1, buffer_a2, buffer_var_a2, buffer_coords };
  size_t buffer_sizes[] =
  {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2)-1,  // No need to store the last '\0' character
      sizeof(buffer_coords)
  };

  // Write to array
  CHECK_RC(tiledb_array_write(tiledb_array, buffers, buffer_sizes));

  // Finalize the array
  CHECK_RC(tiledb_array_finalize(tiledb_array));

  // Free array schema
  CHECK_RC(tiledb_array_free_schema(&array_schema));

  /* Finalize context. */
  CHECK_RC(tiledb_ctx_finalize(tiledb_ctx));

  // ================================== ARRAY READ ======================

  if (argc > 1) {
    TileDB_Config tiledb_config;
    tiledb_config.home_ = argv[1];
    CHECK_RC(tiledb_ctx_init(&tiledb_ctx, &tiledb_config));
  } else {
    CHECK_RC(tiledb_ctx_init(&tiledb_ctx, NULL));
  }


  // Initialize array
  CHECK_RC(tiledb_array_init(
           tiledb_ctx,                           // Context
           &tiledb_array,                        // Array object
           array_name,                           // Array name
           TILEDB_ARRAY_READ,                    // Mode
           NULL,                                 // Whole domain
           NULL,                                 // All attributes
           0));                                  // Number of attributes

  // Prepare cell buffers
  int r_buffer_a1[10];
  size_t r_buffer_a2[10];
  char r_buffer_var_a2[30];
  int64_t r_buffer_coords[30];
  void* r_buffers[] =
      { r_buffer_a1, r_buffer_a2, r_buffer_var_a2, r_buffer_coords };
  size_t r_buffer_sizes[] =
  {
      sizeof(r_buffer_a1),
      sizeof(r_buffer_a2),
      sizeof(r_buffer_var_a2),
      sizeof(r_buffer_coords)
  };

  // Read from array
  CHECK_RC(tiledb_array_read(tiledb_array, r_buffers, r_buffer_sizes));

  // Print cell values
  int64_t result_num = r_buffer_sizes[0] / sizeof(int);
  printf("%ld results\n", (long)result_num);
  printf("coords\t a1\t   a2\n");
  printf("-----------------------\n");
  for(int i=0; i<result_num; ++i) {
    printf("%ld, %ld, %ld", (long)r_buffer_coords[3*i], (long)r_buffer_coords[3*i+1], (long)r_buffer_coords[3*i+2]);
    printf("\t %3d", r_buffer_a1[i]);
    size_t var_size = (i != result_num-1) ? r_buffer_a2[i+1] - r_buffer_a2[i]
                                          : r_buffer_sizes[2] - r_buffer_a2[i];
    printf("\t %4.*s\n", int(var_size), &r_buffer_var_a2[r_buffer_a2[i]]);
  }

  // Finalize the array
  CHECK_RC(tiledb_array_finalize(tiledb_array));

  // Finalize context
  CHECK_RC(tiledb_ctx_finalize(tiledb_ctx));

  return 0;
}
