/**
 * @file   tiledb_array_read_sparse_filter_2.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2019 Omics Data Automation, Inc.
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
 * It shows how to read from a sparse array with a filter, constraining the read
 * to a specific subarray and subset of attributes. Moreover, the
 * program shows how to handle deletions and detect buffer overflow. 
 */

#include "tiledb.h"
#include <cstdio>

int main(int argc, char *argv[]) {
  // Initialize context with home dir if specified in command line, else
  // initialize with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  if (argc > 1) {
    TileDB_Config tiledb_config;
    tiledb_config.home_ = argv[1];
    tiledb_ctx_init(&tiledb_ctx, &tiledb_config);
  } else {
    tiledb_ctx_init(&tiledb_ctx, NULL);
  }

  // Subarray and attributes
  int64_t subarray[] = { 3, 4, 2, 4 }; 
  const char* attributes[] = { "a1" };

  // Initialize array 
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,                                       // Context
      &tiledb_array,                                    // Array object
      "my_workspace/sparse_arrays/my_array_B",          // Array name
      TILEDB_ARRAY_READ,                                // Mode
      subarray,                                         // Constrain in subarray
      attributes,                                       // Subset on attributes
      1);                                               // Number of attributes

  tiledb_array_apply_filter(tiledb_array, "a1 > 5 && a1 != 104");

  // Prepare cell buffers 
  int buffer_a1[2];
  void* buffers[] = { buffer_a1 };

  // Loop until no overflow
  printf(" a1\n----\n");
  do {
    printf("Reading cells...\n");

    size_t buffer_sizes[] = { sizeof(buffer_a1) };

    // Read from array
    tiledb_array_read(tiledb_array, buffers, buffer_sizes); 

    // Print cell values
    int64_t result_num = buffer_sizes[0] / sizeof(int);
    int64_t positions[1];
    for(int i=0; i<result_num; ++i) {
      positions[0] = i;
      if (tiledb_array_evaluate_cell(tiledb_array, buffers, buffer_sizes, positions)) {
        if(buffer_a1[i] != TILEDB_EMPTY_INT32) // Check for deletion
          printf("%3d\n", buffer_a1[i]);
      }
    }
  } while(tiledb_array_overflow(tiledb_array, 0) == 1);
 
  // Finalize the array
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
