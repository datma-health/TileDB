/**
 * @file   test_filter_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019, 2022 Omics Data Automation, Inc.
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
 * Tests for the Expression class
 */


#include "catch.h"
#include "tiledb.h"

TEST_CASE("Test genomicsdb_ws_filter", "[genomicsdb_ws_filter]") {
  std::string ws = std::string(TILEDB_TEST_DIR)+"/inputs/genomicsdb_ws";
  std::string array = std::string(TILEDB_TEST_DIR)+"/inputs/genomicsdb_ws/1$1$249250621";

  // Initialize tiledb context
  TileDB_CTX* tiledb_ctx;
  TileDB_Config tiledb_config;
  tiledb_config.home_ =  ws.c_str();
  CHECK_RC(tiledb_ctx_init(&tiledb_ctx, &tiledb_config), TILEDB_OK);

  const char* attributes[] = {"GT", TILEDB_COORDS};

  size_t buffer_size = 1024;
  size_t buffer_sizes[] = { buffer_size, buffer_size, buffer_size, buffer_size, buffer_size, buffer_size, buffer_size, buffer_size }; 
  void *buffers[8];
  for (auto i=0; i<sizeof(buffer_sizes)/sizeof(size_t); i++) {
    buffers[i] = malloc(buffer_sizes[i]);
  }

  // Initialize array 
  TileDB_ArrayIterator* tiledb_array_it;
  CHECK_RC(tiledb_array_iterator_init_with_filter(
      tiledb_ctx,                                       // Context
      &tiledb_array_it,                                 // Array object
      array.c_str(),                                    // Array name
      TILEDB_ARRAY_READ,                                // Mode
      NULL,                                             // Whole domain
      attributes,                                       // Subset on attributes
      sizeof(attributes)/sizeof(char *),                // Number of attributes
      buffers,                                          // Preallocated buffers for internal use
      buffer_sizes,                                     // buffer_sizes
      "GT[0]==1"),                                      // filter expression wth just one match for genomicsdb_ws
           TILEDB_OK);

  
  int* GT_val = NULL;
  size_t GT_size = 0;

  int64_t* coords = NULL;
  size_t coords_size = 0;
  while(!tiledb_array_iterator_end(tiledb_array_it)) {
    // Get value
    CHECK_RC(tiledb_array_iterator_get_value(
        tiledb_array_it,         // Array iterator
        0,                       // Attribute id
        (const void**) &GT_val,  // Value
        &GT_size),               // Value size (useful in variable-sized attributes)
             TILEDB_OK);

    CHECK(GT_size == 3*sizeof(int));

    // Check that it is not a deletion
    int expected_values[3] = {1, 0, 1};
    for (auto i=0; i<GT_size/sizeof(int); i++) {
      CHECK(GT_val[i] == expected_values[i]);
    }
    
    CHECK_RC(tiledb_array_iterator_get_value(
        tiledb_array_it,         // Array iterator
        1,                       // Attribute id for coords
        (const void**) &coords,  // Value
        &coords_size),           // Value size (useful in variable-sized attributes)
             TILEDB_OK);

    if (coords_size > 0) {
      CHECK(coords_size == 16);
      CHECK(coords[0] == 1);
      CHECK(coords[1] == 17384);
    }


    // Advance iterator
    CHECK_RC(tiledb_array_iterator_next(tiledb_array_it), TILEDB_OK);

    // Only one match, so iterator_end() should be true
    CHECK(tiledb_array_iterator_end(tiledb_array_it));
  }
  
  // Finalize the array
  CHECK_RC(tiledb_array_iterator_finalize(tiledb_array_it), TILEDB_OK);

  // Finalize context
  CHECK_RC(tiledb_ctx_finalize(tiledb_ctx), TILEDB_OK);
  
}


