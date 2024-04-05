/**
 * @file   test_reusing_tiledb_context.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 dātma, inc™
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
 * Performance tests for reusing tiledb contexts for large workspaces
 */

#include "catch.h"
#include "tiledb.h"

char *workspace = getenv("GENOMICSDB_DEMO_WS");
std::string array = std::string(workspace) + "/allcontigs$1$3095677412";
const char* attributes[] = { "REF", TILEDB_COORDS };

char *num_iterations_env = getenv("NUM_ITERATIONS");
int num_iterations = num_iterations_env?atoi(num_iterations_env):2;

void array_iterator(TileDB_CTX* tiledb_ctx, void** buffers, size_t* buffer_sizes) {
  // Initialize array
  TileDB_ArrayIterator* tiledb_array_it;
  int rc = tiledb_array_iterator_init(
      tiledb_ctx,                                       // Context
      &tiledb_array_it,                                 // Array object
      array.c_str(),                                    // Array name
      TILEDB_ARRAY_READ,                                // Mode
      NULL,                                             // Whole domain
      attributes,                                       // Subset on attributes
      sizeof(attributes)/sizeof(char *),                // Number of attributes
      buffers,                                          // Preallocated buffers for internal use
      buffer_sizes);                                    // buffer_sizes
    
  CHECK_RC(rc,TILEDB_OK);

  while (tiledb_array_iterator_end(tiledb_array_it) == TILEDB_OK) {
    char* REF_val;
    size_t REF_size;
    CHECK_RC(tiledb_array_iterator_get_value(
        tiledb_array_it,          // Array iterator
        0,                        // Attribute id for REF
        (const void**) &REF_val,  // Value
        &REF_size),               // Value size (useful in variable-sized attributes)
             TILEDB_OK);
    //printf("REF=%s\n", std::string(REF_val, REF_size).c_str());

    int64_t* coords = NULL;
    size_t coords_size = 0;
    CHECK_RC(tiledb_array_iterator_get_value(
        tiledb_array_it,         // Array iterator
        1,                       // Attribute id for coords
        (const void**) &coords,  // Value
        &coords_size),           // Value size (useful in variable-sized attributes)
             TILEDB_OK);
    if (coords_size > 0) {
      CHECK(coords_size == 16);
      //printf("COORDS=%lld %lld\n", coords[0], coords[1]);
    }

    // Advance iterator
    CHECK_RC(tiledb_array_iterator_next(tiledb_array_it), TILEDB_OK);
  }

  // Finalize the array
  CHECK_RC(tiledb_array_iterator_finalize(tiledb_array_it), TILEDB_OK);
}

TEST_CASE("Test reusing tiledb ctx", "[reuse_tiledb_ctx]") {
  if (!workspace) return;

  // Initialize tiledb context
  TileDB_CTX* tiledb_ctx;
  TileDB_Config tiledb_config;
  tiledb_config.home_ = workspace;

  // Initialize Context
  CHECK_RC(tiledb_ctx_init(&tiledb_ctx, &tiledb_config), TILEDB_OK);

  size_t buffer_size = 4096;
  size_t buffer_sizes[] = { buffer_size, buffer_size, buffer_size };
  void *buffers[3];
  auto nBuffers = sizeof(buffer_sizes)/sizeof(size_t);
  CHECK(nBuffers == 3);
  for (auto i=0u; i<nBuffers; i++) {
    buffers[i] = malloc(buffer_sizes[i]);
  }

  for (auto i=0; i<num_iterations; i++) {
    printf("Starting iteration=%d/%d\n", i, num_iterations-1);

    for (auto j=0u; j<nBuffers; j++) {
      buffer_sizes[j] = buffer_size;
    }

    array_iterator(tiledb_ctx, buffers, buffer_sizes); 
  }
  
  // Finalize context
  CHECK_RC(tiledb_ctx_finalize(tiledb_ctx), TILEDB_OK);
  
  // Deallocate memory
  for (auto i=0u; i<nBuffers; i++) {
    free(buffers[i]);
  }
}

TEST_CASE("Test with new tiledb ctx per iteration", "[no_reuse_tiledb_ctx]") {
  if (!workspace) return;

  size_t buffer_size = 4096;
  size_t buffer_sizes[] = { buffer_size, buffer_size, buffer_size };
  void *buffers[3];
  auto nBuffers = sizeof(buffer_sizes)/sizeof(size_t);
  CHECK(nBuffers == 3);
  for (auto i=0u; i<nBuffers; i++) {
    buffers[i] = malloc(buffer_sizes[i]);
  }

  for (auto i=0; i<num_iterations; i++) {
    printf("Starting iteration=%d/%d\n", i, num_iterations-1);
    // Initialize tiledb context
    TileDB_CTX* tiledb_ctx;
    TileDB_Config tiledb_config;
    tiledb_config.home_ = workspace;

    // Initialize Context
    CHECK_RC(tiledb_ctx_init(&tiledb_ctx, &tiledb_config), TILEDB_OK);
    
    for (auto j=0u; j<nBuffers; j++) {
      buffer_sizes[j] = buffer_size;
    }

    array_iterator(tiledb_ctx, buffers, buffer_sizes);
    
    // Finalize context
    CHECK_RC(tiledb_ctx_finalize(tiledb_ctx), TILEDB_OK);
  }
   
  // Deallocate memory
  for (auto i=0u; i<nBuffers; i++) {
    free(buffers[i]);
  }
}
