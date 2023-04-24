/**
 * @file   test_filter_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 Omics Data Automation, Inc.
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
 * Tests for testing filter expressions from a GenomicsDB workspace
 */


#include "catch.h"
#include "tiledb.h"

std::string ws = std::string(TILEDB_TEST_DIR)+"/inputs/genomicsdb_ws";
std::string array = std::string(TILEDB_TEST_DIR)+"/inputs/genomicsdb_ws/1$1$249250621";

int num_attributes = 4;
const char* attributes[] = {"REF", "ALT", "GT", TILEDB_COORDS};

// Sizes to be used for buffers
size_t sizes[2] = { 1024, 40 };

// filter expression that results in just one match for genomicsdb_ws
std::string filters[2] = { "REF == \"G\" && GT[0]==1 && splitcompare(ALT, 124, \"T\")",
                           "REF == \"G\" && GT[0] == 1 && ALT |= \"T\"" };

// Only match expected for the following test cases
char expected_REF = 'G';
char expected_ALT[3]  = { 'T', '|', '&' };
int expected_GT_values[3] = { 1, 0, 1 };
int64_t expected_coords[2] = { 1, 17384 };

TEST_CASE("Test genomicsdb_ws filter with iterator api", "[genomicsdb_ws_filter_iterator]") {
  for (auto i=0; i<2; i++) {
    SECTION("Test filter expressions for iteration " + std::to_string(i)) {
      // Initialize tiledb context
      TileDB_CTX* tiledb_ctx;
      TileDB_Config tiledb_config;
      tiledb_config.home_ =  ws.c_str();
      CHECK_RC(tiledb_ctx_init(&tiledb_ctx, &tiledb_config), TILEDB_OK);

      size_t buffer_size = sizes[i];
      size_t buffer_sizes[] = { buffer_size, buffer_size, buffer_size, buffer_size, buffer_size, buffer_size, buffer_size, buffer_size };
      void *buffers[8];
      CHECK(sizeof(buffer_sizes)/sizeof(size_t) == 8);
      for (auto i=0; i<8; i++) {
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
          filters[i].c_str()),
               TILEDB_OK);

      int* GT_val = NULL;
      size_t GT_size = 0;

      int64_t* coords = NULL;
      size_t coords_size = 0;

      CHECK_RC(tiledb_array_iterator_end(tiledb_array_it), TILEDB_OK);

      // Get values
      char* REF_val;
      size_t REF_size;
      CHECK_RC(tiledb_array_iterator_get_value(
          tiledb_array_it,          // Array iterator
          0,                        // Attribute id for REF
          (const void**) &REF_val,  // Value
          &REF_size),               // Value size (useful in variable-sized attributes)
               TILEDB_OK);
      CHECK(REF_size == sizeof(char));
      CHECK(REF_val[0] == expected_REF);

      char* ALT_val;
      size_t ALT_size;
      CHECK_RC(tiledb_array_iterator_get_value(
          tiledb_array_it,          // Array iterator
          1,                        // Attribute id fogr ALT
          (const void**) &ALT_val,  // Value
          &ALT_size),               // Value size (useful in variable-sized attributes)
               TILEDB_OK);
      CHECK(ALT_size == 3*sizeof(char));
      for (auto i=0; i<ALT_size/sizeof(char); i++) {
        CHECK(ALT_val[i] == expected_ALT[i]);
      }

      CHECK_RC(tiledb_array_iterator_get_value(
          tiledb_array_it,         // Array iterator
          2,                       // Attribute id for GT
          (const void**) &GT_val,  // Value
          &GT_size),               // Value size (useful in variable-sized attributes)
               TILEDB_OK);
      CHECK(GT_size == 3*sizeof(int));
      for (auto i=0; i<GT_size/sizeof(int); i++) {
        CHECK(GT_val[i] == expected_GT_values[i]);
      }
 
      CHECK_RC(tiledb_array_iterator_get_value(
          tiledb_array_it,         // Array iterator
          3,                       // Attribute id for coords
          (const void**) &coords,  // Value
          &coords_size),           // Value size (useful in variable-sized attributes)
               TILEDB_OK);
      if (coords_size > 0) {
        CHECK(coords_size == 16);
        CHECK(coords[0] == expected_coords[0]);
        CHECK(coords[1] == expected_coords[1]);
      }

      // Advance iterator
      CHECK_RC(tiledb_array_iterator_next(tiledb_array_it), TILEDB_OK);

      // Only one match, so iterator_end() should be true
      CHECK(tiledb_array_iterator_end(tiledb_array_it));

      // Finalize the array
      CHECK_RC(tiledb_array_iterator_finalize(tiledb_array_it), TILEDB_OK);

      // Finalize context
      CHECK_RC(tiledb_ctx_finalize(tiledb_ctx), TILEDB_OK);

      // Deallocate memory
      for (auto i=0; i<8; i++) {
        free(buffers[i]);
      }
    }
  }
}

TEST_CASE("Test genomicsdb_ws filter with read api", "[genomicsdb_ws_filter_read]") {
  for (auto i=0; i<1; i++) {
    SECTION("Test filter expressions for " + std::to_string(i)) {
      // Initialize tiledb context
      TileDB_CTX* tiledb_ctx;
      TileDB_Config tiledb_config;
      tiledb_config.home_ =  ws.c_str();
      CHECK_RC(tiledb_ctx_init(&tiledb_ctx, &tiledb_config), TILEDB_OK);

      size_t buffer_size = sizes[i];
      size_t buffer_sizes[] = { buffer_size, buffer_size, buffer_size, buffer_size, buffer_size, buffer_size, buffer_size, buffer_size };
      void *buffers[8];
      CHECK(sizeof(buffer_sizes)/sizeof(size_t) == 8);
      for (auto i=0; i<8; i++) {
        buffers[i] = malloc(buffer_sizes[i]);
      }

      // Initialize array
      TileDB_Array* tiledb_array;
      CHECK_RC(tiledb_array_init(
          tiledb_ctx,                                       // Context
          &tiledb_array,                                    // Array object
          array.c_str(),                                    // Array name
          TILEDB_ARRAY_READ,                                // Mode
          NULL,                                             // Whole domain
          attributes,                                       // Subset on attributes
          sizeof(attributes)/sizeof(char *)),               // Number of attributes
               TILEDB_OK);

      // Apply expression filter
      CHECK_RC(tiledb_array_apply_filter(tiledb_array, filters[i].c_str()), TILEDB_OK);

      // Read from array
      CHECK_RC(tiledb_array_read(tiledb_array, buffers, buffer_sizes), TILEDB_OK);

      // Check return buffers, there should only be one match based on the filter
      for (auto i=0; i<8; i++) {
        switch (i) {
          case 0: // REF - string
            CHECK(buffer_sizes[i] == sizeof(size_t));
            CHECK(*(size_t *)buffers[i++] == 0);
            CHECK(buffer_sizes[i] == sizeof(char));
            CHECK(*(char *)buffers[i] == expected_REF);
            break;
          // ALT - string
          case 2: {
            CHECK(buffer_sizes[i] == sizeof(size_t));
            CHECK(*(size_t *)buffers[i++] == 0);
            CHECK(buffer_sizes[i] == sizeof(char)*3);
            char *ALT = (char *)buffers[i];
            for (auto j=0; j<3; j++) {
              CHECK(ALT[j] == expected_ALT[j]);
            }
            break;
          }
          // GT - variable number of integers
          case 4: {
            CHECK(buffer_sizes[i++] == sizeof(size_t));
            CHECK(buffer_sizes[i] == 3*sizeof(int));
            int *GT_val = (int *)buffers[i];
            for (auto j=0; j<3; j++) {
              CHECK(GT_val[j] == expected_GT_values[j]);
            }
            break;
          }
          // COORDS - 2D
          case 6: {
            CHECK(buffer_sizes[i] == sizeof(int64_t)*2); // For 2D
            int64_t *coords = (int64_t *)buffers[i];
            for (auto j=0; j<2; j++) {
              CHECK(coords[j] == expected_coords[j]);
            }
            break;
          }
          default:
            break;
        }
      }

      // Finalize the array
      CHECK_RC(tiledb_array_finalize(tiledb_array), TILEDB_OK);

      // Finalize context
      CHECK_RC(tiledb_ctx_finalize(tiledb_ctx), TILEDB_OK);

      // Deallocate memory
      for (auto i=0; i<8; i++) {
        free(buffers[i]);
      }
    }
  }
}
