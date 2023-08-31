/**
 * @file   test_filter_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 Omics Data Automation, Inc.
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
 * Tests for testing filter expressions from a GenomicsDB workspace
 */

#include "catch.h"
#include "tiledb.h"

std::string ws = std::string(TILEDB_TEST_DIR)+"/inputs/genomicsdb_ws";
std::string array = std::string(TILEDB_TEST_DIR)+"/inputs/genomicsdb_ws/1$1$249250621";

int num_attributes = 4;
const char* attributes[] = { "REF", "ALT", "GT", TILEDB_COORDS };

// Sizes to be used for buffers
size_t sizes[5] = { 1024, 40, 512, 4096, 4096 };

// filter expression that results in just one match for genomicsdb_ws
std::string filters[5] = { "POS >= 0 && ROW >=0 && REF == \"G\" && GT[0]==1 && splitcompare(ALT, 124, \"T\") && resolve(GT, REF, ALT) &= \"T/T\"",
  "ishomalt(GT) && REF == \"G\" && GT[0]==1 && splitcompare(ALT, 124, \"T\") && resolve(GT, REF, ALT) &= \"T/T\"",
  "ishet(GT) != true && REF == \"G\" && splitcompare(ALT, 124, \"T\") && resolve(GT, REF, ALT) &= \"T/T\"",
  "REF == \"G\" && ALT |= \"T\" && resolve(GT, REF,  ALT) &= \"T\"",
  "POS==17384 && REF == \"G\" && ALT |= \"T\" && resolve(GT, REF, ALT) &= \"T/T\""};

// Only match expected for the following test cases
char expected_REF = 'G';
char expected_ALT[3]  = { 'T', '|', '&' };
int expected_GT_values[3] = { 1, 0, 1 };
int64_t expected_coords[2] = { 1, 17384 };

TEST_CASE("Test genomicsdb_ws filter with iterator api", "[genomicsdb_ws_filter_iterator]") {
  for (auto i=0; i<5; i++) {
    SECTION("Test filter expressions for iteration " + std::to_string(i)) {
      // Initialize tiledb context
      TileDB_CTX* tiledb_ctx;
      TileDB_Config tiledb_config;
      tiledb_config.home_ =  ws.c_str();
      CHECK_RC(tiledb_ctx_init(&tiledb_ctx, &tiledb_config), TILEDB_OK);

      size_t buffer_size = sizes[i];
      size_t buffer_sizes[] = { buffer_size, buffer_size, buffer_size, buffer_size, buffer_size, buffer_size, buffer_size };
      void *buffers[7];
      auto nBuffers = sizeof(buffer_sizes)/sizeof(size_t);
      CHECK(nBuffers == 7);
      for (auto i=0u; i<nBuffers; i++) {
        buffers[i] = malloc(buffer_sizes[i]);
      }

      // Initialize array
      TileDB_ArrayIterator* tiledb_array_it;
      int rc = tiledb_array_iterator_init_with_filter(
          tiledb_ctx,                                       // Context
          &tiledb_array_it,                                 // Array object
          array.c_str(),                                    // Array name
          TILEDB_ARRAY_READ,                                // Mode
          NULL,                                             // Whole domain
          attributes,                                       // Subset on attributes
          sizeof(attributes)/sizeof(char *),                // Number of attributes
          buffers,                                          // Preallocated buffers for internal use
          buffer_sizes,                                     // buffer_sizes
          filters[i].c_str());

      
      CHECK_RC(rc,TILEDB_OK);

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
          1,                        // Attribute id for ALT
          (const void**) &ALT_val,  // Value
          &ALT_size),               // Value size (useful in variable-sized attributes)
               TILEDB_OK);
      CHECK(ALT_size == 3*sizeof(char));
      for (auto i=0u; i<ALT_size/sizeof(char); i++) {
        CHECK(ALT_val[i] == expected_ALT[i]);
      }

      CHECK_RC(tiledb_array_iterator_get_value(
          tiledb_array_it,         // Array iterator
          2,                       // Attribute id for GT
          (const void**) &GT_val,  // Value
          &GT_size),               // Value size (useful in variable-sized attributes)
               TILEDB_OK);
      CHECK(GT_size == 3*sizeof(int));
      for (auto i=0u; i<GT_size/sizeof(int); i++) {
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
      for (auto i=0u; i<nBuffers; i++) {
        free(buffers[i]);
      }
    }
  }
}

TEST_CASE("Test genomicsdb_ws filter with read api", "[genomicsdb_ws_filter_read]") {
  for (auto i=0; i<5; i++) {
    SECTION("Test filter expressions for " + std::to_string(i)) {
      // Initialize tiledb context
      TileDB_CTX* tiledb_ctx;
      TileDB_Config tiledb_config;
      tiledb_config.home_ =  ws.c_str();
      CHECK_RC(tiledb_ctx_init(&tiledb_ctx, &tiledb_config), TILEDB_OK);

      size_t buffer_size = sizes[i];
      size_t buffer_sizes[] = { buffer_size, buffer_size, buffer_size, buffer_size, buffer_size, buffer_size, buffer_size };
      void *buffers[7];
      auto nBuffers = sizeof(buffer_sizes)/sizeof(size_t);
      CHECK(nBuffers == 7);

      for (auto i=0u; i<nBuffers; i++) {
        buffers[i] = malloc(1024);
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
      bool continue_read;
      size_t actual_buffer_sizes[7] = { 0, 0, 0, 0, 0, 0, 0 };
      size_t adjust_offsets[3] = { 0, 0, 0 };
      do {
        continue_read = false;
        CHECK_RC(tiledb_array_read(tiledb_array, buffers, buffer_sizes), TILEDB_OK);

        for (auto i=0, j=0; i<num_attributes; i++, j++) {
          // Adjust offset sizes
          if (i != num_attributes-1 && buffer_sizes[j] != 0) {
            for (auto k=0u; adjust_offsets[i]>0 && k<buffer_sizes[j]; k++) {
              *((size_t *)buffers[j] + k) += adjust_offsets[i];
            }
            adjust_offsets[i] += buffer_sizes[j+1];
          }
          // Update the actual buffer sizes for resetting buffer pointers later
          actual_buffer_sizes[j] += buffer_sizes[j];
          if (i<num_attributes-1) {
            actual_buffer_sizes[j+1] += buffer_sizes[j+1];
          }
          // Potential for overflow. We should read again to be sure there is
          // nothing left to be read.
          if (buffer_sizes[j] != 0) {
            buffers[j] = (char *)buffers[j] + buffer_sizes[j];
            buffer_sizes[j] = buffer_size;
            if (i != num_attributes-1) { // Not Coords
              buffers[j+1] = (char *)buffers[j+1] + buffer_sizes[j+1];
              buffer_sizes[j+1] = buffer_size;
            }
            continue_read = true;
          }
          j++;
        }
      } while(continue_read);

      for (auto j=0u; j<nBuffers; j++) {
        buffer_sizes[j] = actual_buffer_sizes[j];
        buffers[j] = (char *)buffers[j] -  actual_buffer_sizes[j];
      }

      // Evaluate cells basied on the filter expression
      auto ncells = buffer_sizes[nBuffers-1]/16; // Coords
      CHECK(ncells == 7); // Number of cells found should be 7
      for (auto i=0u; i<ncells; i++) {
        // Check return buffers, there should only be one match based on the filter
        int64_t positions[] = {i, i, i, i, i, i, i };
        INFO(std::string("Evaluating cell for cell position=") + std::to_string(i) + " tiledb_errmsg=" + std::string(tiledb_errmsg));
        int rc = tiledb_array_evaluate_cell(tiledb_array, buffers, buffer_sizes, positions);
        CHECK(rc != TILEDB_ERR);
        if (i != 5) {
          REQUIRE(rc == false);
        } else {
          REQUIRE(rc == true);
          for (auto j=0; j<7; j++) {
            switch (j) {
              case 1: // REF - string
                CHECK(*((char *)(buffers[j])+i) == expected_REF);
                break;
              case 3: { // ALT - string
                char *ALT = (char *)(buffers[j]) + *((size_t *)(buffers[j-1])+i);
                for (auto k=0; k<3; k++) {
                  CHECK(ALT[k] == expected_ALT[k]);
                }
                break;
              }
              case 5: { // GT - variable number of integers
                int *GT_val = (int *)buffers[j] + *((size_t *)(buffers[j-1])+i)/sizeof(int);
                for (auto k=0; k<3; k++) {
                  CHECK(GT_val[k] == expected_GT_values[k]);
                }
                break;
              }
              case 6: { // COORDS - 2D
                int64_t *coords = (int64_t *)buffers[j]+2*i;
                for (auto k=0; k<2; k++) {
                  CHECK(coords[k] == expected_coords[k]);
                }
                break;
              }
              default:
                break;
            }
          }
        }
      }

      // Finalize the array
      CHECK_RC(tiledb_array_finalize(tiledb_array), TILEDB_OK);

      // Finalize context
      CHECK_RC(tiledb_ctx_finalize(tiledb_ctx), TILEDB_OK);

      // Deallocate memory
      for (auto i=0u; i<nBuffers; i++) {
        free(buffers[i]);
      }
    }
  }
}

TEST_CASE("Test genomicsdb demo test case", "[genomicsdb_demo]") {
  char *workspace = getenv("GENOMICSDB_DEMO_WS");
  if (!workspace) return;

  std::string array = std::string(workspace) + "/allcontigs$1$3095677412";

  std::vector<std::string> filters = {"", // zlib 38s
    "ROW==10000", // 4s
    "POS==2084172232", // 4s
    "ROW==10000 && REF==\"T\"", // 5s
    "resolve(GT,REF,ALT)&=\"C|C\"",
    "resolve(GT,REF,ALT)&=\"C/C\"",
    "resolve(GT,REF,ALT)&=\"C\"",
    "ROW==10000 && REF==\"T\" && ALT|=\"C\"", // 6s
    "ROW==10000 && REF==\"T\" && ALT|=\"C\" && GT[0]==1", // 7s
    "ROW==10000 && REF==\"T\" && ALT|=\"C\" && resolve(GT,REF,ALT)&=\"C|C\"", // eval true 7s
    "ROW==10000 && REF==\"T\" && ALT|=\"C\" && resolve(GT,REF,ALT)&=\"C/C\"", // eval true 7s
    "ROW==10000 && REF==\"C\" && ALT|=\"T\" && resolve(GT,REF,ALT)&=\"C\"" // eval false 7s
    };

  const int64_t subarray[] = {0ul, 200000ul, 2000000000ul, 2100000000ul};

  for (auto filter_i=0u; filter_i<filters.size(); filter_i++) {
    // Initialize tiledb context
    TileDB_CTX* tiledb_ctx;
    TileDB_Config tiledb_config;
    tiledb_config.home_ = workspace;

    // Initialize Context
    CHECK_RC(tiledb_ctx_init(&tiledb_ctx, &tiledb_config), TILEDB_OK);

    Catch::Timer t;
    t.start();

    printf("Starting evaluation for %s\n", filters[filter_i].c_str());

    size_t buffer_size = 4096;
    size_t buffer_sizes[] = { buffer_size, buffer_size, buffer_size, buffer_size, buffer_size, buffer_size, buffer_size };
    void *buffers[7];
    auto nBuffers = sizeof(buffer_sizes)/sizeof(size_t);
    CHECK(nBuffers == 7);
    for (auto i=0u; i<nBuffers; i++) {
      buffers[i] = malloc(buffer_sizes[i]);
    }

    // Initialize array
    TileDB_ArrayIterator* tiledb_array_it;
    CHECK_RC(tiledb_array_iterator_init_with_filter(
        tiledb_ctx,                                       // Context
        &tiledb_array_it,                                 // Array object
        array.c_str(),                                    // Array name
        TILEDB_ARRAY_READ,                                // Mode
        subarray,                                         // Subarray
        attributes,                                       // Subset on attributes
        sizeof(attributes)/sizeof(char *),                // Number of attributes
        buffers,                                          // Preallocated buffers for internal use
        buffer_sizes,                                     // buffer_sizes
        filters[filter_i].c_str()),
             TILEDB_OK);

    while (tiledb_array_iterator_end(tiledb_array_it) == TILEDB_OK) {
      char* REF_val;
      size_t REF_size;
      CHECK_RC(tiledb_array_iterator_get_value(
          tiledb_array_it,          // Array iterator
          0,                        // Attribute id for REF
          (const void**) &REF_val,  // Value
          &REF_size),               // Value size (useful in variable-sized attributes)
               TILEDB_OK);
      for (auto i=0u; i<REF_size/sizeof(char); i++, REF_val++) {
        if (!i) printf("REF=");
        printf("%c ", *REF_val);
      }
      char* ALT_val;
      size_t ALT_size;
      CHECK_RC(tiledb_array_iterator_get_value(
          tiledb_array_it,          // Array iterator
          1,                        // Attribute id for ALT
          (const void**) &ALT_val,  // Value
          &ALT_size),               // Value size (useful in variable-sized attributes)
               TILEDB_OK);
      for (auto i=0u; i<ALT_size/sizeof(char); i++, ALT_val++) {
        if (!i) printf("ALT=");
        printf("%c ", *ALT_val);
      }
      int* GT_val = NULL;
      size_t GT_size = 0;
      CHECK_RC(tiledb_array_iterator_get_value(
          tiledb_array_it,         // Array iterator
          2,                       // Attribute id for GT
          (const void**) &GT_val,  // Value
          &GT_size),               // Value size (useful in variable-sized attributes)
               TILEDB_OK);
      for (auto i=0u; i<GT_size/sizeof(int); i++, GT_val++) {
        if (!i) printf("GT=");
        printf("%d ", *GT_val);
      }
      int64_t* coords = NULL;
      size_t coords_size = 0;
      CHECK_RC(tiledb_array_iterator_get_value(
          tiledb_array_it,         // Array iterator
          3,                       // Attribute id for coords
          (const void**) &coords,  // Value
          &coords_size),           // Value size (useful in variable-sized attributes)
               TILEDB_OK);
      CHECK(coords_size == 16);
      printf("coords[0]=%lld coords[1]=%lld\n", (long long)(coords[0]), (long long)(coords[1]));

      // Advance iterator
      CHECK_RC(tiledb_array_iterator_next(tiledb_array_it), TILEDB_OK);
    }

    // Finalize the array
    CHECK_RC(tiledb_array_iterator_finalize(tiledb_array_it), TILEDB_OK);

    printf("Elapsed Time=%us for %s\n", t.getElapsedMilliseconds()/1000, filters[filter_i].c_str());

    // Finalize context
    CHECK_RC(tiledb_ctx_finalize(tiledb_ctx), TILEDB_OK);
  }
}
