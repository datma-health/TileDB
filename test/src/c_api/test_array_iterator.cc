/**
 * @file   test_array_iterator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
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
 * Unit Tests for the ArrayIterator class
 */

#include "catch.h"
#include "array_iterator.h"
#include "tiledb.h"

class ArrayIteratorFixture : TempDir {
 public:
  const std::string WORKSPACE = get_temp_dir() + "/array_iterator_test_ws/";
  std::string array_name_;
  TileDB_ArraySchema array_schema_;
  TileDB_CTX* tiledb_ctx_;
  TileDB_ArrayIterator* tiledb_array_iterator;

  ArrayIteratorFixture() {
    CHECK_RC(tiledb_ctx_init(&tiledb_ctx_, NULL), TILEDB_OK);
    CHECK_RC(tiledb_workspace_create(tiledb_ctx_, WORKSPACE.c_str()), TILEDB_OK);
  }

  ~ArrayIteratorFixture() {
    CHECK_RC(tiledb_ctx_finalize(tiledb_ctx_), TILEDB_OK);
  }

  void create_sparse_array(const char *array_name) {
    array_name_ = WORKSPACE + array_name;
    
    // Prepare and set the array schema object and data structures
    const int attribute_num = 1;
    const char* attributes[] = { "ATTR_INT32" };
    const char* dimensions[] = { "X", "Y" };
    int64_t domain[] = { 0, 24, 0, 24 };
    int64_t tile_extents[] = { 4, 4 };
    const int types[] = { TILEDB_INT32, TILEDB_INT64 };
    int compression[] = { TILEDB_GZIP, TILEDB_NO_COMPRESSION };
    const int dense = 0;

    // Set the array schema
    CHECK_RC(tiledb_array_set_schema(
        &array_schema_,
        array_name_.c_str(),
        attributes,
        attribute_num,
        0, // capacity
        TILEDB_COL_MAJOR,
        NULL,
        compression,
        NULL, // compression_level
        NULL, // offsets_compression
        NULL, // offsets_compression_level
        dense,
        dimensions,
        2,
        domain,
        4*sizeof(int64_t),
        tile_extents,
        2*sizeof(int64_t),
        TILEDB_COL_MAJOR,
        types), 0);
    CHECK_RC(tiledb_array_create(tiledb_ctx_, &array_schema_), TILEDB_OK);
    CHECK_RC(tiledb_array_free_schema(&array_schema_), TILEDB_OK);

    // Initialize the array 
    TileDB_Array* tiledb_array;
    CHECK_RC(tiledb_array_init(tiledb_ctx_, &tiledb_array, array_name_.c_str(),
                               TILEDB_ARRAY_WRITE_UNSORTED, NULL, NULL, 0),
             TILEDB_OK);

    // Write to array. Note that the coords are diagonal elements in the array and
    // each cell has its attribute value equal to the coordinate.
    int buffer_a1[8] = { 0, 1, 2, 3, 4, 5, 6, 7 };
    int64_t buffer_coords[16] = { 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7 };
    const void* buffers[] = { buffer_a1, buffer_coords };
    size_t buffer_sizes[2] = { 8, 16 };
    buffer_sizes[0] = sizeof(buffer_a1);
    buffer_sizes[1] = sizeof(buffer_coords);
    CHECK_RC(tiledb_array_write(tiledb_array, buffers, buffer_sizes), TILEDB_OK);
    CHECK_RC(tiledb_array_finalize(tiledb_array), TILEDB_OK);
  }

  void update_array_with_empty_attributes() {
    // Initialize the array
    TileDB_Array* tiledb_array;
    CHECK_RC(tiledb_array_init(tiledb_ctx_, &tiledb_array, array_name_.c_str(),
                               TILEDB_ARRAY_WRITE_UNSORTED, NULL, NULL, 0),
             TILEDB_OK);

    // Write to array. Note that the coords are diagonal elements in the array and
    // each cell has its attribute value equal to the coordinate.
    int buffer_a1[8] = { 0, 1, 2, 3, 4, 5, TILEDB_EMPTY_INT32, 7 };
    int64_t buffer_coords[16] = { 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7 };
    const void* buffers[] = { buffer_a1, buffer_coords };
    size_t buffer_sizes[2] = { 8, 16 };
    buffer_sizes[0] = sizeof(buffer_a1);
    buffer_sizes[1] = sizeof(buffer_coords);
    CHECK_RC(tiledb_array_write(tiledb_array, buffers, buffer_sizes), TILEDB_OK);
    CHECK_RC(tiledb_array_finalize(tiledb_array), TILEDB_OK);
  }

  int buffer_a1[8];
  int64_t buffer_coords[16];
  void* buffers[2];
  size_t buffer_sizes[2] = { 8, 16 };

  void setup_array_iterator() {
    setup_array_iterator(NULL);
  }
  
  void setup_array_iterator(const char *filter_expr) {
    buffers[0] = buffer_a1;
    buffers[1] = buffer_coords;
    if (filter_expr) {
      CHECK_RC(tiledb_array_iterator_init_with_filter(tiledb_ctx_, &tiledb_array_iterator, array_name_.c_str(),
                                                      TILEDB_ARRAY_READ, NULL, NULL, 0, buffers, buffer_sizes,
                                                      filter_expr),
               TILEDB_OK);
    } else {
      CHECK_RC(tiledb_array_iterator_init(tiledb_ctx_, &tiledb_array_iterator, array_name_.c_str(),
                                        TILEDB_ARRAY_READ, NULL, NULL, 0, buffers, buffer_sizes),
               TILEDB_OK);
    }
  }

  void check_array_iterator(int nval) {
    check_array_iterator(nval, false);
  }

  void check_array_iterator(int nval, bool contains_empty_attributes) {
    int* a1_value;
    size_t a1_size;
    int64_t* coords;
    size_t coords_size;

    int i = 0;
    while (!tiledb_array_iterator_end(tiledb_array_iterator)) {
      CHECK_RC(tiledb_array_iterator_get_value(tiledb_array_iterator, 0, (const void **)&a1_value, &a1_size), TILEDB_OK);
      CHECK(a1_size == 4);
      CHECK(*a1_value >= 0);
      
      CHECK_RC(tiledb_array_iterator_get_value(tiledb_array_iterator, 1, (const void **)&coords, &coords_size), TILEDB_OK);
      CHECK(coords_size == 16);
      
      bool cond = *coords >= 0 && *(coords+1) >= 0;
      CHECK(cond);
      CHECK(*coords == *(coords+1));
      
      if (!contains_empty_attributes) {
        CHECK(*a1_value == *coords);
      }
    
      CHECK_RC(tiledb_array_iterator_next(tiledb_array_iterator), TILEDB_OK);

      i++;
    }
    CHECK(i == nval);
  }

  void finalize_array_iterator() {
    CHECK_RC(tiledb_array_iterator_finalize(tiledb_array_iterator), TILEDB_OK);
  } 
  
};

TEST_CASE_METHOD(ArrayIteratorFixture, "Test sparse array iterator", "[sparse_array_iterator_full]") {
    create_sparse_array("test_sparse_array_it_full");
  setup_array_iterator();
  check_array_iterator(8);
  finalize_array_iterator();
}

TEST_CASE_METHOD(ArrayIteratorFixture, "Test sparse array iterator with reset", "[sparse_array_iterator_with_reset]") {
  create_sparse_array("test_sparse_array_it_reset");
  setup_array_iterator();
  check_array_iterator(8);
  int64_t subarray[] = { 0, 7, 0, 7 };
  CHECK_RC(tiledb_array_iterator_reset_subarray(tiledb_array_iterator, subarray), TILEDB_OK);
  check_array_iterator(8);
  finalize_array_iterator();
}

TEST_CASE_METHOD(ArrayIteratorFixture, "Test sparse array iterator with reset subarray", "[sparse_array_iterator_with_reset_subarray]") {
  create_sparse_array("test_sparse_array_it_reset_subarray");
  setup_array_iterator();
  check_array_iterator(8);
  int64_t subarray[] = { 0, 4, 0, 4 };
  CHECK_RC(tiledb_array_iterator_reset_subarray(tiledb_array_iterator, subarray), TILEDB_OK);
  check_array_iterator(5);
  finalize_array_iterator();
}

TEST_CASE_METHOD(ArrayIteratorFixture, "Test sparse array iterator with filter", "[sparse_array_iterator_with_filter]") {
  create_sparse_array("test_sparse_array_it_filter");
  setup_array_iterator("ATTR_INT32 > 3");
  check_array_iterator(4);
  finalize_array_iterator();
}

TEST_CASE_METHOD(ArrayIteratorFixture, "Test sparse array iterator with filter with subarray", "[sparse_array_iterator_with_filter_reset_subarray]") {
  create_sparse_array("test_sparse_array_it_filter_reset");
  setup_array_iterator("ATTR_INT32 > 3");
  check_array_iterator(4);
  int64_t subarray[] = { 0, 4, 0, 4 };
  CHECK_RC(tiledb_array_iterator_reset_subarray(tiledb_array_iterator, subarray), TILEDB_OK);
  check_array_iterator(1);
  finalize_array_iterator();
}

TEST_CASE_METHOD(ArrayIteratorFixture, "Test sparse array iterator with filteri with empty value", "[sparse_array_iterator_with_filter_with_empty_value]") {
  create_sparse_array("test_sparse_array_it_filter_with_empty_value");
  update_array_with_empty_attributes();  
  setup_array_iterator("ATTR_INT32 > 3");
  check_array_iterator(4, true);
  finalize_array_iterator();
}

