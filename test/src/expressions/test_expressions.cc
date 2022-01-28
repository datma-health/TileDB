/**
 * @file   test_expressions.cc
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
#include "expression.h"
#include "storage_posixfs.h"
#include "tiledb.h"

int rc;

template <class T>
class ArrayFixture {
 protected:
  const std::string ARRAYNAME = "test_sparse_array_1D";
  const std::vector<std::string> attribute_names = { "a1" };

  const char* attr_names[1] = { "a1" };
  int types[1] = { TILEDB_CHAR };
  int cell_val_nums[1] = { 1 };

  T buffer_a1[16] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
  void* buffers[1] { buffer_a1 };
  size_t buffer_sizes[1] = { sizeof(buffer_a1) };

  PosixFS *posixfs_;
  ArraySchema *array_schema_;
  
  ArrayFixture() {
    if (typeid(T)== typeid(char)) {
      types[0] = TILEDB_CHAR;
    } else if (typeid(T) == typeid(int)) {
      types[0] = TILEDB_INT32;
    } else if (typeid(T) == typeid(float)) {
      types[0] = TILEDB_FLOAT32;
    }
    posixfs_ = new PosixFS();
    array_schema_ = new ArraySchema(posixfs_);
    array_schema_->set_attributes(const_cast<char **>(attr_names), 1);
    array_schema_->set_cell_val_num(cell_val_nums);
    array_schema_->set_types(types);
    array_schema_->set_dense(0);
  }

  ~ArrayFixture() {
    delete array_schema_;
    delete posixfs_;
  }

  void check_buffer(void **computed, size_t *computed_size, const T *expected, const size_t expected_size) {
    size_t buffer_size = *computed_size/sizeof(T);
    REQUIRE(buffer_size == expected_size);

    T *buffer = reinterpret_cast<T *>(computed[0]);
    for (auto i=0u; i<buffer_size; i++) {
      CHECK(*buffer++ == *expected++);
    }
  }
};

TEST_CASE_METHOD(ArrayFixture<int>, "Test Expressions Empty", "[expressions_empty]") {
  Expression expression("", attribute_names, array_schema_);
  REQUIRE(expression.evaluate(buffers, buffer_sizes) == TILEDB_OK);
  const int expected_buffer[16] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
  check_buffer(buffers, buffer_sizes, expected_buffer, 16);
}

TEST_CASE_METHOD(ArrayFixture<int>, "Test Expressions All", "[expressions_all]") {
  Expression expression("a1 >= 0", attribute_names, array_schema_);
  REQUIRE(expression.evaluate(buffers, buffer_sizes) == TILEDB_OK);
  const int expected_buffer[16] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
  check_buffer(buffers, buffer_sizes, expected_buffer, 16);
}

TEST_CASE_METHOD(ArrayFixture<char>, "Test Expressions All - char", "[expressions_all_char]") {
  Expression expression("a1 >= 0", attribute_names, array_schema_);
  REQUIRE(expression.evaluate(buffers, buffer_sizes) == TILEDB_OK);
  const char expected_buffer[16] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
  check_buffer(buffers, buffer_sizes, expected_buffer, 16);
}

TEST_CASE_METHOD(ArrayFixture<float>, "Test Expressions All - float", "[expressions_all_float]") {
  Expression expression("a1 >= 0", attribute_names, array_schema_);
  REQUIRE(expression.evaluate(buffers, buffer_sizes) == TILEDB_OK);
  const float expected_buffer[16] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
  check_buffer(buffers, buffer_sizes, expected_buffer, 16);
}

TEST_CASE_METHOD(ArrayFixture<int>, "Test Expressions Non-existent Attribute", "[expressions_non_existent_attr]") {
  Expression expression("a2 > a1", attribute_names, array_schema_);
  CHECK_RC(expression.evaluate(buffers, buffer_sizes), TILEDB_ERR);
}

TEST_CASE_METHOD(ArrayFixture<int>, "Test Expressions With Dropped Cells from left", "[expressions_dropped_cells_left]") {
  Expression expression("a1 > 4", attribute_names, array_schema_);
  REQUIRE(expression.evaluate(buffers, buffer_sizes) == TILEDB_OK);
  const int expected_buffer[11] =  { 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
  check_buffer(buffers, buffer_sizes, expected_buffer, 11);
}

TEST_CASE_METHOD(ArrayFixture<int>, "Test Expressions With Dropped Cells from right", "[expressions_dropped_cells_right]") {
  Expression expression("a1 < 4", attribute_names, array_schema_);
  REQUIRE(expression.evaluate(buffers, buffer_sizes) == TILEDB_OK);
  const int expected_buffer[11] =  { 0, 1, 2, 3 };
  check_buffer(buffers, buffer_sizes, expected_buffer, 4);
}

TEST_CASE_METHOD(ArrayFixture<int>, "Test Expressions All Dropped Cells", "[expressions_all_dropped_cells]") {
  Expression expression("a1 > 16", attribute_names, array_schema_);
  REQUIRE(expression.evaluate(buffers, buffer_sizes) == TILEDB_OK);
  const int expected_buffer[0] = { };
  check_buffer(buffers, buffer_sizes, expected_buffer, 0);
}

TEST_CASE_METHOD(ArrayFixture<int>, "Test Expressions All-1 Dropped Cells", "[expressions_all_but_one_dropped_cells]") {
  Expression expression("a1 == 15", attribute_names, array_schema_);
  REQUIRE(expression.evaluate(buffers, buffer_sizes) == TILEDB_OK);
  const int expected_buffer[1] = { 15 };
  check_buffer(buffers, buffer_sizes, expected_buffer, 1);
}

TEST_CASE_METHOD(ArrayFixture<int>, "Test Expressions Start/End Dropped Cells", "[expressions_start_end_dropped_cells]") {
  Expression expression("a1==0 or a1 == 15", attribute_names, array_schema_);
  REQUIRE(expression.evaluate(buffers, buffer_sizes) == TILEDB_OK);
  const int expected_buffer[2] = { 0, 15 };
  check_buffer(buffers, buffer_sizes, expected_buffer, 2);
}

// TODO: Expressions not really supported as yet for dense arrays
TEST_CASE_METHOD(ArrayFixture<int>, "Test Expressions Dense", "[expressions_dense]") {
  array_schema_->set_dense(1);
  Expression expression("a1==0 or a1 == 15", attribute_names, array_schema_);
  REQUIRE(expression.evaluate(buffers, buffer_sizes) == TILEDB_OK);
  const int expected_buffer[16] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
  check_buffer(buffers, buffer_sizes, expected_buffer, 16);
}
