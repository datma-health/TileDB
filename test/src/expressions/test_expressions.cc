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
  int types[2] = { TILEDB_CHAR, TILEDB_INT32 };
  int cell_val_nums[1] = { 1 };

  T buffer_a1[16] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
  void* buffers[1] { buffer_a1 };
  size_t buffer_sizes[1] = { sizeof(buffer_a1) };
  T expected_buffer[16] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };

  size_t buffer_var_a1_offsets[16] = { 0, 1*sizeof(T), 2*sizeof(T), 3*sizeof(T), 4*sizeof(T), 5*sizeof(T), 6*sizeof(T), 7*sizeof(T), 8*sizeof(T), 9*sizeof(T), 10*sizeof(T), 11*sizeof(T), 12*sizeof(T), 13*sizeof(T), 14*sizeof(T), 15*sizeof(T) };
  T buffer_var_a1[16] = { 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31 };
  void* var_buffers[2] = { buffer_var_a1_offsets, buffer_var_a1 };
  size_t var_buffer_sizes[2] = { sizeof(buffer_var_a1_offsets), sizeof(buffer_var_a1) };
  size_t expected_buffer_var_offsets[16] = { 0, 1*sizeof(T), 2*sizeof(T), 3*sizeof(T), 4*sizeof(T), 5*sizeof(T), 6*sizeof(T), 7*sizeof(T), 8*sizeof(T), 9*sizeof(T), 10*sizeof(T), 11*sizeof(T), 12*sizeof(T), 13*sizeof(T), 14*sizeof(T), 15*sizeof(T) };
  T expected_buffer_var[16] = { 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31 };

  size_t buffer_var_a1_offsets_1[8] = { 0, 1*sizeof(T), 3*sizeof(T), 5*sizeof(T), 8*sizeof(T), 11*sizeof(T), 12*sizeof(T), 13*sizeof(T) };
  void* var_buffers_1[2] = { buffer_var_a1_offsets_1, buffer_var_a1 };
  size_t var_buffer_sizes_1[2] = { sizeof(buffer_var_a1_offsets_1), sizeof(buffer_var_a1) };
  
  PosixFS *posixfs_ = 0;
  ArraySchema *array_schema_ = 0;
  
  void setup(int cell_nums) {
    // Only cell_nums =0, 1 or TILEDB_VAR_NUM has been tested. But, this can
    // be extended to support all cell_nums
    bool supported_cell_nums = cell_nums == 1 || cell_nums == 2 || cell_nums == TILEDB_VAR_NUM;
    REQUIRE(supported_cell_nums);
    delete array_schema_;
    delete posixfs_;
    if (typeid(T)== typeid(char)) {
      types[0] = TILEDB_CHAR;
    } else if (typeid(T) == typeid(uint8_t)) {
      types[0] = TILEDB_UINT8;
    } else if (typeid(T) == typeid(uint16_t)) {
      types[0] = TILEDB_UINT16;
    } else if (typeid(T) == typeid(uint32_t)) {
      types[0] = TILEDB_UINT32;
    } else if (typeid(T) == typeid(uint64_t)) {
      types[0] = TILEDB_UINT64;
    } else if (typeid(T) == typeid(int8_t)) {
      types[0] = TILEDB_INT8;
    } else if (typeid(T) == typeid(int16_t)) {
      types[0] = TILEDB_INT16;
    } else if (typeid(T) == typeid(int) || typeid(T) == typeid(int32_t)) {
      types[0] = TILEDB_INT32;
    } else if (typeid(T) == typeid(int64_t)) {
      types[0] = TILEDB_INT64;
    } else if (typeid(T) == typeid(float)) {
      types[0] = TILEDB_FLOAT32;
    } else if (typeid(T) == typeid(double)) {
      types[0] = TILEDB_FLOAT64;
    }
    posixfs_ = new PosixFS();
    array_schema_ = new ArraySchema(posixfs_);
    array_schema_->set_attributes(const_cast<char **>(attr_names), 1);
    cell_val_nums[0] = cell_nums;
    array_schema_->set_cell_val_num(cell_val_nums);
    array_schema_->set_types(types);
    array_schema_->set_dense(0);
  }

  ~ArrayFixture() {
    delete array_schema_;
    delete posixfs_;
  }

  void check_buffer(void **computed, size_t *computed_size, const T *expected, const size_t expected_size) {
    if (cell_val_nums[0] < TILEDB_VAR_NUM) {
      size_t buffer_size = *computed_size/sizeof(T);
      REQUIRE(buffer_size == expected_size);
      
      if (buffer_size) {
        T *buffer = reinterpret_cast<T *>(computed[0]);
        for (auto i=0u; i<buffer_size; i++) {
          CHECK(buffer[i] == expected[i]);
        }
      }
    } else if (cell_val_nums[0] == TILEDB_VAR_NUM) {
      size_t buffer_size = *computed_size/sizeof(size_t);
      REQUIRE(buffer_size == expected_size);
      size_t *buffer = reinterpret_cast<size_t *>(computed[0]);
      for (auto i=0u; i<buffer_size; i++) {
        CHECK(buffer[i] < 16*sizeof(T));
        if (i<buffer_size-1) {
          CHECK(buffer[i+1]-buffer[i] == sizeof(T));
        }
      }
      T *buffer_var = reinterpret_cast<T *>(computed[1]);
      for (auto i=0u; i<buffer_size; i++) {
        CHECK(buffer_var[i] == expected[i]);
      }
    }
  }
};

#define AF ArrayFixture<TestType>

TEMPLATE_TEST_CASE_METHOD(ArrayFixture, "Test Expressions Empty", "[expressions_empty]", char, int, float) {
  SECTION("cell sizes = 1") {
    AF::setup(1);
    Expression expression("", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::buffers, AF::buffer_sizes) == TILEDB_OK);
    AF::check_buffer(AF::buffers, AF::buffer_sizes, &AF::buffer_a1[0], 16);
  }
  SECTION("cell sizes = 2") {
    AF::setup(2);
    Expression expression("", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::buffers, AF::buffer_sizes) == TILEDB_OK);
    AF::check_buffer(AF::buffers, AF::buffer_sizes, &AF::buffer_a1[0], 16);
  }
   SECTION("cell sizes = TILEDB_VAR_NUM") {
    AF::setup(TILEDB_VAR_NUM);
    Expression expression("", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::var_buffers, AF::var_buffer_sizes) == TILEDB_OK);
    AF::check_buffer(AF::var_buffers, AF::var_buffer_sizes, &AF::expected_buffer_var[0], 16);
  }
}

TEMPLATE_TEST_CASE_METHOD(ArrayFixture, "Test Expressions All", "[expressions_all]", uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double) {
  SECTION("cell sizes = 1") {
    AF::setup(1);
    Expression expression("a1 >= 0", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::buffers, AF::buffer_sizes) == TILEDB_OK);
    AF::check_buffer(AF::buffers, AF::buffer_sizes, &AF::expected_buffer[0], 16);
  }
  SECTION("cell sizes = 2") {
    AF::setup(2);
    Expression expression("a1[0] >= 0 && a1[1] >=0", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::buffers, AF::buffer_sizes) == TILEDB_OK);
    AF::check_buffer(AF::buffers, AF::buffer_sizes, &AF::expected_buffer[0], 16);
  }
  SECTION("cell sizes = TILEDB_VAR_NUM") {
    AF::setup(TILEDB_VAR_NUM);
    Expression expression("a1[0] >= 0", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::var_buffers, AF::var_buffer_sizes) == TILEDB_OK);
    AF::check_buffer(AF::var_buffers, AF::var_buffer_sizes, &AF::expected_buffer_var[0], 16);
  }
}

TEMPLATE_TEST_CASE_METHOD(ArrayFixture, "Test Expressions All - Out-of-bound expression", "[expressions_all_exception]", uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double) {
  SECTION("cell sizes = 2") {
    AF::setup(2);
    Expression expression("a1[0] >= 0 && a1[2] >=0", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::buffers, AF::buffer_sizes) == TILEDB_ERR);
  }
  SECTION("cell sizes = VAR") {
    AF::setup(TILEDB_VAR_NUM);
    Expression expression("a1[0] >= 0 && a1[2] >=0", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::var_buffers, AF::var_buffer_sizes) == TILEDB_ERR);
  }
}

TEST_CASE_METHOD(ArrayFixture<int>, "Test Expressions Non-existent Attribute", "[expressions_non_existent_attr]") {
  setup(1);
  Expression expression("a2 > a1", attribute_names, array_schema_);
  CHECK_RC(expression.evaluate(buffers, buffer_sizes), TILEDB_ERR);
}

TEMPLATE_TEST_CASE_METHOD(ArrayFixture, "Test Expressions With Dropped Cells from left", "[expressions_dropped_cells_left]", int, uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double) {
  SECTION("cell sizes = 1") {
    AF::setup(1);
    Expression expression("a1 > 4", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::buffers, AF::buffer_sizes) == TILEDB_OK);
    AF::check_buffer(AF::buffers, AF::buffer_sizes, &AF::expected_buffer[5], 11);
    }
  SECTION("cell sizes = 2") {
    AF::setup(1);
    Expression expression("a1[0] > 4", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::buffers, AF::buffer_sizes) == TILEDB_OK);
    AF::check_buffer(AF::buffers, AF::buffer_sizes, &AF::expected_buffer[5], 11);
  }
  SECTION("cell sizes = TILEDB_VAR_NUM") {
    AF::setup(TILEDB_VAR_NUM);
    Expression expression("a1[0] > 20", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::var_buffers, AF::var_buffer_sizes) == TILEDB_OK);
    AF::check_buffer(AF::var_buffers, AF::var_buffer_sizes, &AF::expected_buffer_var[5], 11);
  }
  SECTION("cell sizes = TILEDB_VAR_NUM and selection expression includes a second value") {
    AF::setup(TILEDB_VAR_NUM);
    Expression expression("a1[0] > 20 && a1[0] > 21", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::var_buffers, AF::var_buffer_sizes) == TILEDB_OK);
    AF::check_buffer(AF::var_buffers, AF::var_buffer_sizes, &AF::expected_buffer_var[6], 10);
  }
}

TEMPLATE_TEST_CASE_METHOD(ArrayFixture, "Test Expressions With Dropped Cells from right", "[expressions_dropped_cells_right]", int, uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double) {
  SECTION("cell sizes = 1") {
    AF::setup(1);
    Expression expression("a1 < 4", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::buffers, AF::buffer_sizes) == TILEDB_OK);
    AF::check_buffer(AF::buffers, AF::buffer_sizes, &AF::expected_buffer[0], 4);
  }
  SECTION("cell sizes = 2") {
    AF::setup(2);
    Expression expression("a1[0] < 4", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::buffers, AF::buffer_sizes) == TILEDB_OK);
    AF::check_buffer(AF::buffers, AF::buffer_sizes, &AF::expected_buffer[0], 4);
  }
  SECTION("cell sizes = TILEDB_VAR_NUM") {
    AF::setup(TILEDB_VAR_NUM);
    Expression expression("a1[0] < 20", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::var_buffers, AF::var_buffer_sizes) == TILEDB_OK);
    AF::check_buffer(AF::var_buffers, AF::var_buffer_sizes, &AF::expected_buffer_var[0], 4);
  }
}

TEMPLATE_TEST_CASE_METHOD(ArrayFixture, "Test Expressions All Dropped Cells", "[expressions_all_dropped_cells]", int, uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double) {
  SECTION("cell sizes = 1") {
    AF::setup(1);
    Expression expression("a1 > 16", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::buffers, AF::buffer_sizes) == TILEDB_OK);
    AF::check_buffer(AF::buffers, AF::buffer_sizes, 0, 0);
  }
  SECTION("cell sizes = 2") {
    AF::setup(2);
    Expression expression("a1[0] > 16", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::buffers, AF::buffer_sizes) == TILEDB_OK);
    AF::check_buffer(AF::buffers, AF::buffer_sizes, 0, 0);
  }
  SECTION("cell sizes = TILEDB_VAR_NUM") {
    AF::setup(TILEDB_VAR_NUM);
    Expression expression("a1[0] > 32", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::var_buffers, AF::var_buffer_sizes) == TILEDB_OK);  
    AF::check_buffer(AF::var_buffers, AF::var_buffer_sizes, 0, 0);
  }
}

TEMPLATE_TEST_CASE_METHOD(ArrayFixture, "Test Expressions Some Dropped Cells", "[expressions_some_cells]", int, uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double) {
  SECTION("cell sizes = 1") {
    AF::setup(1);
    Expression expression("a1 == 15 || a1 == 7 || a1 == 0", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::buffers, AF::buffer_sizes) == TILEDB_OK);
    const TestType expected_buffer[3] = { 0, 7, 15 };
    AF::check_buffer(AF::buffers, AF::buffer_sizes, expected_buffer, 3);
  }
  SECTION("cell sizes = 2") {
    AF::setup(2);
    Expression expression("a1[0] == 6 || a1[1] == 11", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::buffers, AF::buffer_sizes) == TILEDB_OK);
    const TestType expected_buffer[4] = { 6, 7, 10, 11 };
    AF::check_buffer(AF::buffers, AF::buffer_sizes, expected_buffer, 4);
  }
  SECTION("cell sizes = TILEDB_VAR_NUM") {
    AF::setup(TILEDB_VAR_NUM);
    Expression expression("a1[0] == 26 || a1[0] == 29", AF::attribute_names, AF::array_schema_);
    REQUIRE(expression.evaluate(AF::var_buffers, AF::var_buffer_sizes) == TILEDB_OK);
    const TestType expected_buffer[2] = { 26, 29 };
    AF::check_buffer(AF::var_buffers, AF::var_buffer_sizes, expected_buffer, 2);
  }
}

TEMPLATE_TEST_CASE_METHOD(ArrayFixture, "Test Expressions with VAR_NUM and different offsets", "[expressions_different_offsets]", int, uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double) {
  AF::setup(TILEDB_VAR_NUM);
  Expression expression("a1[0] == 17 || a1[0] == 29", AF::attribute_names, AF::array_schema_);
  REQUIRE(expression.evaluate(AF::var_buffers_1, AF::var_buffer_sizes_1) == TILEDB_OK);
  CHECK(AF::var_buffer_sizes_1[0] == 2*sizeof(size_t));
  CHECK(AF::var_buffer_sizes_1[1] == 5*sizeof(TestType));
  // Check offset sizes first
  size_t expected_offsets[2] = {0, 2*sizeof(TestType)};
  size_t *buffer_offsets = reinterpret_cast<size_t *>(AF::var_buffers_1[0]);
  for (auto i=0u; i<2; i++) {
    CHECK(buffer_offsets[i] == expected_offsets[i]);
  }
  // Check cell values
  TestType expected_values[5] = {17, 18, 29, 30, 31};
  TestType *buffer_values = reinterpret_cast<TestType *>(AF::var_buffers_1[1]);
  for (auto i=0u; i<5; i++) {
    CHECK(buffer_values[i] == expected_values[i]);
  }
}

TEST_CASE("Test custom operator |= in Expression filters", "[custom_operators]") {
  const std::string array_name = "test_custom_operator_array";
  const char *attr_names[] = { "a1" };
  std::vector<std::string> attribute_names = { "a1" };
  int types[2] = { TILEDB_CHAR, TILEDB_INT32 };
  int cell_val_nums[1] = { TILEDB_VAR_NUM };

  PosixFS posix_fs;
  ArraySchema array_schema(&posix_fs);
  array_schema.set_attributes(const_cast<char **>(attr_names), 1);
  array_schema.set_cell_val_num(cell_val_nums);
  array_schema.set_types(types);
  array_schema.set_dense(0);

  size_t buffer_offsets[] = { 0, 3, 6, 13 };
  char buffer[17] = "A|CT|GA|C|T|GA|C"; // A|C T|G A|C|T|G A|C
  void *buffers[2] = { buffer_offsets, buffer };
  size_t buffer_sizes[2] = { sizeof(buffer_offsets), sizeof(buffer) };
  int64_t positions[] = { 0 };

  Expression expression("a1 |= \"A\"", attribute_names, &array_schema);
  REQUIRE(expression.evaluate_cell(buffers, buffer_sizes, positions)); // A|C
  positions[0] = 1;
  REQUIRE(!expression.evaluate_cell(buffers, buffer_sizes, positions)); // T|G
  positions[0] = 2;
  REQUIRE(expression.evaluate_cell(buffers, buffer_sizes, positions)); // A|C|T|G
  positions[0] = 3;
  REQUIRE(expression.evaluate_cell(buffers, buffer_sizes, positions)); // A|C
}

TEST_CASE_METHOD(ArrayFixture<int>, "Test custom operator &= in Expression filters", "[custom_operators]") {
  setup(2);
  SECTION("passing expression evaluation") {
    Expression expression("a1 &= \"0/1\"", attribute_names, array_schema_);
    int64_t positions[] = { 0 };
    REQUIRE(expression.evaluate_cell(buffers, buffer_sizes, positions));
  }
  SECTION("failing expression evaluation") {
    Expression expression("a1 &= \"1/0\"", attribute_names, array_schema_);
    int64_t positions[] = { 0 };
    REQUIRE(!expression.evaluate_cell(buffers, buffer_sizes, positions));
  }
  SECTION("another failing expression evaluation") {
    Expression expression("a1 &= \"0\"", attribute_names, array_schema_);
    int64_t positions[] = { 0 };
    REQUIRE(!expression.evaluate_cell(buffers, buffer_sizes, positions));
  }
}

TEST_CASE("Test custom operator &= in Expression filters with delimiters", "[custom_operators]") {
  const std::string array_name = "test_custom_operator_array";
  const char *attr_names[] = { "a1" };
  std::vector<std::string> attribute_names = { "a1" };
  int types[2] = { TILEDB_INT32 };
  int cell_val_nums[1] = { 3 };

  PosixFS posix_fs;
  ArraySchema array_schema(&posix_fs);
  array_schema.set_attributes(const_cast<char **>(attr_names), 1);
  array_schema.set_cell_val_num(cell_val_nums);
  array_schema.set_types(types);
  array_schema.set_dense(0);

  int buffer[12] = { 0, 1, 1, 2, 1, 0, 0, 1, 1, 3, 0, 3 };
  void *buffers[1] = { buffer };
  size_t buffer_sizes[1] = { sizeof(buffer) };
  int64_t positions[] = { 0 };

  SECTION("with 0|1") {
    Expression expression("a1 &= \"0|1\"", attribute_names, &array_schema);
    REQUIRE(expression.evaluate_cell(buffers, buffer_sizes, positions));
    positions[0] = 1;
    REQUIRE(!expression.evaluate_cell(buffers, buffer_sizes, positions));
    positions[0] = 2;
    REQUIRE(expression.evaluate_cell(buffers, buffer_sizes, positions));
    positions[0] = 3;
    REQUIRE(!expression.evaluate_cell(buffers, buffer_sizes, positions));
  }
  SECTION("with 20") {
    Expression expression("a1 &= \"20\"", attribute_names, &array_schema);
    REQUIRE(!expression.evaluate_cell(buffers, buffer_sizes, positions));
    positions[0] = 1;
    REQUIRE(expression.evaluate_cell(buffers, buffer_sizes, positions));
    positions[0] = 2;
    REQUIRE(!expression.evaluate_cell(buffers, buffer_sizes, positions));
    positions[0] = 3;
    REQUIRE(!expression.evaluate_cell(buffers, buffer_sizes, positions));
  }
  SECTION("with 3|3") {
    Expression expression("a1 &= \"3|3\"", attribute_names, &array_schema);
    REQUIRE(!expression.evaluate_cell(buffers, buffer_sizes, positions));
    positions[0] = 1;
    REQUIRE(!expression.evaluate_cell(buffers, buffer_sizes, positions));
    positions[0] = 2;
    REQUIRE(!expression.evaluate_cell(buffers, buffer_sizes, positions));
    positions[0] = 3;
    REQUIRE(expression.evaluate_cell(buffers, buffer_sizes, positions));
  }
}

// TODO: Expressions not really supported as yet for dense arrays
TEST_CASE_METHOD(ArrayFixture<int>, "Test Expressions Dense", "[expressions_dense]") {
  setup(1);
  array_schema_->set_dense(1);
  Expression expression("a1==0 or a1 == 15", attribute_names, array_schema_);
  REQUIRE(expression.evaluate(buffers, buffer_sizes) == TILEDB_OK);
  const int expected_buffer[16] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
  check_buffer(buffers, buffer_sizes, expected_buffer, 16);
}
