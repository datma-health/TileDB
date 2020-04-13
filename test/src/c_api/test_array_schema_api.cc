/**
 * @file test_array_schema_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 * @copyright Copyright (c) 2018-2020 Omics Data Automation, Inc.
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
 * Tests for the C API array schema spec.
 */

#include "c_api_array_schema_spec.h"
#include "utils.h"
#include <unistd.h>

#include "catch.h"

using Catch::Equals;
using Catch::EndsWith;

ArraySchemaTestFixture::ArraySchemaTestFixture() {
  // Error code
  int rc;

  // Array schema not set yet
  array_schema_set_ = false;
 
  // Initialize context
  rc = tiledb_ctx_init(&tiledb_ctx_, NULL);
  REQUIRE(rc == TILEDB_OK);

  // Create workspace
  rc = tiledb_workspace_create(tiledb_ctx_, WORKSPACE.c_str());
  REQUIRE(rc == TILEDB_OK);
 
  // Set array name
  array_name_ = WORKSPACE + ARRAYNAME;
}

ArraySchemaTestFixture::~ArraySchemaTestFixture() {
  // Error code
  int rc;

  // Finalize TileDB context
  rc = tiledb_ctx_finalize(tiledb_ctx_);
  REQUIRE(rc == TILEDB_OK);

  // Remove the temporary workspace
  std::string command = "rm -rf ";
  command.append(WORKSPACE);
  rc = system(command.c_str());
  REQUIRE(rc == 0);

  // Free array schema
  if(array_schema_set_) {
    rc = tiledb_array_free_schema(&array_schema_);
    REQUIRE(rc == TILEDB_OK);
  }
}




/* ****************************** */
/*         PUBLIC METHODS         */
/* ****************************** */

int ArraySchemaTestFixture::create_dense_array(std::string array_name, int attribute_datatype, int compression_type) {
  // Initialization s
  int rc;
  const char* attributes[] = { "MY_ATTRIBUTE" };
  const char* dimensions[] = { "X", "Y" };
  int64_t domain[] = { 0, 99, 0, 99 };
  int64_t tile_extents[] = { 10, 10 };
  const int types[] = { attribute_datatype, TILEDB_INT64 };
  const int compression[] = { compression_type, TILEDB_NO_COMPRESSION };

  // Set array schema
  rc = tiledb_array_set_schema(
      // The array schema structure
      &array_schema_,
      // Array name
      array_name.c_str(),
      // Attributes
      attributes,
      // Number of attributes
      1,
      // Capacity
      1000,
      // Cell order
      TILEDB_COL_MAJOR,
      // Number of cell values per attribute (NULL means 1 everywhere)
      NULL,
      // Compression
      compression,
      // Compression level, NULL will get defaults
      NULL,
      // Dense array
      1,
      // Dimensions
      dimensions,
      // Number of dimensions
      2,
      // Domain
      domain,
      // Domain length in bytes
      4*sizeof(int64_t),
      // Tile extents (no regular tiles defined)
      tile_extents,
      // Tile extents in bytes
      2*sizeof(int64_t),
      // Tile order (0 means ignore in sparse arrays and default in dense)
      0,
      // Types
      types
  );
  if(rc != TILEDB_OK)
    return TILEDB_ERR;

  // Remember that the array schema is set
  array_schema_set_ = true;

  // Create the array
  return tiledb_array_create(tiledb_ctx_, &array_schema_);
}

void ArraySchemaTestFixture::check_dense_array(std::string array_name) {
  int rc;

  // Load array schema from the disk
  TileDB_ArraySchema array_schema_disk;
  rc = tiledb_array_load_schema(
           tiledb_ctx_, 
           array_name.c_str(), 
           &array_schema_disk);
  REQUIRE(rc == TILEDB_OK);

  // For easy reference
  int64_t* tile_extents_disk = 
      static_cast<int64_t*>(array_schema_disk.tile_extents_);
  int64_t* tile_extents = 
      static_cast<int64_t*>(array_schema_.tile_extents_);

  // Get real array path
  std::string array_name_real = fs_.real_dir(array_name);
  CHECK_FALSE(array_name_real == "");
  CHECK_THAT(array_name_real, EndsWith(array_name));

  // Tests
  //absolute path isn't relevant anymore
  //ASSERT_STREQ(array_schema_disk.array_name_, array_name_real.c_str());
  CHECK(array_schema_disk.attribute_num_ == array_schema_.attribute_num_);
  CHECK(array_schema_disk.dim_num_ == array_schema_.dim_num_);
  CHECK(array_schema_disk.capacity_ == array_schema_.capacity_);
  CHECK(array_schema_disk.cell_order_ == array_schema_.cell_order_);
  CHECK(array_schema_disk.tile_order_ == array_schema_.tile_order_);
  CHECK(array_schema_disk.dense_ == array_schema_.dense_);
  CHECK_THAT(array_schema_disk.attributes_[0], Equals(array_schema_.attributes_[0]));
  CHECK(array_schema_disk.compression_[0] == array_schema_.compression_[0]);
  CHECK(array_schema_disk.compression_[1] == array_schema_.compression_[1]);
  CHECK(array_schema_disk.types_[0] == array_schema_.types_[0]);
  CHECK(array_schema_disk.types_[1] == array_schema_.types_[1]);
  CHECK(tile_extents_disk[0] == tile_extents[0]);
  CHECK(tile_extents_disk[1] == tile_extents[1]);

  // Free array schema
  rc = tiledb_array_free_schema(&array_schema_disk);
  REQUIRE(rc == TILEDB_OK);
}




/* ****************************** */
/*             TESTS              */
/* ****************************** */

/**
 * Tests the array schema creation and retrieval.
 */
TEST_CASE_METHOD(ArraySchemaTestFixture, "Test Array Schema char", "[array_schema_char]") {
  std::string array_name = array_name_ + "char";

  // Create array
  int rc = create_dense_array(array_name, TILEDB_CHAR, TILEDB_NO_COMPRESSION);
  REQUIRE(rc == TILEDB_OK);

  check_dense_array(array_name);
}

TEST_CASE_METHOD(ArraySchemaTestFixture, "Test Array Schema int8", "[array_schema_int8]") {
  std::string array_name = array_name_ + "int8";

  // Create array
  int rc = create_dense_array(array_name, TILEDB_INT8, TILEDB_NO_COMPRESSION);
  REQUIRE(rc == TILEDB_OK);

  check_dense_array(array_name);
}

TEST_CASE_METHOD(ArraySchemaTestFixture, "Test Array Schema int16", "[array_schema_int16]") {
  std::string array_name = array_name_ + "int16";

  // Create array
  int rc = create_dense_array(array_name, TILEDB_INT16, TILEDB_ZSTD);
  REQUIRE(rc == TILEDB_OK);

  check_dense_array(array_name);
}

TEST_CASE_METHOD(ArraySchemaTestFixture, "Test Array Schema int32", "[array_schema_int32]") {
  std::string array_name = array_name_ + "int32";

  // Create array
  int rc = create_dense_array(array_name, TILEDB_INT32, TILEDB_LZ4);
  REQUIRE(rc == TILEDB_OK);

  check_dense_array(array_name);
}

TEST_CASE_METHOD(ArraySchemaTestFixture, "Test Array Schema int64", "[array_schema_int64]") {
  std::string array_name = array_name_ + "int64";

  // Create array
  int rc = create_dense_array(array_name, TILEDB_INT64, TILEDB_BLOSC);
  REQUIRE(rc == TILEDB_OK);

  check_dense_array(array_name);
}

TEST_CASE_METHOD(ArraySchemaTestFixture, "Test Array Schema uint8", "[array_schema_uint8]") {
  std::string array_name = array_name_ + "uint8";

  // Create array
  int rc = create_dense_array(array_name, TILEDB_UINT8, TILEDB_GZIP);
  REQUIRE(rc == TILEDB_OK);

  check_dense_array(array_name);
}

TEST_CASE_METHOD(ArraySchemaTestFixture, "Test Array Schema uint16", "[array_schema_uint16]") {
  std::string array_name = array_name_ + "uint16";

  // Create array
  int rc = create_dense_array(array_name, TILEDB_UINT16, TILEDB_ZSTD);
  REQUIRE(rc == TILEDB_OK);

  check_dense_array(array_name);
}

TEST_CASE_METHOD(ArraySchemaTestFixture, "Test Array Schema uint32", "[array_schema_uint32]") {
  std::string array_name = array_name_ + "uint32";

  // Create array
  int rc = create_dense_array(array_name, TILEDB_UINT32, TILEDB_LZ4);
  REQUIRE(rc == TILEDB_OK);

  check_dense_array(array_name);
}

TEST_CASE_METHOD(ArraySchemaTestFixture, "Test Array Schema uint64", "[array_schema_uint64]") {
  std::string array_name = array_name_ + "uint64";

  // Create array
  int rc = create_dense_array(array_name, TILEDB_UINT64, TILEDB_LZ4);
  REQUIRE(rc == TILEDB_OK);

  check_dense_array(array_name);
}

TEST_CASE_METHOD(ArraySchemaTestFixture, "Test Array Schema float32", "[array_schema_float32]") {
  std::string array_name = array_name_ + "float32";

  // Create array
  int rc = create_dense_array(array_name, TILEDB_FLOAT32, TILEDB_RLE);
  REQUIRE(rc == TILEDB_OK);

  check_dense_array(array_name);
}

TEST_CASE_METHOD(ArraySchemaTestFixture, "Test Array Schema float64", "[array_schema_float64]") {
  std::string array_name = array_name_ + "float64";

  // Create array
  int rc = create_dense_array(array_name, TILEDB_FLOAT64, TILEDB_BLOSC);
  REQUIRE(rc == TILEDB_OK);

  check_dense_array(array_name);
}


TEST_CASE("Test array schema backward compatibility", "[compatibility]") {
  TileDB_CTX* tiledb_ctx;
  int rc = tiledb_ctx_init(&tiledb_ctx, NULL);
  REQUIRE(rc == TILEDB_OK);

  PosixFS fs;
  std::string array_name = std::string(TILEDB_TEST_DIR)+"/inputs/compatibility_gdb_pre100_ws/t0_1_2";
  REQUIRE(fs.is_dir(array_name));

  TileDB_ArraySchema array_schema;
  rc = tiledb_array_load_schema(tiledb_ctx, array_name.c_str(), &array_schema);
  REQUIRE(rc == TILEDB_OK);

  CHECK_THAT(array_schema.array_name_, Equals("/tmp/tmp7l5mFz/ws/t0_1_2"));
  CHECK(array_schema.attribute_num_ == 22);
  CHECK(array_schema.capacity_ == 3);
  CHECK(array_schema.cell_order_ == 1);
  CHECK(array_schema.dim_num_ == 2);
  CHECK(array_schema.compression_[0] == 1);
}

