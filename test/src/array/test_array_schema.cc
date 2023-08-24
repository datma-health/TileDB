/**
 * @file test_array_schema.cc 
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 Omics Data Automation, Inc.
 * @copyright Copyright (C) 2023 dātma, inc™
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
 * Tests for array schema.
 */

#include "array_schema.h"
#include "utils.h"
#include "storage_posixfs.h"
#include <unistd.h>

#include "catch.h"

class ArraySchemaTestClass {
 public:
  const char *ws = "this_workspace";
  const char *array = "this_array";
  const char* attributes[1] = {"this_attribute"};
  const char* dimensions[1] = { "one_dim" };
  int32_t domain[2] = {1, 100};
  int32_t tile_extents[1] = {25};
  int types[2] =  {TILEDB_INT32, TILEDB_INT32};

  ArraySchemaC c_schema;
  
  void init() {
    c_schema.array_workspace_= const_cast<char *>(ws);
    c_schema.array_name_= const_cast<char *>(array);
    c_schema.attributes_= const_cast<char **>(attributes);
    c_schema.attribute_num_ = 1;
    c_schema.capacity_= 0;
    c_schema.cell_order_= TILEDB_ROW_MAJOR;
    c_schema.cell_val_num_= NULL;
    c_schema.compression_= {0};
    c_schema.compression_level_ = NULL;
    c_schema.offsets_compression_ = NULL;
    c_schema.offsets_compression_level_ = NULL;
    c_schema.dimensions_ = const_cast<char **>(dimensions);
    c_schema.dim_num_= 1;
    c_schema.domain_= domain;
    c_schema.tile_extents_= tile_extents;
    c_schema.tile_order_= TILEDB_ROW_MAJOR;
    c_schema.types_= types;
  }
};

class ArraySchemaTestClass64BitDim {
 public:
  const char *ws = "this_workspace";
  const char *array = "this_array_64";
  const char* attributes[1] = {"this_attribute"};
  const char* dimensions[1] = { "one_dim_64" };
  int64_t domain[2] = {1, 1<33};
  int64_t tile_extents[1] = {25};
  int types[2] =  {TILEDB_INT64, TILEDB_INT64};

  ArraySchemaC c_schema;

  void init() {
    c_schema.array_workspace_= const_cast<char *>(ws);
    c_schema.array_name_= const_cast<char *>(array);
    c_schema.attributes_= const_cast<char **>(attributes);
    c_schema.attribute_num_ = 1;
    c_schema.capacity_= 0;
    c_schema.cell_order_= TILEDB_ROW_MAJOR;
    c_schema.cell_val_num_= NULL;
    c_schema.compression_= {0};
    c_schema.compression_level_ = NULL;
    c_schema.offsets_compression_ = NULL;
    c_schema.offsets_compression_level_ = NULL;
    c_schema.dimensions_ = const_cast<char **>(dimensions);
    c_schema.dim_num_= 1;
    c_schema.domain_= domain;
    c_schema.tile_extents_= tile_extents;
    c_schema.tile_order_= TILEDB_ROW_MAJOR;
    c_schema.types_= types;
  }
};


TEST_CASE("Test Array Schema Version", "[array_schema_version]") {
  ArraySchema schema(NULL);
  CHECK(schema.version_tag_exists());
  CHECK(schema.get_version() == TILEDB_ARRAY_SCHEMA_VERSION_TAG);
}

TEST_CASE("Test Array Schema", "[array_schema]") {
  ArraySchema schema(new PosixFS());
  schema.set_array_workspace(NULL);
  schema.set_array_name(NULL);
  CHECK(schema.set_attributes(NULL, 0) != TILEDB_OK);

  const char* attr1[1] = {"test_attr"};
  CHECK(schema.set_attributes(const_cast<char **>(attr1), 0) != TILEDB_OK);
  CHECK(schema.set_attributes(const_cast<char **>(attr1), 1) == TILEDB_OK);
  const char* attr2[2] = {"test_attr", "test_attr"};
  CHECK(schema.set_attributes(const_cast<char **>(attr2), 2) != TILEDB_OK);
  const char* attr3[2] = {"test_attr", "test_attr1"};
  CHECK(schema.set_attributes(const_cast<char **>(attr3), 2) == TILEDB_OK);
  
  schema.set_capacity(100);
  schema.set_capacity(0);
  
  schema.set_cell_val_num(NULL);
  const int cell_val_num[2] = {1, TILEDB_VAR_NUM};
  schema.set_cell_val_num(const_cast<int *>(cell_val_num));
  
  CHECK(schema.set_cell_order(-100) != TILEDB_OK);
  CHECK(schema.set_cell_order(TILEDB_ROW_MAJOR) == TILEDB_OK);
  CHECK(schema.set_cell_order(TILEDB_COL_MAJOR) == TILEDB_OK);
  
  CHECK(schema.set_compression(NULL) == TILEDB_OK);
  CHECK(schema.set_compression_level(NULL) == TILEDB_OK);

  CHECK(schema.set_offsets_compression(NULL) == TILEDB_OK);
  CHECK(schema.set_offsets_compression_level(NULL) == TILEDB_OK);

  const int compression[3] = {TILEDB_GZIP, TILEDB_GZIP, TILEDB_GZIP};
  CHECK(schema.set_compression(const_cast<int *>(compression)) == TILEDB_OK);
  CHECK(schema.set_offsets_compression(NULL) == TILEDB_OK);
  CHECK(schema.set_offsets_compression(const_cast<int *>(compression)) == TILEDB_OK);
  const int offsets_compression[2] = {0, 0};
  CHECK(schema.set_offsets_compression(const_cast<int *>(offsets_compression)) != TILEDB_OK);
  const int offsets_compression1[2] = {0, TILEDB_GZIP+TILEDB_DELTA_ENCODE};
  CHECK(schema.set_offsets_compression(const_cast<int *>(offsets_compression1)) == TILEDB_OK);

  CHECK(schema.set_dimensions(NULL, 0) != TILEDB_OK);
  const char* dim1[1] = {"dim1"};
  CHECK(schema.set_dimensions(const_cast<char **>(dim1), 0) != TILEDB_OK);
  CHECK(schema.set_dimensions(const_cast<char **>(dim1), 1) == TILEDB_OK);
  const char* dim2[2] = {"dim1", "dim1"};
  CHECK(schema.set_dimensions(const_cast<char **>(dim2), 2) != TILEDB_OK);
  CHECK(schema.set_dimensions(const_cast<char **>(attr1), 1) != TILEDB_OK);
  const char* dim3[2] = {"dim1", "dim2"};
  CHECK(schema.set_dimensions(const_cast<char **>(dim3), 2) == TILEDB_OK);

  CHECK(schema.set_types(NULL) != TILEDB_OK);
  int attr_types1[2] = {-100, -100};
  CHECK(schema.set_types(attr_types1) != TILEDB_OK);
  int attr_types2[3] = {TILEDB_INT32, TILEDB_INT32, TILEDB_INT32};
  CHECK(schema.set_types(attr_types2) == TILEDB_OK);

  CHECK(schema.set_domain(NULL) != TILEDB_OK);
  int domain[4] = {0,99,0,99};
  CHECK(schema.set_domain(domain) == TILEDB_OK);
  
  schema.set_dense(0);
  
  CHECK(schema.set_tile_extents(NULL) == TILEDB_OK);
  CHECK(schema.set_tile_order(-1) != TILEDB_OK);

  schema.print();
}

TEST_CASE_METHOD(ArraySchemaTestClass, "Test Array Schema Print", "[array_schema_print]") {
  init();
  ArraySchema schema(NULL);
  CHECK(schema.init(&c_schema) == TILEDB_OK);
  schema.print();
}

TEST_CASE_METHOD(ArraySchemaTestClass, "Test Array Schema Init With Print", "[array_schema_init_with_print]") {
  init();
  ArraySchema schema(NULL);
  CHECK(schema.init(&c_schema, true) == TILEDB_OK);
}

TEST_CASE_METHOD(ArraySchemaTestClass, "Test Array Schema Serialize", "[array_schema_serialize]") {
  init();
  ArraySchema schema(NULL);
  CHECK(schema.init(&c_schema) == TILEDB_OK);
  void *array_schema_serialized;
  size_t array_schema_serialized_size;
  CHECK(schema.serialize(array_schema_serialized, array_schema_serialized_size) == TILEDB_OK);
  CHECK(array_schema_serialized_size > 0);
  ArraySchema schema_for_deserialize(NULL);
  CHECK(schema_for_deserialize.deserialize(array_schema_serialized, array_schema_serialized_size) == TILEDB_OK);
}

TEST_CASE_METHOD(ArraySchemaTestClass64BitDim, "Test Array Schema Print 64-Bit", "[array_schema_print_64]") {
  init();
  ArraySchema schema(NULL);
  CHECK(schema.init(&c_schema) == TILEDB_OK);
  schema.print();
}

TEST_CASE_METHOD(ArraySchemaTestClass64BitDim, "Test Array Schema Serialize 64-Bit", "[array_schema_serialize_64]") {
  init();
  ArraySchema schema(NULL);
  CHECK(schema.init(&c_schema) == TILEDB_OK);
  void *array_schema_serialized;
  size_t array_schema_serialized_size;
  CHECK(schema.serialize(array_schema_serialized, array_schema_serialized_size) == TILEDB_OK);
  CHECK(array_schema_serialized_size > 0);
  ArraySchema schema_for_deserialize(NULL);
  CHECK(schema_for_deserialize.deserialize(array_schema_serialized, array_schema_serialized_size) == TILEDB_OK);
}
