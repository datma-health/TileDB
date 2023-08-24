/**
 * @file   test_utils.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 * @copyright Copyright (c) 2018-2019,2021 Omics Data Automation, Inc.
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
 * Tests for the utility functionality.
 */

/* ****************************** */
/*             TESTS              */
/* ****************************** */

#include "catch.h"
#include "storage_posixfs.h"
#include "utils.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

/** Tests RLE compression (attribute). */
TEST_CASE("Test RLE compression(attribute)", "rle") {
  // Initializations
  unsigned char input[1000000];
  size_t input_size = 0;
  unsigned char compressed[1000000];
  size_t compressed_size = 0;
  unsigned char decompressed[1000000];
  size_t decompressed_size = 0;
  size_t value_size;
  size_t run_size = 6;
  size_t compress_bound;
  int64_t rc;

  // === Attribute compression (value_size = sizeof(int)) === //
  value_size = sizeof(int);

  // Test empty bufffer
  rc = RLE_compress(input, input_size, compressed, compressed_size, value_size);
  CHECK(rc == 0);

  // Test input buffer invalid format
  input_size = 5; 
  rc = RLE_compress(input, input_size, compressed, compressed_size, value_size);
  CHECK(rc == TILEDB_UT_ERR);

  // Test output buffer overflow
  input_size = 16; 
  rc = RLE_compress(input, input_size, compressed, compressed_size, value_size);
  CHECK(rc == TILEDB_UT_ERR);

  // Test compress bound 
  compress_bound = RLE_compress_bound(input_size, value_size);
  CHECK(compress_bound == input_size + ((input_size/value_size) * 2));

  // Test all values unique (many unitary runs)
  for(int i=0; i<100; ++i) 
    memcpy(input + i*value_size, &i, value_size); 
  input_size = 100*value_size;
  compressed_size = RLE_compress_bound(input_size, value_size);
  rc = RLE_compress(input, input_size, compressed, compressed_size, value_size);
  REQUIRE(static_cast<size_t>(rc) == compressed_size);
  decompressed_size = input_size;
  rc = RLE_decompress(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size);
  REQUIRE(rc == TILEDB_UT_OK);
  CHECK_FALSE(memcmp(input, decompressed, input_size));

  // Test all values the same (a single long run)
  int v = 111;
  for(int i=0; i<100; ++i) 
    memcpy(input + i*value_size, &v, value_size); 
  input_size = 100*value_size;
  compressed_size = 
      RLE_compress(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size);
  REQUIRE(compressed_size == run_size);
  decompressed_size = input_size;
  rc = RLE_decompress(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size);
  REQUIRE(rc == TILEDB_UT_OK);
  CHECK_FALSE(memcmp(input, decompressed, input_size));

  // Test a mix of short and long runs
  for(int i=0; i<10; ++i) 
    memcpy(input + i*value_size, &i, value_size); 
  for(int i=10; i<100; ++i) 
    memcpy(input + i*value_size, &v, value_size); 
  for(int i=100; i<110; ++i) 
    memcpy(input + i*value_size, &i, value_size); 
  input_size = 110*value_size;
  compressed_size = RLE_compress_bound(input_size, value_size);
  compressed_size = 
      RLE_compress(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size);
  REQUIRE(compressed_size == 21*run_size);
  decompressed_size = input_size;
  rc = RLE_decompress(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size);
  CHECK(rc == TILEDB_UT_OK);
  CHECK_FALSE(memcmp(input, decompressed, input_size));
  
  // Test when a run exceeds max run length
  for(int i=0; i<10; ++i) 
    memcpy(input + i*value_size, &i, value_size); 
  for(int i=10; i<70010; ++i) 
    memcpy(input + i*value_size, &v, value_size); 
  for(int i=70010; i<70030; ++i) 
    memcpy(input + i*value_size, &i, value_size); 
  input_size = 70030*value_size;
  compressed_size = RLE_compress_bound(input_size, value_size);
  compressed_size = 
      RLE_compress(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size);
  CHECK(compressed_size == 32*run_size);
  decompressed_size = input_size;
  rc = RLE_decompress(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size);
  CHECK(rc == TILEDB_UT_OK);
  CHECK_FALSE(memcmp(input, decompressed, input_size));

  // === Attribute compression (value_size = 2*sizeof(double)) === //
  value_size = 2*sizeof(double);
  run_size = value_size + 2;

  // Test a mix of short and long runs
  double j = 0.1, k = 0.2;
  for(int i=0; i<10; ++i) { 
    j+= 10000.12;
    memcpy(input + 2*i*sizeof(double), &j, value_size); 
    k+= 1000.12;
    memcpy(input + (2*i+1)*sizeof(double), &k, value_size); 
  }
  j+= 10000.12;
  k+= 1000.12;
  for(int i=10; i<100; ++i) { 
    memcpy(input + 2*i*sizeof(double), &j, value_size); 
    memcpy(input + (2*i+1)*sizeof(double), &k, value_size); 
  }
  for(int i=100; i<110; ++i) { 
    j+= 10000.12;
    memcpy(input + 2*i*sizeof(double), &j, value_size); 
    k+= 1000.12;
    memcpy(input + (2*i+1)*sizeof(double), &k, value_size); 
  }
  input_size = 110*value_size;
  compressed_size = RLE_compress_bound(input_size, value_size);
  compressed_size = 
      RLE_compress(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size);
  REQUIRE(compressed_size == 21*run_size);
  decompressed_size = input_size;
  rc = RLE_decompress(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size);
  CHECK(rc == TILEDB_UT_OK);
  CHECK_FALSE(memcmp(input, decompressed, input_size));
}

/** Tests RLE compression (coordinates, row-major cell order). */
TEST_CASE("Tests RLE compression (coordinates, row-major cell order)", "[test_RLE_coords_row]") {
  // Initializations
  unsigned char input[1000000];
  unsigned char compressed[1000000];
  unsigned char decompressed[1000000];
  size_t input_size = 0;
  size_t compressed_size = 0;
  size_t decompressed_size = 0;
  size_t value_size;
  size_t coords_size;
  size_t run_size;
  size_t compress_bound;
  int dim_num = 2;
  int rc;

  // === Coordinates compression (row-major) === //
  value_size = sizeof(int);
  coords_size = dim_num*value_size;
  run_size = sizeof(int) + 2*sizeof(char);

  // Test empty bufffer
  rc = RLE_compress_coords_row(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  CHECK(rc == 0);

  // Test input buffer invalid format
  input_size = 5; 
  rc = RLE_compress_coords_row(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  CHECK(rc == TILEDB_UT_ERR);

  // Test output buffer overflow
  input_size = 16; 
  compressed_size = 0;
  rc = RLE_compress_coords_row(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  CHECK(rc == TILEDB_UT_ERR);

  // Test compress bound 
  input_size = 64;
  compress_bound = RLE_compress_bound_coords(input_size, value_size, dim_num);
  int64_t cell_num = input_size / coords_size;
  size_t compress_bound_expected = 
      input_size + cell_num*(dim_num-1)*2 + sizeof(int64_t);
  CHECK(compress_bound == compress_bound_expected);

  // Test all values unique (many unitary runs)
  int v;
  for(int i=0; i<100; ++i) { 
    v = i;
    memcpy(input + 2*i*value_size, &v, value_size); 
    memcpy(input + (2*i+1)*value_size, &i, value_size); 
  }
  input_size = 100*value_size*dim_num;
  compressed_size = RLE_compress_bound_coords(input_size, value_size, dim_num);
  rc = RLE_compress_coords_row(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  REQUIRE(static_cast<size_t>(rc) == compressed_size);
  decompressed_size = input_size;
  rc = RLE_decompress_coords_row(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size,
           dim_num);
  CHECK(rc == TILEDB_UT_OK);
  CHECK_FALSE(memcmp(input, decompressed, input_size));

  // Test all values the same (a single long run)
  v = 111;
  for(int i=0; i<100; ++i) { 
    memcpy(input + 2*i*value_size, &v, value_size); 
    memcpy(input + (2*i+1)*value_size, &i, value_size); 
  }
  input_size = 100*value_size*dim_num;
  compressed_size = RLE_compress_bound_coords(input_size, value_size, dim_num);
  compressed_size = 
      RLE_compress_coords_row(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size,
          dim_num);
  REQUIRE(compressed_size == 100*value_size + run_size + sizeof(int64_t));
  decompressed_size = input_size;
  rc = RLE_decompress_coords_row(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size,
           dim_num);
  CHECK(rc == TILEDB_UT_OK);
  CHECK_FALSE(memcmp(input, decompressed, input_size));

  // Test a mix of short and long runs
  for(int i=0; i<10; ++i) {
    v = i;
    memcpy(input + 2*i*value_size, &v, value_size); 
    memcpy(input + (2*i+1)*value_size, &i, value_size); 
  }
  v = 111;
  for(int i=10; i<90; ++i) { 
    memcpy(input + 2*i*value_size, &v, value_size); 
    memcpy(input + (2*i+1)*value_size, &i, value_size); 
  }
  for(int i=90; i<100; ++i) { 
    v = i;
    memcpy(input + 2*i*value_size, &v, value_size); 
    memcpy(input + (2*i+1)*value_size, &i, value_size); 
  }
  input_size = 100*value_size*dim_num;
  compressed_size = RLE_compress_bound_coords(input_size, value_size, dim_num);
  compressed_size = 
      RLE_compress_coords_row(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size,
          dim_num);
  REQUIRE(compressed_size == 100*value_size + 21*run_size + sizeof(int64_t));
  decompressed_size = input_size;
  rc = RLE_decompress_coords_row(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size,
           dim_num);
  CHECK(rc == TILEDB_UT_OK);
  CHECK_FALSE(memcmp(input, decompressed, input_size));
}

/** Tests RLE compression (coordinates, column-major cell order). */
TEST_CASE("Tests RLE compression (coordinates, column-major cell order)", "[test_RLE_coords_col]") {
  // Initializations
  unsigned char input[1000000];
  unsigned char compressed[1000000];
  unsigned char decompressed[1000000];
  size_t input_size = 0;
  size_t compressed_size = 0;
  size_t decompressed_size = 0;
  size_t value_size;
  size_t coords_size;
  size_t run_size;
  size_t compress_bound;
  int dim_num = 2;
  int rc;

  // === Coordinates compression (row-major) === //
  value_size = sizeof(int);
  coords_size = dim_num*value_size;
  run_size = sizeof(int) + 2*sizeof(char);

  // Test empty bufffer
  rc = RLE_compress_coords_col(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  CHECK(rc == 0);

  // Test input buffer invalid format
  input_size = 5; 
  rc = RLE_compress_coords_col(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  CHECK(rc == TILEDB_UT_ERR);

  // Test output buffer overflow
  input_size = 16; 
  compressed_size = 0;
  rc = RLE_compress_coords_col(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  CHECK(rc == TILEDB_UT_ERR);

  // Test compress bound 
  input_size = 64;
  compress_bound = RLE_compress_bound_coords(input_size, value_size, dim_num);
  int64_t cell_num = input_size / coords_size;
  size_t compress_bound_expected = 
      input_size + cell_num*(dim_num-1)*2 + sizeof(int64_t);
  CHECK(compress_bound == compress_bound_expected);

  // Test all values unique (many unitary runs)
  int v;
  for(int i=0; i<100; ++i) { 
    v = i;
    memcpy(input + 2*i*value_size, &i, value_size); 
    memcpy(input + (2*i+1)*value_size, &v, value_size); 
  }
  input_size = 100*value_size*dim_num;
  compressed_size = RLE_compress_bound_coords(input_size, value_size, dim_num);
  rc = RLE_compress_coords_col(
           input, 
           input_size, 
           compressed, 
           compressed_size, 
           value_size,
           dim_num);
  REQUIRE(static_cast<size_t>(rc) == compressed_size);
  decompressed_size = input_size;
  rc = RLE_decompress_coords_col(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size,
           dim_num);
  CHECK(rc == TILEDB_UT_OK);
  CHECK_FALSE(memcmp(input, decompressed, input_size));

  // Test all values the same (a single long run)
  v = 111;
  for(int i=0; i<100; ++i) { 
    memcpy(input + 2*i*value_size, &i, value_size); 
    memcpy(input + (2*i+1)*value_size, &v, value_size); 
  }
  input_size = 100*value_size*dim_num;
  compressed_size = RLE_compress_bound_coords(input_size, value_size, dim_num);
  compressed_size = 
      RLE_compress_coords_col(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size,
          dim_num);
  CHECK(compressed_size == 100*value_size + run_size + sizeof(int64_t));
  decompressed_size = input_size;
  rc = RLE_decompress_coords_col(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size,
           dim_num);
  CHECK(rc == TILEDB_UT_OK);
  CHECK_FALSE(memcmp(input, decompressed, input_size));

  // Test a mix of short and long runs
  for(int i=0; i<10; ++i) {
    v = i;
    memcpy(input + 2*i*value_size, &i, value_size); 
    memcpy(input + (2*i+1)*value_size, &v, value_size); 
  }
  v = 111;
  for(int i=10; i<90; ++i) { 
    memcpy(input + 2*i*value_size, &i, value_size); 
    memcpy(input + (2*i+1)*value_size, &v, value_size); 
  }
  for(int i=90; i<100; ++i) { 
    v = i;
    memcpy(input + 2*i*value_size, &i, value_size); 
    memcpy(input + (2*i+1)*value_size, &v, value_size); 
  }
  input_size = 100*value_size*dim_num;
  compressed_size = RLE_compress_bound_coords(input_size, value_size, dim_num);
  compressed_size = 
      RLE_compress_coords_col(
          input, 
          input_size, 
          compressed, 
          compressed_size, 
          value_size,
          dim_num);
  REQUIRE(compressed_size == 100*value_size + 21*run_size + sizeof(int64_t));
  decompressed_size = input_size;
  rc = RLE_decompress_coords_col(
           compressed, 
           compressed_size, 
           decompressed, 
           decompressed_size,
           value_size,
           dim_num);
  CHECK(rc == TILEDB_UT_OK);
  CHECK_FALSE(memcmp(input, decompressed, input_size));
}

TEST_CASE_METHOD(TempDir, "Test utils file system operations", "[test_utils_fs]") {
  PosixFS test_fs;
  PosixFS *fs = &test_fs;
  
  std::string temp_dir = get_temp_dir()+"/temp";
  CHECK(create_dir(fs, "/non-existent-dir/dir") == TILEDB_UT_ERR);
  CHECK(create_file(fs, "/non-existent-dir/file", 0, 0) == TILEDB_UT_ERR);
  CHECK(write_to_file(fs, "/non-existent-dir/file", "Hello", 6) == TILEDB_UT_ERR);
  char check_str[6];
  CHECK(read_from_file(fs, "/non-existent-dir/file", 0, &check_str[0], 6) == TILEDB_UT_ERR);
  CHECK(!sync_path(fs, "/non-existent-dir/file")); // This is OK for an non-existent path
  CHECK(delete_file(fs, "/non-existent-dir/file") == TILEDB_UT_ERR);
  CHECK(delete_dir(fs, "/non-existent-dir/dir") == TILEDB_UT_ERR);
  CHECK(move_path(fs, "/non-existent-dir/old", "/non-existent-dir/new") == TILEDB_UT_ERR);

  CHECK(!create_dir(fs, temp_dir));
  CHECK(is_dir(fs, temp_dir));

  const std::string path = temp_dir+"/foo";
  CHECK(!create_file(fs, path,  O_WRONLY|O_CREAT,  S_IRWXU));
  CHECK(is_file(fs, path));
  CHECK(!write_to_file(fs, path, "Hello", 6));
  CHECK(!sync_path(fs, path));
  CHECK(!read_from_file(fs, path, 0, &check_str[0], 6));
  CHECK(file_size(fs, path) == 6);
  CHECK(strcmp(&check_str[0], "Hello") == 0);

  const std::string new_path = temp_dir+"/new";
  CHECK(!move_path(fs, path, new_path));
  CHECK(is_file(fs, new_path));
  CHECK(file_size(fs, new_path) == 6);
  CHECK(!delete_file(fs, new_path));
}

TEST_CASE("Test empty value concept", "[empty_cell_val]") {
  char char_max = get_tiledb_empty_value<char>();
  CHECK(char_max == CHAR_MAX);

  int8_t int8_max =  get_tiledb_empty_value<int8_t>();
  CHECK(int8_max == INT8_MAX);
  int16_t int16_max =  get_tiledb_empty_value<int16_t>();
  CHECK(int16_max == INT16_MAX);
  int32_t int32_max =  get_tiledb_empty_value<int32_t>();
  CHECK(int32_max == INT32_MAX);
  int64_t int64_max =  get_tiledb_empty_value<int64_t>();
  CHECK(int64_max == INT64_MAX);

  uint64_t uint8_max =  get_tiledb_empty_value<uint8_t>();
  CHECK(uint8_max == UINT8_MAX);
  uint16_t uint16_max =  get_tiledb_empty_value<uint16_t>();
  CHECK(uint16_max == UINT16_MAX);
  uint32_t uint32_max =  get_tiledb_empty_value<uint32_t>();
  CHECK(uint32_max == UINT32_MAX);
  uint64_t uint64_max =  get_tiledb_empty_value<uint64_t>();
  CHECK(uint64_max == UINT64_MAX);

  float float_max =  get_tiledb_empty_value<float>();
  CHECK(float_max == FLT_MAX);
  double double_max =  get_tiledb_empty_value<double>();
  CHECK(double_max == DBL_MAX);

}

TEST_CASE("Test storage URIs", "[storage_uris]") {
  CHECK(!is_supported_cloud_path("gibberish://ddd/d"));

  CHECK(is_supported_cloud_path("hdfs://ddd/d"));

  CHECK(is_supported_cloud_path("s3://ddd/d"));

  CHECK(is_supported_cloud_path("gs://ddd/d"));

  CHECK(is_supported_cloud_path("wasb://ddd/d"));
  CHECK(is_supported_cloud_path("wasbs://ddd/d"));
  CHECK(is_supported_cloud_path("abfs://ddd/d"));
  CHECK(is_supported_cloud_path("abfss://ddd/d"));
  CHECK(is_supported_cloud_path("adl://ddd/d"));
}
