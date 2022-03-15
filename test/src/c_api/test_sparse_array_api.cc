/**
 * @file   c_api_sparse_array_spec.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 * @copyright Copyright (c) 2018-2020, 2022 Omics Data Automation, Inc.
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
 * Tests of C API for sparse array operations.
 */

#include "catch.h"

#include "c_api_sparse_array_spec.h"
#include "progress_bar.h"
#include "storage_manager.h"
#include "storage_posixfs.h"
#include "utils.h"

#include <cstring>
#include <iostream>
#include <map>
#include <time.h>
#include <sys/time.h>
#include <sstream>


template <class T>
class ArrayTestFixture1D : TempDir {

 public:
  const std::string WORKSPACE = get_temp_dir() + "/array_test_fixture_ws_1D/";
  std::string array_name_;
  TileDB_ArraySchema array_schema_;
  TileDB_CTX* tiledb_ctx_;

  std::shared_ptr<std::vector<T>> buffer_;
  size_t num_elements_ = 10;
  
  std::shared_ptr<std::vector<int32_t>> buffer_coords_;
  size_t buffer_coords_size_;

  std::vector<void *> buffers_;
  std::vector<size_t> buffer_sizes_;
  
  ArrayTestFixture1D() {
    CHECK_RC(tiledb_ctx_init(&tiledb_ctx_, NULL), TILEDB_OK);
    CHECK_RC(tiledb_workspace_create(tiledb_ctx_, WORKSPACE.c_str()), TILEDB_OK);
  }

  ~ArrayTestFixture1D() {
    // Finalize TileDB context
    int rc = tiledb_ctx_finalize(tiledb_ctx_);
    CHECK_RC(rc, TILEDB_OK);
  }

  // Create a buffer to store values of given Type
  // Returns a buffer and size to calling client
  void create_buffer(size_t size, T** buffer, size_t* bytes) {
    buffer_ = std::make_shared<std::vector<T>>(size);

    // Set some arbitrary values
    for(auto i = 0u; i < size; ++i) {
      buffer_.get()->data()[i] = (T)i;
    }
    
    *bytes = (buffer_.get()->size())*sizeof(T);
    *buffer = &buffer_.get()->data()[0];
  }

  void clear_buffer() {
    for(auto i = 0u; i < buffer_.get()->size(); i++) {
      buffer_.get()->data()[i] = (T)0;
    }
    for(auto i = 0u; i < buffer_coords_.get()->size(); i++) {
      buffer_coords_.get()->data()[i] = 0;
    }
  }
  
  void validate_buffer() {
    for(auto i = 0u; i < buffer_coords_.get()->size(); i++) {
      CHECK(buffer_.get()->data()[i] == (T)(buffer_coords_.get()->data()[i]));
    }
  }
  
  int create_array(const int32_t tile_extent, const int32_t domain_lo, const int32_t domain_hi,
                   const int cell_order, const int tile_order) {
    // Error code
    int rc;

    int attribute_type = TILEDB_CHAR;
    if (typeid(T) == typeid(uint8_t)) {
      attribute_type = TILEDB_UINT8;
    } else if (typeid(T) == typeid(uint16_t)) {
      attribute_type = TILEDB_UINT16;
    } else if (typeid(T) == typeid(uint32_t)) {
      attribute_type = TILEDB_UINT32;
    } else if (typeid(T) == typeid(uint64_t)) {
      attribute_type = TILEDB_UINT64;
    } else if (typeid(T) == typeid(int8_t)) {
      attribute_type = TILEDB_INT8;
    } else if (typeid(T) == typeid(int16_t)) {
      attribute_type = TILEDB_INT16;
    } else if (typeid(T) == typeid(int) || typeid(T) == typeid(int32_t)) {
      attribute_type = TILEDB_INT32;
    } else if (typeid(T) == typeid(int64_t)) {
      attribute_type = TILEDB_INT64;
    } else if (typeid(T) == typeid(float)) {
      attribute_type = TILEDB_FLOAT32;
    } else if (typeid(T) == typeid(double)) {
      attribute_type = TILEDB_FLOAT64;
    }

    // Setup array
    const int attribute_num = 1;
    const char* attributes[] = { "MY_ATTRIBUTE" };
    const char* dimensions[] = { "X"};
    int32_t domain[] = { domain_lo, domain_hi };
    int32_t tile_extents[] = { tile_extent };
    const int types[] = { attribute_type, TILEDB_INT32 };
    int compression[] = { TILEDB_NO_COMPRESSION, TILEDB_NO_COMPRESSION};
    int compression_level[] = { 0, 0 };
    
    // Set the array schema
    rc = tiledb_array_set_schema(
        &array_schema_,
        array_name_.c_str(),
        attributes,
        attribute_num,
        0,
        cell_order,
        NULL,
        compression,
        compression_level,
        NULL, // Offsets compression
        NULL, // Offsets compression level
        0, // Sparse
        dimensions,
        1,
        domain,
        2*sizeof(int32_t),
        tile_extents,
        sizeof(int32_t),
        tile_order,
        types);
    CHECK_RC(rc, TILEDB_OK);
    
    // Create the array
    rc = tiledb_array_create(tiledb_ctx_, &array_schema_);
    CHECK_RC(rc, TILEDB_OK);

    // Free array schema
    rc = tiledb_array_free_schema(&array_schema_);
    CHECK_RC(rc, TILEDB_OK);

    buffer_coords_ = std::make_shared<std::vector<int32_t>>(num_elements_);
    for(auto i = 0u; i < num_elements_; ++i) {
      buffer_coords_.get()->data()[i] = (int32_t)i;
    }
    size_t buffer_coords_size = num_elements_ * sizeof(int32_t);

    T *buffer = nullptr;
    size_t buffer_bytes = 0;
    create_buffer(num_elements_, &buffer, &buffer_bytes);
    CHECK(buffer != nullptr);
  
    buffers_.push_back(buffer);
    buffer_sizes_.push_back(buffer_bytes);
    
    buffers_.push_back(&buffer_coords_.get()->data()[0]);
    buffer_sizes_.push_back(buffer_coords_size);
    
    return TILEDB_OK;
  }

  int write_array() {
    // Intialize array
    TileDB_Array* tiledb_array;
    int rc = tiledb_array_init(
        tiledb_ctx_,
        &tiledb_array,
        array_name_.c_str(),
        TILEDB_ARRAY_WRITE,
        NULL,
        NULL,
        0);   
    CHECK_RC(rc, TILEDB_OK);
    
    // Write array
    rc = tiledb_array_write(tiledb_array, const_cast<const void **>(buffers_.data()), buffer_sizes_.data());
    CHECK_RC(rc, TILEDB_OK);
   
    // Finalize the array
    rc = tiledb_array_finalize(tiledb_array);
    CHECK_RC(rc, TILEDB_OK);

    return TILEDB_OK;
  }

  int read_array() {
    clear_buffer();
    
    // Initialize array
    TileDB_Array* tiledb_array;
    int rc = tiledb_array_init(
        tiledb_ctx_,
        &tiledb_array,
        array_name_.c_str(),
        TILEDB_ARRAY_READ,
        NULL,
        NULL,
        0);   
    CHECK_RC(rc, TILEDB_OK);

    // Read array
    rc = tiledb_array_read(tiledb_array, buffers_.data(), buffer_sizes_.data());
    CHECK_RC(rc, TILEDB_OK);

    CHECK(buffer_sizes_.data()[0] == num_elements_*sizeof(T));
    CHECK(buffer_sizes_.data()[1] == num_elements_*sizeof(int32_t));

    // Check no overflow
    // CHECK_RC(tiledb_array_overflow(tiledb_array, 0), TILEDB_OK);
    
    // Finalize the array
    rc = tiledb_array_finalize(tiledb_array);
    CHECK_RC(rc, TILEDB_OK);

    validate_buffer();
    return TILEDB_OK;
  }

  int consolidate_array(size_t segment_size=0) {
    // Consolidate array
    int rc;
    if (segment_size) {
      rc = tiledb_array_consolidate(tiledb_ctx_, array_name_.c_str(), segment_size);
    } else {
      rc = tiledb_array_consolidate(tiledb_ctx_, array_name_.c_str());
    }
    CHECK_RC(rc, TILEDB_OK);
    
    return TILEDB_OK;
  }

  void set_array_name(const char *name) {
    array_name_ = WORKSPACE + name;
  }
};

#define AF ArrayTestFixture1D<TestType>
 
TEMPLATE_TEST_CASE_METHOD(ArrayTestFixture1D, "Test sparse write with attribute types 1", "[test_new]", 
                          char,  uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double) {
  SECTION("row_major") {
    AF::set_array_name("sparse_test_1D_row");
    CHECK_RC(AF::create_array(5, 0, 99, TILEDB_ROW_MAJOR/*cell_order*/, TILEDB_ROW_MAJOR/*tile_order*/), TILEDB_OK);
    CHECK_RC(AF::write_array(), TILEDB_OK);
    CHECK_RC(AF::read_array(), TILEDB_OK);
    CHECK_RC(AF::write_array(), TILEDB_OK);
    CHECK_RC(AF::read_array(), TILEDB_OK);
    CHECK_RC(AF::consolidate_array(), TILEDB_OK);
    CHECK_RC(AF::read_array(), TILEDB_OK);
    CHECK_RC(AF::write_array(), TILEDB_OK);
    CHECK_RC(AF::write_array(), TILEDB_OK);
    CHECK_RC(AF::consolidate_array(200), TILEDB_OK);
    CHECK_RC(AF::read_array(), TILEDB_OK);
  }
  SECTION("col_major") {
    AF::set_array_name("sparse_test_1D_col");
    CHECK_RC(AF::create_array(10, 0, 990, TILEDB_COL_MAJOR, TILEDB_COL_MAJOR), TILEDB_OK);
    CHECK_RC(AF::write_array(), TILEDB_OK);
    CHECK_RC(AF::read_array(), TILEDB_OK);
  }
  SECTION("col_mixed") {
    AF::set_array_name("sparse_test_1D_mixed");
    CHECK_RC(AF::create_array(100, 0, 990, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR), TILEDB_OK);
    CHECK_RC(AF::write_array(), TILEDB_OK);
    CHECK_RC(AF::read_array(), TILEDB_OK);
  }
  SECTION("col_mixed_1") {
    AF::set_array_name("sparse_test_1D_mixed_1");
    CHECK_RC(AF::create_array(200, 0, 990, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR), TILEDB_OK);
    CHECK_RC(AF::write_array(), TILEDB_OK);
    CHECK_RC(AF::read_array(), TILEDB_OK);
  } 
}

SparseArrayTestFixture::SparseArrayTestFixture() {
  // Error code
  int rc;
 
  // Initialize context
  rc = tiledb_ctx_init(&tiledb_ctx_, NULL);
  CHECK_RC(rc, TILEDB_OK);

  // Create workspace
  rc = tiledb_workspace_create(tiledb_ctx_, WORKSPACE.c_str());
  CHECK_RC(rc, TILEDB_OK);
}

SparseArrayTestFixture::~SparseArrayTestFixture() {
  // Error code
  int rc;

  // Finalize TileDB context
  rc = tiledb_ctx_finalize(tiledb_ctx_);
  CHECK_RC(rc, TILEDB_OK);
}


/* ****************************** */
/*          PUBLIC METHODS        */
/* ****************************** */

int SparseArrayTestFixture::create_sparse_array_2D(
    const int64_t tile_extent_0,
    const int64_t tile_extent_1,
    const int64_t domain_0_lo,
    const int64_t domain_0_hi,
    const int64_t domain_1_lo,
    const int64_t domain_1_hi,
    const int64_t capacity,
    const bool enable_compression,
    const int cell_order,
    const int tile_order) {
  // Error code
  int rc;

  // Prepare and set the array schema object and data structures
  const int attribute_num = 1;
  const char* attributes[] = { "ATTR_INT32" };
  const char* dimensions[] = { "X", "Y" };
  int64_t domain[] = { domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi };
  int64_t tile_extents[] = { tile_extent_0, tile_extent_1 };
  const int types[] = { TILEDB_INT32, TILEDB_INT64 };
  int compression[2];
  const int dense = 0;

  if(!enable_compression) {
    compression[0] = TILEDB_NO_COMPRESSION;
    compression[1] = TILEDB_NO_COMPRESSION;
  } else {
    compression[0] = TILEDB_GZIP;
    compression[1] = TILEDB_GZIP;
  }

  // Set the array schema
  rc = tiledb_array_set_schema(
           &array_schema_,
           array_name_.c_str(),
           attributes,
           attribute_num,
           capacity,
           cell_order,
           NULL,
           compression,
	   NULL,
           NULL, // Offsets compression
           NULL, // Offsets compression level
           dense,
           dimensions,
           2,
           domain,
           4*sizeof(int64_t),
           tile_extents,
           2*sizeof(int64_t),
           tile_order,
           types);
  if(rc != TILEDB_OK)
    return TILEDB_ERR;

  // Create the array
  rc = tiledb_array_create(tiledb_ctx_, &array_schema_);
  if(rc != TILEDB_OK)
    return TILEDB_ERR;

  // Free array schema
  rc = tiledb_array_free_schema(&array_schema_);
  if(rc != TILEDB_OK)
    return TILEDB_ERR;
  
  // Success
  return TILEDB_OK;
}

int* SparseArrayTestFixture::read_sparse_array_2D(
    const int64_t domain_0_lo,
    const int64_t domain_0_hi,
    const int64_t domain_1_lo,
    const int64_t domain_1_hi,
    const int read_mode) {
  // Error code
  int rc;

  // Initialize a subarray
  const int64_t subarray[] = {
      domain_0_lo, 
      domain_0_hi, 
      domain_1_lo, 
      domain_1_hi
  };

  // Subset over a specific attribute
  const char* attributes[] = { "ATTR_INT32" };

  // Initialize the array in the input mode
  TileDB_Array* tiledb_array;
  rc = tiledb_array_init(
           tiledb_ctx_,
           &tiledb_array,
           array_name_.c_str(),
           read_mode,
           subarray,
           attributes,
           1);
  if(rc != TILEDB_OK) 
    return NULL;

  // Prepare the buffers that will store the result
  int64_t domain_size_0 = domain_0_hi - domain_0_lo + 1;
  int64_t domain_size_1 = domain_1_hi - domain_1_lo + 1;
  int64_t cell_num = domain_size_0 * domain_size_1;
  int* buffer_a1 = new int[cell_num];
  assert(buffer_a1);
  void* buffers[] = { buffer_a1 };
  size_t buffer_size_a1 = cell_num * sizeof(int);
  size_t buffer_sizes[] = { buffer_size_a1 };

  // Read from array
  rc = tiledb_array_read(tiledb_array, buffers, buffer_sizes);
  if(rc != TILEDB_OK) {
    tiledb_array_finalize(tiledb_array);
    return NULL;
  }

  // Finalize the array
  rc = tiledb_array_finalize(tiledb_array);
  if(rc != TILEDB_OK) 
    return NULL;

  // Success - return the created buffer
  return buffer_a1;
}

void SparseArrayTestFixture::set_array_name(const char *name) {
  array_name_ = WORKSPACE + name;
}

int SparseArrayTestFixture::write_sparse_array_unsorted_2D(
    const int64_t domain_size_0,
    const int64_t domain_size_1) {
  // Error code
  int rc;

  // Generate random attribute values and coordinates for sparse write
  int64_t cell_num = domain_size_0*domain_size_1;
  int* buffer_a1 = new int[cell_num];
  int64_t* buffer_coords = new int64_t[2*cell_num];
  int64_t coords_index = 0L;
  for (int64_t i = 0; i < domain_size_0; ++i) {
    for (int64_t j = 0; j < domain_size_1; ++j) {
      buffer_a1[i*domain_size_1+j] = i*domain_size_1+j;
      buffer_coords[2*coords_index] = i;
      buffer_coords[2*coords_index+1] = j;
      coords_index++;
    }
  }

  // Initialize the array 
  TileDB_Array* tiledb_array;
  rc = tiledb_array_init(
           tiledb_ctx_,
           &tiledb_array,
           array_name_.c_str(),
           TILEDB_ARRAY_WRITE_UNSORTED,
           NULL,
           NULL,
           0);  
  if(rc != TILEDB_OK)
    return TILEDB_ERR;

  // Write to array
  const void* buffers[] = { buffer_a1, buffer_coords };
  size_t buffer_sizes[2];
  buffer_sizes[0] = cell_num*sizeof(int);
  buffer_sizes[1] = 2*cell_num*sizeof(int64_t);
  rc = tiledb_array_write(tiledb_array, buffers, buffer_sizes);
  if(rc != TILEDB_OK)
    return TILEDB_ERR;

  // Finalize the array
  rc = tiledb_array_finalize(tiledb_array);
  if(rc != TILEDB_OK)
    return TILEDB_ERR;

  // Clean up
  delete [] buffer_a1;
  delete [] buffer_coords;

  // Success
  return TILEDB_OK;
} 

/**
 * Test is to randomly read subregions of the array and
 * check with corresponding value set by row_id*dim1+col_id
 * Top left corner is always 4,4
 * Test runs through 10 iterations to choose random
 * width and height of the subregions
 */
TEST_CASE_METHOD(SparseArrayTestFixture, "Test random read subregions", "[test_random_sparse_sorted_reads]") {
  // Error code
  int rc;

  // Parameters used in this test
  int64_t domain_size_0 = 5000;
  int64_t domain_size_1 = 1000;
  int64_t tile_extent_0 = 100;
  int64_t tile_extent_1 = 100;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0-1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1-1;
  int64_t capacity = 0; // 0 means use default capacity
  int cell_order = TILEDB_ROW_MAJOR;
  int tile_order = TILEDB_ROW_MAJOR;
  int iter_num = 10;

  // Set array name
  set_array_name("sparse_test_5000x1000_100x100");

  // Create a progress bar
  ProgressBar* progress_bar = new ProgressBar();

  // Create a dense integer array
  rc = create_sparse_array_2D(
           tile_extent_0,
           tile_extent_1,
           domain_0_lo,
           domain_0_hi,
           domain_1_lo,
           domain_1_hi,
           capacity,
           false,
           cell_order,
           tile_order);
  CHECK_RC(rc, TILEDB_OK);

  // Write array cells with value = row id * COLUMNS + col id
  // to disk
  rc = write_sparse_array_unsorted_2D(domain_size_0, domain_size_1);
  CHECK_RC(rc, TILEDB_OK);

  // Test random subarrays and check with corresponding value set by
  // row_id*dim1+col_id. Top left corner is always 4,4.
  int64_t d0_lo = 4;
  int64_t d0_hi = 0;
  int64_t d1_lo = 4;
  int64_t d1_hi = 0;
  int64_t height = 0, width = 0;

  for(int iter = 0; iter < iter_num; ++iter) {
    height = rand() % (domain_size_0 - d0_lo);
    width = rand() % (domain_size_1 - d1_lo);
    d0_hi = d0_lo + height;
    d1_hi = d1_lo + width;
    int64_t index = 0;

    // Read subarray
    int *buffer = read_sparse_array_2D(
                      d0_lo,
                      d0_hi,
                      d1_lo,
                      d1_hi,
                      TILEDB_ARRAY_READ_SORTED_ROW);
    REQUIRE(buffer != NULL);

    // Check
    for(int64_t i = d0_lo; i <= d0_hi; ++i) {
      for(int64_t j = d1_lo; j <= d1_hi; ++j) {
        CHECK(buffer[index] == i*domain_size_1+j);
        if (buffer[index] !=  (i*domain_size_1+j)) {
          std::cout << "mismatch: " << i
              << "," << j << "=" << buffer[index] << "!="
              << ((i*domain_size_1+j)) << "\n";
          return;
        }
        ++index;
      }
    }

    // Clean up
    delete [] buffer;

    // Update progress bar
    progress_bar->load(1.0/iter_num);
  } 

  // Delete progress bar
  delete progress_bar;
}

class SparseArrayEnvTestFixture : SparseArrayTestFixture {
  public:
  SparseArrayTestFixture *test_fixture;

  SparseArrayEnvTestFixture() {
    test_fixture = NULL;
  }

  ~SparseArrayEnvTestFixture() {
    delete test_fixture;
  }

  void set_disable_file_locking() {
    CHECK(setenv("TILEDB_DISABLE_FILE_LOCKING", "1", 1) == 0);
    CHECK(is_env_set("TILEDB_DISABLE_FILE_LOCKING"));

    PosixFS fs;
    REQUIRE(!fs.locking_support());
  }

  void unset_disable_file_locking() {
    unsetenv("TILEDB_DISABLE_FILE_LOCKING");
    CHECK(!is_env_set("TILEDB_DISABLE_FILE_LOCKING"));
    PosixFS fs;
    REQUIRE(fs.locking_support());
  }

  void set_keep_write_file_handles_open() {
    CHECK(setenv("TILEDB_KEEP_FILE_HANDLES_OPEN", "1", 1) == 0);
    CHECK(is_env_set("TILEDB_KEEP_FILE_HANDLES_OPEN"));
    
    PosixFS fs;
    REQUIRE(fs.keep_write_file_handles_open());
  }

  void unset_keep_write_file_handles_open() {
    unsetenv("TILEDB_KEEP_FILE_HANDLES_OPEN");
    CHECK(!is_env_set("KEEP_FILE_HANDLES_OPEN"));
    PosixFS fs;
    REQUIRE(!fs.keep_write_file_handles_open());
  }

  int write_array() {
    // Set array name
    set_array_name("sparse_test_disable_file_locking_env");
    CHECK_RC(create_sparse_array_2D(4, 4, 0, 15, 0, 15, 0, false, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR), TILEDB_OK);

    // Write array cells with value = row id * COLUMNS + col id
    // to disk
    CHECK_RC(write_sparse_array_unsorted_2D(16, 16), TILEDB_OK);

    return 0;
  }

  int read_array() {
    CHECK(read_sparse_array_2D(4, 0, 4, 0, TILEDB_ARRAY_READ) != NULL);
    return 0;
  }

  bool consolidation_file_lock_exists() {
    PosixFS fs;
    return fs.is_file(array_name_+"/"+TILEDB_SM_CONSOLIDATION_FILELOCK_NAME);
  }
};

TEST_CASE_METHOD(SparseArrayEnvTestFixture, "Test reading/writing with env unset", "[test_sparse_read_with_env_unset]") {
  unset_disable_file_locking();
  unset_keep_write_file_handles_open();
  write_array();
  CHECK(consolidation_file_lock_exists());

  // TILEDB_DISABLE_FILE_LOCK unset
  unset_disable_file_locking();
  read_array();

  // TILEDB_DISABLE_FILE_LOCK=1
  set_disable_file_locking();
  read_array();
}

TEST_CASE_METHOD(SparseArrayEnvTestFixture, "Test reading/writing with env set", "[test_sparse_read_with_env_set]") {
  set_disable_file_locking();
  set_keep_write_file_handles_open();
  write_array();
  CHECK(consolidation_file_lock_exists());

  // TILEDB_DISABLE_FILE_LOCK unset
  unset_disable_file_locking();
  read_array();

  // TILEDB_DISABLE_FILE_LOCK=1
  set_disable_file_locking();
  read_array();
}
