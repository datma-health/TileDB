/**
 * @file   test_gcs_storage.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 Omics Data Automation, Inc.
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
 * Tests for the GCS storage class
 */

#include "catch.h"
#include "storage_gcs.h"
#include "uri.h"
#include "utils.h"

#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>


class GCSTestFixture {
 protected:
  TempDir *temp_dir = NULL;
  GCS *gcs_instance = NULL;
  
  GCSTestFixture() {
    if (is_gcs_path(get_test_dir())) {
      try {
        temp_dir = new TempDir();
        std::string home_dir = temp_dir->get_temp_dir()+"/test_gcs";
        gcs_instance = new GCS(home_dir);
        CHECK(!gcs_instance->locking_support());
      } catch(...) {
        INFO("GCS Storage could not be credentialed");
      }
    }
    INFO("GCS Storage not specified as a --test-dir option");
  }

  ~GCSTestFixture() {
    if (gcs_instance) {
      delete gcs_instance;
    }
    if (temp_dir) {
      delete temp_dir;
    }
  }
};


TEST_CASE("Test GCS constructor", "[constr]") {
  CHECK_THROWS(new GCS("gcs://my_container/path"));
  CHECK_THROWS(new GCS("az://my_container@my_account.blob.core.windows.net/path"));
  CHECK_THROWS(new GCS("gs://my_container/path"));
}


TEST_CASE_METHOD(GCSTestFixture, "Test GCS cwd", "[cwd]") {
  if (gcs_instance == nullptr) {
    return;
  }
  REQUIRE(gcs_instance->current_dir().length() > 0);
  REQUIRE(gcs_instance->create_dir(gcs_instance->current_dir()) == TILEDB_FS_OK);
  REQUIRE(gcs_instance->is_dir(gcs_instance->current_dir()));
  REQUIRE(!gcs_instance->is_file(gcs_instance->current_dir()));
  REQUIRE(gcs_instance->real_dir(gcs_instance->current_dir()).length() > 0);
}


TEST_CASE_METHOD(GCSTestFixture, "Test GCS real_dir", "[real_dir]") {
  if (gcs_instance == nullptr) {
    return;
  }
  CHECK(gcs_instance->real_dir("").compare("") == 0);
  CHECK(gcs_instance->real_dir("xxx").compare("xxx") == 0);
  CHECK(gcs_instance->real_dir("xxx/yyy").compare("xxx/yyy") == 0);
  CHECK(gcs_instance->real_dir("/xxx/yyy").compare("/xxx/yyy") == 0);
  gcs_uri test_uri(get_test_dir());
  auto real_path = gcs_instance->real_dir(get_test_dir());
  CHECK(real_path.compare(test_uri.path()) == 0);
  CHECK(real_path.find("//") == std::npos);
  CHECK_THROWS(gcs_instance->real_dir("xxx://yyy"));
}

TEST_CASE_METHOD(GCSTestFixture, "Test GCS dir", "[dir]") {
  if (gcs_instance == nullptr) {
    return;
  }
  CHECK(!gcs_instance->is_dir("non-existent-dir"));
  CHECK(!gcs_instance->is_dir("non-existent-parent-dir/dir"));
      
  std::string test_dir("dir");
  CHECK_RC(gcs_instance->create_dir(test_dir), TILEDB_FS_OK);
  CHECK(gcs_instance->is_dir(test_dir));
  CHECK(!gcs_instance->is_file(test_dir));
  CHECK_RC(gcs_instance->create_dir(test_dir), TILEDB_FS_ERR); // Dir already exists
  CHECK_RC(gcs_instance->create_file(test_dir, 0, 0), TILEDB_FS_ERR);
  CHECK_RC(gcs_instance->create_file(gcs_instance->real_dir(test_dir), 0, 0), TILEDB_FS_ERR);
  CHECK(gcs_instance->file_size(test_dir) == TILEDB_FS_ERR);
  CHECK(gcs_instance->get_dirs(test_dir).size() == 0);
  CHECK(gcs_instance->get_files(test_dir).size() == 0);
  CHECK(gcs_instance->get_dirs("non-existent-dir").size() == 0);
  CHECK(gcs_instance->get_files("non-existent-dir").size() == 0);

  // Try directories with trailing slash
  std::string test_dir1 = test_dir + "1/";
  CHECK_RC(gcs_instance->create_dir(test_dir1), TILEDB_FS_OK);
  CHECK(gcs_instance->is_dir(test_dir1));
  CHECK(gcs_instance->is_dir(test_dir + "1"));
  CHECK(!gcs_instance->is_file(test_dir1));
  CHECK(!gcs_instance->is_file(test_dir1 + "1"));

  // TBD: move_path
  std::string new_dir = test_dir+"-new";
  CHECK_THROWS(gcs_instance->move_path(test_dir, new_dir));

  CHECK_RC(gcs_instance->sync_path(test_dir), TILEDB_FS_OK);
  // No support for returning errors for non-existent paths
  CHECK_RC(gcs_instance->sync_path("non-existent-dir"), TILEDB_FS_OK);

  CHECK_RC(gcs_instance->delete_dir(test_dir), TILEDB_FS_OK);
  CHECK_RC(gcs_instance->delete_dir("non-existent-dir"), TILEDB_FS_ERR);
  CHECK(!gcs_instance->is_dir(test_dir));

  CHECK(!gcs_instance->is_file(test_dir));
}


TEST_CASE_METHOD(GCSTestFixture, "Test GCS file", "[file]") {
  if (gcs_instance == nullptr) {
    return;
  }
  std::string test_dir("file");
  CHECK_RC(gcs_instance->create_dir(test_dir), 0);
  REQUIRE(gcs_instance->is_dir(test_dir));
  CHECK_RC(gcs_instance->create_file(test_dir+"/foo", O_WRONLY|O_CREAT,  S_IRWXU), TILEDB_FS_OK);
  CHECK(gcs_instance->is_file(test_dir+"/foo"));
  CHECK(!gcs_instance->is_dir(test_dir+"/foo"));
  CHECK(gcs_instance->file_size(test_dir+"/foo") == 0);
  CHECK(gcs_instance->file_size(test_dir+"/foo1") == TILEDB_FS_ERR);

  CHECK(gcs_instance->get_files(test_dir).size() == 1);
  CHECK(gcs_instance->get_files("non-existent-dir").size() == 0);
  
  CHECK_RC(gcs_instance->create_file(test_dir+"/foo1", O_WRONLY|O_CREAT,  S_IRWXU), TILEDB_FS_OK);
  CHECK_RC(gcs_instance->create_dir(test_dir+"/subdir"), TILEDB_FS_OK);
  CHECK_RC(gcs_instance->create_file(test_dir+"/subdir/subfoo", O_WRONLY|O_CREAT,  S_IRWXU), TILEDB_FS_OK);
  CHECK(gcs_instance->get_files(test_dir).size() == 2);

  CHECK_RC(gcs_instance->sync_path(test_dir+"/foo"), TILEDB_FS_OK);
  CHECK_RC(gcs_instance->sync_path(test_dir), TILEDB_FS_OK);

  CHECK_RC(gcs_instance->delete_file(test_dir+"/foo"), TILEDB_FS_OK);
  CHECK_RC(gcs_instance->delete_file(test_dir+"/foo1"), TILEDB_FS_OK);
  CHECK_RC(gcs_instance->delete_file(test_dir+"/foo2"), TILEDB_FS_ERR);

  CHECK(!gcs_instance->is_file(test_dir+"/foo1"));
  CHECK(!gcs_instance->is_file(test_dir+"/foo2"));
  CHECK(!gcs_instance->is_dir(test_dir+"/foo1"));
  CHECK(!gcs_instance->is_dir(test_dir+"/foo2"));
}


TEST_CASE_METHOD(GCSTestFixture, "Test GCS read/write file", "[read-write]") {
  if (gcs_instance == nullptr) {
    return;
  }
  std::string test_dir("read_write");
  CHECK_RC(gcs_instance->create_dir(test_dir), TILEDB_FS_OK);
  REQUIRE(gcs_instance->is_dir(test_dir));
  CHECK_RC(gcs_instance->write_to_file(test_dir+"/foo", "hello", 5), TILEDB_FS_OK);

  CHECK_RC(gcs_instance->sync_path(test_dir+"/foo"), TILEDB_FS_OK);
  CHECK_RC(gcs_instance->close_file(test_dir+"/foo"), TILEDB_FS_OK);
  auto files = gcs_instance->get_files(test_dir);
  for(auto f: files) {
    std::cerr << "file=" << f << std::endl;
  }
  REQUIRE(gcs_instance->is_file(test_dir+"/foo"));
  CHECK(gcs_instance->file_size(test_dir+"/foo") == 5);

  void *buffer = malloc(20);
  memset(buffer, 'X', 20);
  CHECK_RC(gcs_instance->read_from_file(test_dir+"/foo", 0, buffer, 0), TILEDB_FS_OK);
  CHECK_RC(gcs_instance->read_from_file(test_dir+"/foo", 0, buffer, 2), TILEDB_FS_OK);
  CHECK(((char *)buffer)[0] == 'h');
  CHECK(((char *)buffer)[1] == 'e');
  CHECK_RC(gcs_instance->read_from_file(test_dir+"/foo", 0, buffer, 5), TILEDB_FS_OK);
  CHECK(((char *)buffer)[4] == 'o');
  CHECK_RC(gcs_instance->read_from_file(test_dir+"/foo", 0, buffer, 6), TILEDB_FS_ERR);
  CHECK_RC(gcs_instance->close_file(test_dir+"/foo"), TILEDB_FS_OK);

  memset(buffer, 'X', 20);
  CHECK_RC(gcs_instance->write_to_file(test_dir+"/foo1", buffer, 20), TILEDB_FS_OK);
  CHECK_RC(gcs_instance->close_file(test_dir+"/foo1"), TILEDB_FS_OK);
  REQUIRE(gcs_instance->is_file(test_dir+"/foo1"));
  for (int i=0; i<4; i++) {
    void *buf = malloc(5);
    memset(buf, 'Z', 5);
    CHECK_RC(gcs_instance->read_from_file(test_dir+"/foo1", i*5, buf, 5), TILEDB_FS_OK);
    for (int j=0; j<5; j++) {
      CHECK(((char *)buf)[j] == 'X');
    }
    free(buf);
  }
  
  // This part is different from Azure Blob Storage where you are allowed to write as many chunks in any size
  // No overwriting allowed with GCS, so only write-once semantics allowed after a file is committed
  CHECK_RC(gcs_instance->write_to_file(test_dir+"/foo2", "hello", 5), TILEDB_FS_OK);
  CHECK_RC(gcs_instance->write_to_file(test_dir+"/foo2", " there ", 6), TILEDB_FS_ERR); // Prev part should at least be 5M, otherwise should be aborted
  CHECK_RC(gcs_instance->close_file(test_dir+"/foo2"), TILEDB_FS_OK);
  CHECK(gcs_instance->file_size(test_dir+"/foo2") == 5); // Previous file size as the current set of writes failed

  CHECK_RC(gcs_instance->read_from_file(test_dir+"/non-existent-file", 0, buffer, 5), TILEDB_FS_ERR);
  CHECK_RC(gcs_instance->read_from_file("non-existent-dir/foo", 0, buffer, 5), TILEDB_FS_ERR);
  CHECK_RC(gcs_instance->close_file("non-existent-dir/foo"), TILEDB_FS_OK);

  free(buffer);
}

TEST_CASE_METHOD(GCSTestFixture, "Test GCS large read/write file", "[read-write-large]") {
  if (gcs_instance == nullptr) {
    return;
  }
  std::string test_dir("read_write_large");
  // size_t size = ((size_t)TILEDB_UT_MAX_WRITE_COUNT)*4
  size_t size = ((size_t)TILEDB_UT_MAX_WRITE_COUNT);
  void *buffer = malloc(size);
  if (buffer) {
    memset(buffer, 'B', size);
    REQUIRE(gcs_instance->create_dir(test_dir) == TILEDB_FS_OK);
    CHECK_RC(gcs_instance->write_to_file(test_dir+"/foo", buffer, size), TILEDB_FS_OK);
    CHECK_RC(gcs_instance->write_to_file(test_dir+"/foo", buffer, size), TILEDB_FS_OK);
    CHECK_RC(gcs_instance->close_file(test_dir+"/foo"), TILEDB_FS_OK);
    CHECK(gcs_instance->is_file(test_dir+"/foo"));
    CHECK((size_t)gcs_instance->file_size(test_dir+"/foo") == size*2);

    void *buffer1 = malloc(size);
    if (buffer1) {
      memset(buffer1, 0, size);
      CHECK_RC(gcs_instance->read_from_file(test_dir+"/foo", 0, buffer1, size), TILEDB_FS_OK);

      CHECK(memcmp(buffer, buffer1, size) == 0);

      free(buffer1);
    }
    free(buffer);
  }
}

TEST_CASE_METHOD(GCSTestFixture, "Test GCS operations", "[parallel]") {
  if (gcs_instance == nullptr) {
    return;
  }

  std::string test_dir("parallel");
  REQUIRE(gcs_instance->create_dir(test_dir) == TILEDB_FS_OK);

  bool complete = true;
  uint iterations = 2;

  #pragma omp parallel for
  for (uint i=0; i<iterations; i++) {
    std::string filename = test_dir+"/foo"+std::to_string(i);

    for (auto j=0; j<2; j++) {
      size_t size = 10*1024*1024;
      void *buffer = malloc(size);
      if (buffer) {
	memset(buffer, 'X', size);
	CHECK_RC(gcs_instance->write_to_file(filename, buffer, size), TILEDB_FS_OK);
	free(buffer);
      } else {
	complete = false;
      }
    }
  }

  CHECK_RC(gcs_instance->sync_path(test_dir), TILEDB_FS_OK);

  if (complete) {
    #pragma omp parallel for
    for (uint i=0; i<iterations; i++) {
      std::string filename = test_dir+"/foo"+std::to_string(i);
      CHECK_RC(gcs_instance->close_file(filename), TILEDB_FS_OK);
      CHECK(gcs_instance->is_file(filename));
      CHECK(gcs_instance->file_size(filename) == (size_t)(10*1024*1024*2));
    }
  }

  CHECK_RC(gcs_instance->delete_dir(test_dir), TILEDB_FS_OK);
}
