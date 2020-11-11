/**
 * @file   test_azure_blob_storage.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 Omics Data Automation, Inc.
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
 * Tests for the AzureBlob class
 */

#include "catch.h"
#include "storage_azure_blob.h"
#include "url.h"
#include "utils.h"

#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>


class AzureBlobTestFixture {
 protected:
  TempDir *temp_dir = NULL;
  AzureBlob *azure_blob = NULL;

  AzureBlobTestFixture() {
    if (is_azure_blob_storage_path(get_test_dir())) {
      try {
        temp_dir = new TempDir();
        std::string home_dir = temp_dir->get_temp_dir()+"/test_azure_blob";
        azure_blob = new AzureBlob(home_dir);
        CHECK(!azure_blob->locking_support());
      } catch(...) {
        INFO("Azure Blob Storage could not be credentialed. Set env AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY");
      }
    }
    INFO("Azure Blob Storage not specified as a --test-dir option");
  }

  ~AzureBlobTestFixture() {
    if (azure_blob) {
      delete azure_blob;
    }
    if (temp_dir) {
      delete temp_dir;
    }
  }
};

TEST_CASE_METHOD(AzureBlobTestFixture, "Test AzureBlob constructor", "[constr]") {
  if (azure_blob == nullptr) {
    return;
  }
  CHECK_THROWS(new AzureBlob("wasbs://my_container/path"));
  CHECK_THROWS(new AzureBlob("az://my_container@my_account.blob.core.windows.net/path"));
  CHECK_THROWS(new AzureBlob("az://my_container@blob.core.windows.net/path"));
  CHECK_THROWS(new AzureBlob("az://non-existent-container@blob.core.windows.met/path"));
}

TEST_CASE_METHOD(AzureBlobTestFixture, "Test AzureBlob cwd", "[cwd]") {
  if (azure_blob == nullptr) {
    return;
  }
  REQUIRE(azure_blob->current_dir().length() > 0);
  REQUIRE(azure_blob->create_dir(azure_blob->current_dir()) == TILEDB_FS_OK);
  REQUIRE(azure_blob->is_dir(azure_blob->current_dir()));
  REQUIRE(!azure_blob->is_file(azure_blob->current_dir()));
  REQUIRE(azure_blob->real_dir(azure_blob->current_dir()).length() > 0);
}

TEST_CASE_METHOD(AzureBlobTestFixture, "Test AzureBlob real_dir", "[real_dir]") {
  if (azure_blob == nullptr) {
    return;
  }
  CHECK(azure_blob->real_dir("").compare(azure_blob->current_dir()) == 0);
  CHECK(azure_blob->real_dir("xxx").compare(azure_blob->current_dir()+"/xxx") == 0);
  CHECK(azure_blob->real_dir("xxx/yyy").compare(azure_blob->current_dir()+"/xxx/yyy") == 0);
  CHECK(azure_blob->real_dir("/xxx/yyy").compare("xxx/yyy") == 0);
  azure_url test_url(get_test_dir());
  CHECK(azure_blob->real_dir(get_test_dir()).compare(test_url.path().substr(1)) == 0);
  CHECK_THROWS(azure_blob->real_dir("xxx://yyy"));
}

TEST_CASE_METHOD(AzureBlobTestFixture, "Test AzureBlob dir", "[dir]") {
  if (azure_blob == nullptr) {
    return;
  }
  std::string test_dir("dir");
  CHECK_RC(azure_blob->create_dir(test_dir), TILEDB_FS_OK);
  CHECK(azure_blob->is_dir(test_dir));
  CHECK(!azure_blob->is_file(test_dir));
  CHECK_RC(azure_blob->create_dir(test_dir), TILEDB_FS_ERR); // Dir already exists
  CHECK(!azure_blob->is_dir("parent_foo"));
  CHECK_RC(azure_blob->create_dir("parent_foo/foo"), TILEDB_FS_ERR); 
  CHECK_RC(azure_blob->create_file(test_dir, 0, 0), TILEDB_FS_ERR);
  CHECK_RC(azure_blob->create_file(azure_blob->real_dir(test_dir), 0, 0), TILEDB_FS_ERR);
  CHECK(azure_blob->file_size(test_dir) == TILEDB_FS_ERR);
  CHECK(azure_blob->get_dirs(test_dir).size() == 0);
  CHECK(azure_blob->get_dirs("non-existent-dir").size() == 0);

  // TBD: move_path
  std::string new_dir = test_dir+"-new";
  CHECK_THROWS(azure_blob->move_path(test_dir, new_dir));

  CHECK_RC(azure_blob->sync_path(test_dir), TILEDB_FS_OK);
  // No support for returning errors for non-existent paths
  CHECK_RC(azure_blob->sync_path("non-existent-dir"), TILEDB_FS_OK);

  CHECK_RC(azure_blob->delete_dir(test_dir), TILEDB_FS_OK);
  CHECK_RC(azure_blob->delete_dir("non-existent-dir"), TILEDB_FS_ERR);

  CHECK(!azure_blob->is_dir(test_dir));
  CHECK(!azure_blob->is_file(test_dir));
}

TEST_CASE_METHOD(AzureBlobTestFixture, "Test AzureBlob file", "[file]") {
  if (azure_blob == nullptr) {
    return;
  }
  std::string test_dir("file");
  CHECK_RC(azure_blob->create_dir(test_dir), 0);
  REQUIRE(azure_blob->is_dir(test_dir));
  CHECK_RC(azure_blob->create_file(test_dir+"/foo", O_WRONLY|O_CREAT,  S_IRWXU), TILEDB_FS_OK);
  CHECK(azure_blob->is_file(test_dir+"/foo"));
  CHECK(!azure_blob->is_dir(test_dir+"/foo"));
  CHECK(azure_blob->file_size(test_dir+"/foo") == 0);
  CHECK(azure_blob->file_size(test_dir+"/foo1") == TILEDB_FS_ERR);
  // Add +1 to account for directory marker
  CHECK(azure_blob->get_files(test_dir).size() == 1+1);
  CHECK(azure_blob->get_files("non-existent-dir").size() == 0);
  
  CHECK_RC(azure_blob->create_file(test_dir+"/foo1", O_WRONLY|O_CREAT,  S_IRWXU), TILEDB_FS_OK);
  CHECK(azure_blob->get_files(test_dir).size() == 2+1);

  CHECK_RC(azure_blob->sync_path(test_dir+"/foo"), TILEDB_FS_OK);
  CHECK_RC(azure_blob->sync_path(test_dir), TILEDB_FS_OK);

  CHECK_RC(azure_blob->delete_file(test_dir+"/foo"), TILEDB_FS_OK);
  CHECK_RC(azure_blob->delete_file(test_dir+"/foo1"), TILEDB_FS_OK);
  CHECK_RC(azure_blob->delete_file(test_dir+"/foo2"), TILEDB_FS_ERR);

  CHECK(!azure_blob->is_file(test_dir+"/foo1"));
  CHECK(!azure_blob->is_file(test_dir+"/foo2"));
  CHECK(!azure_blob->is_dir(test_dir+"/foo1"));
  CHECK(!azure_blob->is_dir(test_dir+"/foo2"));
}


TEST_CASE_METHOD(AzureBlobTestFixture, "Test AzureBlob read/write file", "[read-write]") {
  if (azure_blob == nullptr) {
    return;
  }
  std::string test_dir("read_write");
  CHECK_RC(azure_blob->create_dir(test_dir), TILEDB_FS_OK);
  REQUIRE(azure_blob->is_dir(test_dir));
  CHECK_RC(azure_blob->write_to_file(test_dir+"/foo", "hello", 5), TILEDB_FS_OK);
  CHECK_RC(azure_blob->sync_path(test_dir+"/foo"), TILEDB_FS_OK);
  REQUIRE(azure_blob->is_file(test_dir+"/foo"));
  CHECK(azure_blob->file_size(test_dir+"/foo") == 5);

  void *buffer = malloc(20);
  memset(buffer, 'X', 20);
  CHECK_RC(azure_blob->read_from_file(test_dir+"/foo", 0, buffer, 0), TILEDB_FS_OK);
  CHECK_RC(azure_blob->read_from_file(test_dir+"/foo", 0, buffer, 2), TILEDB_FS_OK);
  CHECK(((char *)buffer)[0] == 'h');
  CHECK(((char *)buffer)[1] == 'e');
  CHECK_RC(azure_blob->read_from_file(test_dir+"/foo", 0, buffer, 5), TILEDB_FS_OK);
  CHECK(((char *)buffer)[4] == 'o');
  CHECK_RC(azure_blob->read_from_file(test_dir+"/foo", 0, buffer, 6), TILEDB_FS_ERR);
  CHECK_RC(azure_blob->read_from_file(test_dir+"/foo", 3, buffer, 2), TILEDB_FS_OK);
  CHECK_RC(azure_blob->read_from_file(test_dir+"/foo", 3, buffer, 6), TILEDB_FS_ERR);
  CHECK_RC(azure_blob->read_from_file(test_dir+"/foo", 6, buffer, 2), TILEDB_FS_ERR);

  
  CHECK_RC(azure_blob->write_to_file(test_dir+"/foo", "hello", 5), TILEDB_FS_OK);
  CHECK_RC(azure_blob->write_to_file(test_dir+"/foo", " there ", 6), TILEDB_FS_OK);
  CHECK_RC(azure_blob->close_file(test_dir+"/foo"), TILEDB_FS_OK);
  CHECK(azure_blob->file_size(test_dir+"/foo") == 11);
  CHECK_RC(azure_blob->read_from_file(test_dir+"/foo", 0, buffer, 11), TILEDB_FS_OK);
  CHECK(((char *)buffer)[10] == 'e');

  CHECK_RC(azure_blob->sync_path(test_dir+"/foo"), TILEDB_FS_OK);
  CHECK_RC(azure_blob->delete_file(test_dir+"/foo"), TILEDB_FS_OK);

  CHECK_RC(azure_blob->sync_path(test_dir), TILEDB_FS_OK);

  CHECK_RC(azure_blob->read_from_file(test_dir+"/non-existent-file", 0, buffer, 5), TILEDB_FS_ERR);
  CHECK_RC(azure_blob->read_from_file("non-existent-dir/foo", 0, buffer, 5), TILEDB_FS_ERR);
  CHECK_RC(azure_blob->write_to_file("non-existent-dir/foo", "hello", 5), TILEDB_FS_ERR);
  CHECK_RC(azure_blob->close_file("non-existent-dir/foo"), TILEDB_FS_OK);

  free(buffer);
}


TEST_CASE_METHOD(AzureBlobTestFixture, "Test AzureBlob large read/write file", "[read-write-large]") {
  if (azure_blob == nullptr) {
    return;
  }
  char *travis_build = getenv("TRAVIS_BUILD_DIR");
  if (travis_build && strlen(travis_build) > 0) {
    std::cerr << "Skipping the test of AzureBlob parallel operations on Travis for now as it timing out\n";
    return;
  }
  std::string test_dir("read_write_large");
  // size_t size = ((size_t)TILEDB_UT_MAX_WRITE_COUNT)*4
  size_t size = ((size_t)TILEDB_UT_MAX_WRITE_COUNT);
  void *buffer = malloc(size);
  if (buffer) {
    memset(buffer, 'B', size);
    REQUIRE(azure_blob->create_dir(test_dir) == TILEDB_FS_OK);
    CHECK_RC(azure_blob->write_to_file(test_dir+"/foo", buffer, size), TILEDB_FS_OK);
    CHECK_RC(azure_blob->sync_path(test_dir+"/foo"), TILEDB_FS_OK);
    CHECK(azure_blob->is_file(test_dir+"/foo"));
    CHECK(azure_blob->file_size(test_dir+"/foo") == size);

    void *buffer1 = malloc(size);
    if (buffer1) {
      memset(buffer1, 0, size);
      CHECK_RC(azure_blob->read_from_file(test_dir+"/foo", 0, buffer1, size), TILEDB_FS_OK);

      CHECK(memcmp(buffer, buffer1, size) == 0);

      free(buffer1);
    }
    free(buffer);
  }
}

TEST_CASE_METHOD(AzureBlobTestFixture, "Test AzureBlob parallel operations", "[parallel]") {
  if (azure_blob == nullptr) {
    return;
  }
  char *travis_build = getenv("TRAVIS_BUILD_DIR");
  if (travis_build && strlen(travis_build) > 0) {
    std::cerr << "Skipping the test of AzureBlob parallel operations on Travis for now as it timing out\n";
    return;
  }
  std::string test_dir("parallel");
  REQUIRE(azure_blob->create_dir(test_dir) == TILEDB_FS_OK);

  bool complete = true;
  uint iterations = 2;

  #pragma omp parallel for
  for (uint i=0; i<iterations; i++) {
    std::string filename = test_dir+"/foo"+std::to_string(i);

    for (auto j=0; j<2; j++) {
      size_t size = TILEDB_UT_MAX_WRITE_COUNT;
      void *buffer = malloc(size);
      if (buffer) {
	memset(buffer, 'X', size);
	CHECK_RC(azure_blob->write_to_file(filename, buffer, size), TILEDB_FS_OK);
	free(buffer);
      } else {
	complete = false;
      }
    }
  }

  CHECK_RC(azure_blob->sync_path(test_dir), TILEDB_FS_OK);

  if (complete) {
    #pragma omp parallel for
    for (uint i=0; i<iterations; i++) {
      std::string filename = test_dir+"/foo"+std::to_string(i);
      CHECK_RC(azure_blob->sync_path(filename), TILEDB_FS_OK);
      CHECK(azure_blob->is_file(filename));
      CHECK(azure_blob->file_size(filename) == ((size_t)TILEDB_UT_MAX_WRITE_COUNT)*2);
    }
  }

  CHECK_RC(azure_blob->delete_dir(test_dir), TILEDB_FS_OK);
}

