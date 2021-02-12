/**
 * @file   test_s3_storage.cc
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
 * Tests for the S3 storage class
 */

#include "catch.h"
#include "storage_s3.h"
#include "uri.h"
#include "utils.h"

#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>


class S3TestFixture {
 protected:
  TempDir *temp_dir = NULL;
  S3 *s3_instance = NULL;
  bool is_minio = false;

  S3TestFixture() {
    if (is_s3_storage_path(get_test_dir())) {
      try {
        temp_dir = new TempDir();
        std::string home_dir = temp_dir->get_temp_dir()+"/test_s3";
        s3_instance = new S3(home_dir);
        CHECK(!s3_instance->locking_support());
        if (getenv("MINIO_ACCESS_KEY")) {
          is_minio = true;
        }
      } catch(...) {
        INFO("S3 Storage could not be credentialed");
      }
    }
    INFO("S3 Storage not specified as a --test-dir option");
  }

  ~S3TestFixture() {
    if (s3_instance) {
      delete s3_instance;
    }
    if (temp_dir) {
      delete temp_dir;
    }
  }
};


TEST_CASE("Test S3 constructor", "[constr]") {
  CHECK_THROWS(new S3("s3kdd://my_container/path"));
  CHECK_THROWS(new S3("az://my_container@my_account.blob.core.windows.net/path"));
  CHECK_THROWS(new S3("s3://my_container/path"));
}

TEST_CASE_METHOD(S3TestFixture, "Test S3 cwd", "[cwd]") {
  if (s3_instance == nullptr) {
    return;
  }
  REQUIRE(s3_instance->current_dir().length() > 0);
  REQUIRE(s3_instance->create_dir(s3_instance->current_dir()) == TILEDB_FS_OK);
  REQUIRE(s3_instance->is_dir(s3_instance->current_dir()));
  REQUIRE(!s3_instance->is_file(s3_instance->current_dir()));
  REQUIRE(s3_instance->real_dir(s3_instance->current_dir()).length() > 0);
}

TEST_CASE_METHOD(S3TestFixture, "Test S3 real_dir", "[real_dir]") {
  if (s3_instance == nullptr) {
    return;
  }
  CHECK(s3_instance->real_dir("").compare(s3_instance->current_dir()) == 0);
  CHECK(s3_instance->real_dir("xxx").compare(s3_instance->current_dir()+"/xxx") == 0);
  CHECK(s3_instance->real_dir("xxx/yyy").compare(s3_instance->current_dir()+"/xxx/yyy") == 0);
  CHECK(s3_instance->real_dir("/xxx/yyy").compare("xxx/yyy") == 0);
  s3_uri test_uri(get_test_dir());
  CHECK(s3_instance->real_dir(get_test_dir()).compare(test_uri.path().substr(1)) == 0);
  CHECK_THROWS(s3_instance->real_dir("xxx://yyy"));
}

TEST_CASE_METHOD(S3TestFixture, "Test S3 dir", "[dir]") {
  if (s3_instance == nullptr) {
    return;
  }
  CHECK(!s3_instance->is_dir("non-existent-dir"));
  CHECK(!s3_instance->is_dir("non-existent-parent-dir/dir"));
      
  std::string test_dir("dir");
  CHECK_RC(s3_instance->create_dir(test_dir), TILEDB_FS_OK);
  CHECK(s3_instance->is_dir(test_dir));
  CHECK(!s3_instance->is_file(test_dir));
  CHECK_RC(s3_instance->create_dir(test_dir), TILEDB_FS_ERR); // Dir already exists
  CHECK_RC(s3_instance->create_file(test_dir, 0, 0), TILEDB_FS_ERR);
  CHECK_RC(s3_instance->create_file(s3_instance->real_dir(test_dir), 0, 0), TILEDB_FS_ERR);
  CHECK(s3_instance->file_size(test_dir) == TILEDB_FS_ERR);
  CHECK(s3_instance->get_dirs(test_dir).size() == 0);
  CHECK(s3_instance->get_files(test_dir).size() == 0);
  CHECK(s3_instance->get_dirs("non-existent-dir").size() == 0);
  CHECK(s3_instance->get_files("non-existent-dir").size() == 0);

  // TBD: move_path
  std::string new_dir = test_dir+"-new";
  CHECK_THROWS(s3_instance->move_path(test_dir, new_dir));

  CHECK_RC(s3_instance->sync_path(test_dir), TILEDB_FS_OK);
  // No support for returning errors for non-existent paths
  CHECK_RC(s3_instance->sync_path("non-existent-dir"), TILEDB_FS_OK);

  CHECK_RC(s3_instance->delete_dir(test_dir), TILEDB_FS_OK);
  CHECK_RC(s3_instance->delete_dir("non-existent-dir"), TILEDB_FS_ERR);
  CHECK(!s3_instance->is_dir(test_dir));

  CHECK(!s3_instance->is_file(test_dir));
}

TEST_CASE_METHOD(S3TestFixture, "Test S3 file", "[file]") {
  if (s3_instance == nullptr) {
    return;
  }
  std::string test_dir("file");
  CHECK_RC(s3_instance->create_dir(test_dir), 0);
  REQUIRE(s3_instance->is_dir(test_dir));
  CHECK_RC(s3_instance->create_file(test_dir+"/foo", O_WRONLY|O_CREAT,  S_IRWXU), TILEDB_FS_OK);
  CHECK(s3_instance->is_file(test_dir+"/foo"));
  CHECK(!s3_instance->is_dir(test_dir+"/foo"));
  CHECK(s3_instance->file_size(test_dir+"/foo") == 0);
  CHECK(s3_instance->file_size(test_dir+"/foo1") == TILEDB_FS_ERR);

  CHECK(s3_instance->get_files(test_dir).size() == 1);
  CHECK(s3_instance->get_files("non-existent-dir").size() == 0);
  
  CHECK_RC(s3_instance->create_file(test_dir+"/foo1", O_WRONLY|O_CREAT,  S_IRWXU), TILEDB_FS_OK);
  CHECK(s3_instance->get_files(test_dir).size() == 2);

  CHECK_RC(s3_instance->sync_path(test_dir+"/foo"), TILEDB_FS_OK);
  CHECK_RC(s3_instance->sync_path(test_dir), TILEDB_FS_OK);

  CHECK_RC(s3_instance->delete_file(test_dir+"/foo"), TILEDB_FS_OK);
  CHECK_RC(s3_instance->delete_file(test_dir+"/foo1"), TILEDB_FS_OK);
  CHECK_RC(s3_instance->delete_file(test_dir+"/foo2"), TILEDB_FS_ERR);

  CHECK(!s3_instance->is_file(test_dir+"/foo1"));
  CHECK(!s3_instance->is_file(test_dir+"/foo2"));
  CHECK(!s3_instance->is_dir(test_dir+"/foo1"));
  CHECK(!s3_instance->is_dir(test_dir+"/foo2"));
}


TEST_CASE_METHOD(S3TestFixture, "Test S3 read/write file", "[read-write]") {
  if (s3_instance == nullptr) {
    return;
  }
  std::string test_dir("read_write");
  CHECK_RC(s3_instance->create_dir(test_dir), TILEDB_FS_OK);
  REQUIRE(s3_instance->is_dir(test_dir));
  CHECK_RC(s3_instance->write_to_file(test_dir+"/foo", "hello", 5), TILEDB_FS_OK);

  CHECK_RC(s3_instance->sync_path(test_dir+"/foo"), TILEDB_FS_OK);
  auto files = s3_instance->get_files(test_dir);
  for(auto f: files) {
    std::cerr << "file=" << f << std::endl;
  }
  REQUIRE(s3_instance->is_file(test_dir+"/foo"));
  CHECK(s3_instance->file_size(test_dir+"/foo") == 5);

  void *buffer = malloc(20);
  memset(buffer, 'X', 20);
  CHECK_RC(s3_instance->read_from_file(test_dir+"/foo", 0, buffer, 0), TILEDB_FS_OK);
  CHECK_RC(s3_instance->read_from_file(test_dir+"/foo", 0, buffer, 2), TILEDB_FS_OK);
  CHECK(((char *)buffer)[0] == 'h');
  CHECK(((char *)buffer)[1] == 'e');
  CHECK_RC(s3_instance->read_from_file(test_dir+"/foo", 0, buffer, 5), TILEDB_FS_OK);
  CHECK(((char *)buffer)[4] == 'o');
  CHECK_RC(s3_instance->read_from_file(test_dir+"/foo", 0, buffer, 6), TILEDB_FS_ERR);
  CHECK_RC(s3_instance->close_file(test_dir+"/foo"), TILEDB_FS_OK);

  if (!is_minio) {
    memset(buffer, 'X', 20);
    CHECK_RC(s3_instance->write_to_file(test_dir+"/foo1", buffer, 20), TILEDB_FS_OK);
    CHECK_RC(s3_instance->close_file(test_dir+"/foo1"), TILEDB_FS_OK);
    REQUIRE(s3_instance->is_file(test_dir+"/foo1"));
    for (int i=0; i<4; i++) {
      void *buf = malloc(5);
      memset(buf, 'Z', 5);
      CHECK_RC(s3_instance->read_from_file(test_dir+"/foo1", i*5, buf, 5), TILEDB_FS_OK);
      for (int j=0; j<5; j++) {
        CHECK(((char *)buf)[j] == 'X');
      }
      free(buf);
    }
  }
  
  // This part is different from Azure Blob Storage where you are allowed to write as many chunks in any size
  if (!is_minio) {
    // No overwriting allowed with minio, so skip this set of tests for now
    CHECK_RC(s3_instance->write_to_file(test_dir+"/foo", "hello", 5), TILEDB_FS_OK);
    CHECK_RC(s3_instance->write_to_file(test_dir+"/foo", " there ", 6), TILEDB_FS_ERR); // Prev part should at least be 5M, otherwise should be aborted
    CHECK_RC(s3_instance->close_file(test_dir+"/foo"), TILEDB_FS_OK);
    CHECK(s3_instance->file_size(test_dir+"/foo") == 5); // Previous file size as the current set of writes failed
  }

  CHECK_RC(s3_instance->read_from_file(test_dir+"/non-existent-file", 0, buffer, 5), TILEDB_FS_ERR);
  CHECK_RC(s3_instance->read_from_file("non-existent-dir/foo", 0, buffer, 5), TILEDB_FS_ERR);
  CHECK_RC(s3_instance->close_file("non-existent-dir/foo"), TILEDB_FS_OK);

  free(buffer);
}

TEST_CASE_METHOD(S3TestFixture, "Test S3 large read/write file", "[read-write-large]") {
  if (s3_instance == nullptr || is_minio) {
    return;
  }
  std::string test_dir("read_write_large");
  // size_t size = ((size_t)TILEDB_UT_MAX_WRITE_COUNT)*4
  size_t size = ((size_t)TILEDB_UT_MAX_WRITE_COUNT);
  void *buffer = malloc(size);
  if (buffer) {
    memset(buffer, 'B', size);
    REQUIRE(s3_instance->create_dir(test_dir) == TILEDB_FS_OK);
    CHECK_RC(s3_instance->write_to_file(test_dir+"/foo", buffer, size), TILEDB_FS_OK);
    CHECK_RC(s3_instance->sync_path(test_dir+"/foo"), TILEDB_FS_OK);
    CHECK(s3_instance->is_file(test_dir+"/foo"));
    CHECK(s3_instance->file_size(test_dir+"/foo") == size);

    void *buffer1 = malloc(size);
    if (buffer1) {
      memset(buffer1, 0, size);
      CHECK_RC(s3_instance->read_from_file(test_dir+"/foo", 0, buffer1, size), TILEDB_FS_OK);

      CHECK(memcmp(buffer, buffer1, size) == 0);

      free(buffer1);
    }
    free(buffer);
  }
}

TEST_CASE_METHOD(S3TestFixture, "Test S3 operations", "[parallel]") {
  if (s3_instance == nullptr) {
    return;
  }

  std::string test_dir("parallel");
  REQUIRE(s3_instance->create_dir(test_dir) == TILEDB_FS_OK);

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
	CHECK_RC(s3_instance->write_to_file(filename, buffer, size), TILEDB_FS_OK);
	free(buffer);
      } else {
	complete = false;
      }
    }
  }

  CHECK_RC(s3_instance->sync_path(test_dir), TILEDB_FS_OK);

  if (complete) {
    #pragma omp parallel for
    for (uint i=0; i<iterations; i++) {
      std::string filename = test_dir+"/foo"+std::to_string(i);
      CHECK_RC(s3_instance->sync_path(filename), TILEDB_FS_OK);
      CHECK(s3_instance->is_file(filename));
      CHECK(s3_instance->file_size(filename) == (size_t)(10*1024*1024*2));
    }
  }

  CHECK_RC(s3_instance->delete_dir(test_dir), TILEDB_FS_OK);
}

