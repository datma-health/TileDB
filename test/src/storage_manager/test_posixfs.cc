/**
 * @file   test_posixfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 Omics Data Automation, Inc.
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
 * Tests for the PosixFS class
 */

#include "catch.h"
#include "buffer.h"
#include "storage_posixfs.h"
#include "utils.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <cstdio>
#include <string>
#include <thread>


class PosixFSTestFixture {
 protected:
  PosixFS fs;
  std::string test_dir = "test_posixfs_dir";

  ~PosixFSTestFixture() {
    // Remove the temporary dir
    std::string command = "rm -rf ";
    command.append(test_dir);
    CHECK_RC(system(command.c_str()), 0);
    CHECK_RC(fs.sync_path("."), TILEDB_FS_OK);
  }
 };

TEST_CASE_METHOD(PosixFSTestFixture, "Test PosixFS cwd", "[cwd]") {
  REQUIRE(fs.current_dir().length() > 0);
  REQUIRE(fs.is_dir(fs.current_dir()));
  REQUIRE(!fs.is_file(fs.current_dir()));
  REQUIRE(fs.real_dir(fs.current_dir()).length() > 0);
}

TEST_CASE_METHOD(PosixFSTestFixture, "Test PosixFS real_dir", "[real_dir]") {
  CHECK(fs.real_dir("").compare(fs.current_dir()) == 0);
  CHECK(fs.real_dir(".").compare(fs.current_dir()) == 0);
  CHECK(fs.real_dir("./").compare(fs.current_dir()) == 0);
  CHECK(fs.real_dir("./foo").compare(fs.current_dir()+"/foo") == 0);
  CHECK(fs.real_dir("/").compare("/") == 0);
  CHECK(fs.real_dir("~").compare(getenv("HOME")) == 0);
  CHECK(fs.real_dir("~/foo").compare(std::string(getenv("HOME"))+"/foo") == 0);
  CHECK(fs.real_dir("/tmp").compare("/tmp") == 0);
  CHECK(fs.real_dir("/tmp/./xxx").compare("/tmp/xxx") == 0);
  CHECK(fs.real_dir("/tmp//xxx").compare("/tmp/xxx") == 0);
  CHECK(fs.real_dir("/tmp/../xxx").compare("/xxx") == 0);
  CHECK(fs.real_dir("xxx").compare(fs.current_dir()+"/xxx") == 0);
}

TEST_CASE_METHOD(PosixFSTestFixture, "Test PosixFS dir", "[dir]") {
  CHECK_RC(fs.create_dir(test_dir), TILEDB_FS_OK);
  CHECK(fs.is_dir(test_dir));
  CHECK(!fs.is_file(test_dir));
  CHECK_RC(fs.create_dir(test_dir), TILEDB_FS_ERR); // Dir already exists
  CHECK_RC(fs.create_dir("/non-existent-dir/foo"), TILEDB_FS_ERR);
  CHECK_RC(fs.create_file(test_dir, O_WRONLY|O_CREAT,  S_IRWXU), TILEDB_FS_ERR);
  CHECK_RC(fs.create_file(fs.real_dir(test_dir), O_WRONLY|O_CREAT,  S_IRWXU), TILEDB_FS_ERR);
  CHECK(fs.file_size(test_dir) == TILEDB_FS_ERR);
  CHECK(fs.get_dirs(".").size() > 0);
  CHECK(fs.get_dirs(test_dir).size() == 0);
  CHECK(fs.get_dirs("non-existent-dir").size() == 0);

  CHECK_RC(fs.move_path(test_dir, "new-dir"), TILEDB_FS_OK);
  CHECK(fs.is_dir("new-dir"));
  CHECK(!fs.is_dir(test_dir));
  CHECK_RC(fs.move_path("new-dir", test_dir), TILEDB_FS_OK);
  CHECK(!fs.is_dir("new-dir"));
  CHECK(fs.is_dir(test_dir));
  CHECK_RC(fs.move_path("non-existent-dir", "new-dir"), TILEDB_FS_ERR);
  CHECK_RC(fs.move_path(test_dir, test_dir), TILEDB_FS_OK);
  CHECK(fs.is_dir(test_dir));

  CHECK_RC(fs.sync_path(test_dir), TILEDB_FS_OK);
  CHECK_RC(fs.sync_path("non-existent-dir"), TILEDB_FS_OK);

  CHECK_RC(fs.delete_dir(test_dir), TILEDB_FS_OK);
  CHECK_RC(fs.delete_dir("non-existent-dir"), TILEDB_FS_ERR);
}

TEST_CASE_METHOD(PosixFSTestFixture, "Test PosixFS file", "[file]") {
  CHECK_RC(fs.create_dir(test_dir), 0);
  REQUIRE(fs.is_dir(test_dir));
  CHECK_RC(fs.create_file(test_dir+"/foo", O_WRONLY|O_CREAT,  S_IRWXU), TILEDB_FS_OK);
  CHECK(fs.is_file(test_dir+"/foo"));
  CHECK(!fs.is_dir(test_dir+"/foo"));
  CHECK(fs.file_size(test_dir+"/foo") == 0);
  CHECK(fs.file_size(test_dir+"/foo1") == TILEDB_FS_ERR);
  CHECK(fs.get_files(".").size() > 0);
  CHECK(fs.get_files(test_dir).size() == 1);
  CHECK(fs.get_files("non-existent-dir").size() == 0);

  CHECK_RC(fs.move_path(test_dir+"/foo", test_dir+"/foo2"), TILEDB_FS_OK);
  CHECK(fs.is_file(test_dir+"/foo2"));
  CHECK(!fs.is_file(test_dir+"/foo"));
  CHECK_RC(fs.move_path(test_dir+"/foo2", test_dir+"/foo"), TILEDB_FS_OK);
  CHECK(!fs.is_file(test_dir+"/foo2"));
  CHECK(fs.is_file(test_dir+"/foo"));
  CHECK_RC(fs.move_path(test_dir+"/foo", test_dir+"/foo"), TILEDB_FS_OK);
  CHECK(fs.is_file(test_dir+"/foo"));

  CHECK_RC(fs.sync_path(test_dir+"/foo"), TILEDB_FS_OK);
  CHECK_RC(fs.sync_path(test_dir), TILEDB_FS_OK);

  CHECK_RC(fs.delete_file(test_dir+"/foo"), TILEDB_FS_OK);
  CHECK_RC(fs.delete_file(test_dir+"/foo1"), TILEDB_FS_ERR);
}

TEST_CASE_METHOD(PosixFSTestFixture, "Test PosixFS read/write file", "[read-write]") {
 CHECK_RC(fs.create_dir(test_dir), 0);
 REQUIRE(fs.is_dir(test_dir));
 CHECK_RC(fs.write_to_file(test_dir+"/foo", "hello", 5), TILEDB_FS_OK);
 REQUIRE(fs.is_file(test_dir+"/foo"));
 CHECK(fs.file_size(test_dir+"/foo") == 5);

 void *buffer = malloc(20);
 CHECK_RC(fs.read_from_file(test_dir+"/foo", 0, buffer, 0), TILEDB_FS_OK);
 CHECK_RC(fs.read_from_file(test_dir+"/foo", 0, buffer, 2), TILEDB_FS_OK);
 CHECK_RC(fs.read_from_file(test_dir+"/foo", 0, buffer, 5), TILEDB_FS_OK);
 CHECK_RC(fs.read_from_file(test_dir+"/foo", 0, buffer, 6), TILEDB_FS_ERR);
 CHECK_RC(fs.read_from_file(test_dir+"/foo", 3, buffer, 2), TILEDB_FS_OK);
 CHECK_RC(fs.read_from_file(test_dir+"/foo", 3, buffer, 6), TILEDB_FS_ERR);
 CHECK_RC(fs.read_from_file(test_dir+"/foo", 6, buffer, 2), TILEDB_FS_ERR);
 
 CHECK_RC(fs.write_to_file(test_dir+"/foo", "hello", 5), TILEDB_FS_OK);
 CHECK(fs.file_size(test_dir+"/foo") == 10);
 
 CHECK_RC(fs.read_from_file("non-existen-dir/foo", 0, buffer, 5), TILEDB_FS_ERR);
 CHECK_RC(fs.write_to_file("non-existent-dir/foo", "hello", 5), TILEDB_FS_ERR);

 free(buffer);
}

TEST_CASE_METHOD(PosixFSTestFixture, "Test PosixFS large read/write file", "[read-write-large]") {
  size_t size = ((size_t)TILEDB_UT_MAX_WRITE_COUNT)*4;
  void *buffer = malloc(size);
  if (buffer) {
    memset(buffer, 'B', size);
    REQUIRE(fs.create_dir(test_dir) == TILEDB_FS_OK);
    CHECK_RC(fs.write_to_file(test_dir+"/foo", buffer, size), TILEDB_FS_OK);
    CHECK(fs.file_size(test_dir+"/foo") == size);

    void *buffer1 = malloc(size);
    if (buffer1) {
      memset(buffer1, 0, size);
      CHECK_RC(fs.read_from_file(test_dir+"/foo", 0, buffer1, size), TILEDB_FS_OK);

      CHECK(memcmp(buffer, buffer1, size) == 0);

      free(buffer1);
    }
    free(buffer);
  }
}

TEST_CASE_METHOD(PosixFSTestFixture, "Test PosixFS parallel operations", "[parallel]") {
  REQUIRE(fs.create_dir(test_dir) == TILEDB_FS_OK);

  bool complete = true;
  uint iterations = std::thread::hardware_concurrency();
  for (uint i=0; i<iterations; i++) {
    std::string filename = test_dir+"/foo"+std::to_string(i);

    for (auto j=0; j<2; j++) {
      size_t size = TILEDB_UT_MAX_WRITE_COUNT;
      void *buffer = malloc(size);
      if (buffer) {
	memset(buffer, 'X', size);
	CHECK_RC(fs.write_to_file(filename, buffer, size), TILEDB_FS_OK);
	free(buffer);
      } else {
	complete = false;
      }
    }
  }

  CHECK_RC(fs.sync_path(test_dir), TILEDB_FS_OK);
  CHECK_RC(fs.move_path(test_dir, test_dir+"new"), TILEDB_FS_OK);
  CHECK(fs.is_dir(test_dir+"new"));

  if (complete) {
    for (uint i=0; i<iterations; i++) {
      std::string filename = test_dir+"new/foo"+std::to_string(i);
      CHECK(fs.is_file(filename));
      CHECK(fs.file_size(filename) == ((size_t)TILEDB_UT_MAX_WRITE_COUNT)*2);
    }
  }

  CHECK_RC(fs.delete_dir(test_dir+"new"), TILEDB_FS_OK);
}
