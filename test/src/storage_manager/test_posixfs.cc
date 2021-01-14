/**
 * @file   test_posixfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
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
 * Tests for the PosixFS class
 */

#include "catch.h"
#include "buffer.h"
#include "storage_posixfs.h"
#include "utils.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <cctype>
#include <cstdio>
#include <string>
#include <thread>

#ifdef HAVE_OPENMP
  #include <omp.h>
#endif
#include <unistd.h>

class PosixFSTestFixture {
 protected:
  PosixFS fs;

  TempDir *td;
  std::string test_dir;

  PosixFSTestFixture() {
    td = new TempDir();
    CHECK(fs.is_dir(td->get_temp_dir()));
    test_dir = td->get_temp_dir()+"/test_posixfs_dir";
    if (getenv( "TILEDB_DISABLE_FILE_LOCKING")) {
      unsetenv( "TILEDB_DISABLE_FILE_LOCKING");
    }
    REQUIRE(fs.locking_support());
    CHECK(fs.locking_support());
  }

  ~PosixFSTestFixture() {
    delete td;
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
  test_dir += "dir";
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

  std::string new_dir = test_dir+"-new";
  CHECK_RC(fs.move_path(test_dir, new_dir), TILEDB_FS_OK);
  CHECK(fs.is_dir(new_dir));
  CHECK(!fs.is_dir(test_dir));
  CHECK_RC(fs.move_path(new_dir, test_dir), TILEDB_FS_OK);
  CHECK(!fs.is_dir(new_dir));
  CHECK(fs.is_dir(test_dir));
  CHECK_RC(fs.move_path("non-existent-dir", new_dir), TILEDB_FS_ERR);
  CHECK_RC(fs.move_path(test_dir, test_dir), TILEDB_FS_OK);
  CHECK(fs.is_dir(test_dir));

  CHECK_RC(fs.sync_path(test_dir), TILEDB_FS_OK);
  CHECK_RC(fs.sync_path("non-existent-dir"), TILEDB_FS_OK);

  CHECK_RC(fs.delete_dir(test_dir), TILEDB_FS_OK);
  CHECK_RC(fs.delete_dir("non-existent-dir"), TILEDB_FS_ERR);
}

TEST_CASE_METHOD(PosixFSTestFixture, "Test PosixFS file", "[file]") {
  test_dir += "file";
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
  test_dir += "read_write";
  CHECK_RC(fs.create_dir(test_dir), TILEDB_FS_OK);
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
  CHECK_RC(fs.close_file(test_dir+"/foo"), TILEDB_FS_OK); // NOP when there is no locking support

  CHECK_RC(fs.sync_path(test_dir+"/foo"), TILEDB_FS_OK);
  CHECK_RC(fs.delete_file(test_dir+"/foo"), TILEDB_FS_OK);

  CHECK_RC(fs.sync_path(test_dir), TILEDB_FS_OK);

  CHECK_RC(fs.read_from_file("non-existent-dir/foo", 0, buffer, 5), TILEDB_FS_ERR);
  CHECK_RC(fs.write_to_file("non-existent-dir/foo", "hello", 5), TILEDB_FS_ERR);
  CHECK_RC(fs.close_file("non-existent-dir/foo"), TILEDB_FS_OK); // NOP when there is no locking support

  free(buffer);
}

TEST_CASE_METHOD(PosixFSTestFixture, "Test PosixFS large read/write file", "[read-write-large]") {
  if (!is_env_set("TILEDB_TEST_POSIXFS_LARGE")) {
    return;
  }
  test_dir += "read_write_large";
  ssize_t size = (ssize_t)TILEDB_UT_MAX_WRITE_COUNT*4;
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
  test_dir += "parallel";
  REQUIRE(fs.create_dir(test_dir) == TILEDB_FS_OK);

  bool complete = true;
  uint iterations = 4;

  #pragma omp parallel for
  for (uint i=0; i<iterations; i++) {
    std::string filename = test_dir+"/foo"+std::to_string(i);

    for (auto j=0; j<2; j++) {
      size_t size = 2048;
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
    #pragma omp parallel for
    for (uint i=0; i<iterations; i++) {
      std::string filename = test_dir+"new/foo"+std::to_string(i);
      CHECK(fs.is_file(filename));
      CHECK(fs.file_size(filename) == 2048*2);
    }
  }

  CHECK_RC(fs.delete_dir(test_dir+"new"), TILEDB_FS_OK);
}

void test_locking_support(const std::string& disable_file_locking_value) {
  std::string disable_file_locking_env = "TILEDB_DISABLE_FILE_LOCKING="+disable_file_locking_value;
  CHECK(putenv(const_cast<char *>(disable_file_locking_env.c_str())) == 0);
  const char *value = disable_file_locking_value.c_str();
  const char *env_value = getenv("TILEDB_DISABLE_FILE_LOCKING");
  REQUIRE(env_value != NULL);
  CHECK(strcmp(value, env_value) == 0);
  PosixFS fs;
  if (strcasecmp(value, "true") == 0 || strcmp(value, "1") == 0) {
    CHECK(!fs.locking_support());
  } else {
    CHECK(fs.locking_support());
  }
}

TEST_CASE("Test locking support", "[locking_support]") {
  PosixFS fs;
  CHECK(!fs.disable_file_locking()); // default
  CHECK(fs.locking_support());
  fs.set_disable_file_locking(true);
  CHECK(fs.disable_file_locking());
  CHECK(!fs.locking_support());
  fs.set_disable_file_locking(false);
  CHECK(!fs.disable_file_locking());
  CHECK(fs.locking_support());

  test_locking_support("True");
  test_locking_support("true");
  test_locking_support("TRUE");
  test_locking_support("1");
  test_locking_support("0");
  test_locking_support("False");
  test_locking_support("false");
  test_locking_support("FALSE");
  test_locking_support("Gibberish");
}

void test_keep_file_handles_open_support(const std::string& keep_file_handles_open_value) {
  std::string keep_file_handles_open_env = "TILEDB_KEEP_FILE_HANDLES_OPEN="+keep_file_handles_open_value;
  CHECK(putenv(const_cast<char *>(keep_file_handles_open_env.c_str())) == 0);
  const char *value = keep_file_handles_open_value.c_str();
  const char *env_value = getenv("TILEDB_KEEP_FILE_HANDLES_OPEN");
  REQUIRE(env_value != NULL);
  CHECK(strcmp(value, env_value) == 0);
  PosixFS fs;
  if (strcasecmp(value, "true") == 0 || strcmp(value, "1") == 0) {
    CHECK(fs.keep_write_file_handles_open());
  } else {
    CHECK(!fs.keep_write_file_handles_open());
  }
}

TEST_CASE("Test keep file handles open", "[keep_file_handles_open_support]") {
  PosixFS fs;
  CHECK(!fs.keep_write_file_handles_open()); // default
  fs.set_keep_write_file_handles_open(true);
  CHECK(fs.keep_write_file_handles_open());
  fs.set_keep_write_file_handles_open(false);
  CHECK(!fs.keep_write_file_handles_open());

  test_keep_file_handles_open_support("True");
  test_keep_file_handles_open_support("true");
  test_keep_file_handles_open_support("TRUE");
  test_keep_file_handles_open_support("1");
  test_keep_file_handles_open_support("0");
  test_keep_file_handles_open_support("False");
  test_keep_file_handles_open_support("false");
  test_keep_file_handles_open_support("FALSE");
  test_keep_file_handles_open_support("Gibberish");
}

TEST_CASE("Test writing with keeps file descriptors open until explicitly closed", "[write_keep_file_handles_open]") {
  CHECK(setenv("TILEDB_KEEP_FILE_HANDLES_OPEN", "1", 1) == 0);

  PosixFS fs;
  REQUIRE(fs.keep_write_file_handles_open());

  std::string test_dir = "test_posixfs_dir_locking";
  if (fs.is_dir(test_dir)) {
    REQUIRE(fs.delete_dir(test_dir) == TILEDB_OK);
  }
  CHECK_RC(fs.create_dir(test_dir), 0);
  REQUIRE(fs.is_dir(test_dir));
  CHECK_RC(fs.write_to_file(test_dir+"/foo", "hello", 6), TILEDB_FS_OK);
  REQUIRE(fs.is_file(test_dir+"/foo"));
  CHECK(fs.file_size(test_dir+"/foo") == 6);
  void *buffer = malloc(20);
  CHECK_RC(fs.read_from_file(test_dir+"/foo", 0, buffer, 6), TILEDB_FS_ERR); // No simultaneous read/write allowed on open files
  CHECK_RC(fs.sync_path(test_dir+"/foo"), TILEDB_FS_OK);
  CHECK_RC(fs.close_file(test_dir+"/foo"), TILEDB_FS_OK);
  CHECK_RC(fs.read_from_file(test_dir+"/foo", 0, buffer, 6), TILEDB_FS_OK);
  char *buffer_str = (char *)buffer;
  CHECK(strlen(buffer_str) == 5);
  CHECK(strcmp(buffer_str, "hello") == 0);

  CHECK_RC(fs.delete_file(test_dir+"/foo"), TILEDB_FS_OK);
  REQUIRE(!fs.is_file(test_dir+"/foo"));

  // Test multiple writes
  int n_iter = 2;
  for (int i=0; i<n_iter; i++) {
    CHECK_RC(fs.write_to_file(test_dir+"/foo", "hello", 6), TILEDB_FS_OK);
    REQUIRE(fs.is_file(test_dir+"/foo"));
    CHECK(fs.file_size(test_dir+"/foo") == 6*(i+1));
  }
  CHECK_RC(fs.close_file(test_dir+"/foo"), TILEDB_FS_OK);

  PosixFS fs1;
  
  buffer = realloc(buffer, 6*n_iter);
  CHECK_RC(fs1.read_from_file(test_dir+"/foo", 0, buffer, 6*n_iter), TILEDB_FS_OK);
  CHECK(fs1.file_size(test_dir+"/foo") == 6*n_iter);
  CHECK_RC(fs1.delete_file(test_dir+"/foo"), TILEDB_FS_OK);
  
  // Try closing non-existent file
  REQUIRE(!fs1.is_file(test_dir+"/foo"));
  CHECK_RC(fs1.close_file(test_dir+"/foo"), TILEDB_FS_OK);

  CHECK_RC(fs1.delete_dir(test_dir), TILEDB_FS_OK);

  free(buffer);
}



TEST_CASE("Test reading/writing with keep file handles open set for write and unset for read", "[write_keep_file_handles_set_write_unset_read]") {
  CHECK(setenv("TILEDB_KEEP_FILE_HANDLES_OPEN", "1", 1) == 0);

  PosixFS fs;
  REQUIRE(fs.keep_write_file_handles_open());

  std::string test_dir = "test_dir_locking1";
  if (fs.is_dir(test_dir)) {
    REQUIRE(fs.delete_dir(test_dir) == TILEDB_OK);
  }
  CHECK_RC(fs.create_dir(test_dir), 0);
  REQUIRE(fs.is_dir(test_dir));

  // Perform some writes and close the file
  int n_iter = 2;
  for (int i=0; i<n_iter; i++) {
    CHECK_RC(fs.write_to_file(test_dir+"/foo", "hello", 6), TILEDB_FS_OK);
    REQUIRE(fs.is_file(test_dir+"/foo"));
    CHECK(fs.file_size(test_dir+"/foo") == 6*(i+1));
  }
  CHECK_RC(fs.close_file(test_dir+"/foo"), TILEDB_FS_OK);

  // Perform some writes and sync the file. This should log an error but complete successfully
  PosixFS *fs_sync = new PosixFS();
  CHECK_RC(fs_sync->write_to_file(test_dir+"/foo_sync", "hello", 6), TILEDB_FS_OK);
  REQUIRE(fs_sync->is_file(test_dir+"/foo_sync"));
  CHECK(fs_sync->file_size(test_dir+"/foo_sync") == 6);
  CHECK_RC(fs_sync->sync_path(test_dir+"/foo"), TILEDB_FS_OK);
  delete fs_sync;
  
  // Perform some writes and do not close/sync the file. This should log an error but complete successfully
  PosixFS *fs_no_close = new PosixFS();
  CHECK_RC(fs_no_close->write_to_file(test_dir+"/foo_no_close", "hello", 6), TILEDB_FS_OK);
  REQUIRE(fs_no_close->is_file(test_dir+"/foo_no_close"));
  CHECK(fs_no_close->file_size(test_dir+"/foo_no_close") == 6);
  delete fs_no_close;

  unsetenv("TILEDB_KEEP_FILE_HANDLES_OPEN");

  PosixFS fs1;
  
  void *buffer = malloc(6*n_iter);
  CHECK_RC(fs1.read_from_file(test_dir+"/foo", 0, buffer, 6*n_iter), TILEDB_FS_OK);
  CHECK(fs1.file_size(test_dir+"/foo") == 6*n_iter);
  CHECK_RC(fs1.delete_file(test_dir+"/foo"), TILEDB_FS_OK);
  
  // Try closing non-existent file
  REQUIRE(!fs1.is_file(test_dir+"/foo"));
  CHECK_RC(fs1.close_file(test_dir+"/foo"), TILEDB_FS_OK);

  CHECK_RC(fs1.delete_dir(test_dir), TILEDB_FS_OK);

  free(buffer);
}
