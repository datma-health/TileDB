/**
 * @file   test_tiledb_utils.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 Omics Data Automation, Inc.
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
 * Unit Tests for tiledb_utils.cc 
 */

#include "catch.h"

#include "tiledb.h"
#include "tiledb_storage.h"
#include "tiledb_utils.h"

#include <string.h>
#include <fcntl.h>

const std::string& workspace("WORKSPACE");

TEST_CASE_METHOD(TempDir, "Test initialize_workspace", "[initialize_workspace]") {
  std::string workspace_path = get_temp_dir()+"/"+workspace;

  TileDB_CTX *tiledb_ctx;
  CHECK(TileDBUtils::initialize_workspace(&tiledb_ctx, workspace_path, false) == 0); // OK
  CHECK(!set_working_dir(tiledb_ctx, workspace_path));
  std::string cwd = current_working_dir(tiledb_ctx);
  CHECK(cwd.size() > 0);
  CHECK(set_working_dir(tiledb_ctx, workspace_path+".non-existent"));
  CHECK(current_working_dir(tiledb_ctx) == cwd);

  CHECK(!tiledb_ctx_finalize(tiledb_ctx));
  CHECK(TileDBUtils::workspace_exists(workspace_path));

  CHECK(TileDBUtils::initialize_workspace(&tiledb_ctx, workspace_path, false) == 1); // EXISTS
  CHECK(!tiledb_ctx_finalize(tiledb_ctx));

  CHECK(TileDBUtils::initialize_workspace(&tiledb_ctx, workspace_path, true) == 0); // OK
  CHECK(!create_file(tiledb_ctx, workspace_path+".new", O_WRONLY | O_CREAT | O_SYNC, S_IRWXU));
  CHECK(!tiledb_ctx_finalize(tiledb_ctx));

  CHECK(TileDBUtils::initialize_workspace(&tiledb_ctx, workspace_path+".new", true) == -1); // NOT_DIR
  
  CHECK(TileDBUtils::initialize_workspace(&tiledb_ctx, workspace_path+".new") == -1); // NOT_DIR
}

TEST_CASE_METHOD(TempDir, "Test create_workspace", "[create_workspace]") {
  std::string workspace_path = get_temp_dir()+"/"+workspace;

  CHECK(!TileDBUtils::workspace_exists(workspace_path));
  
  CHECK(TileDBUtils::create_workspace(workspace_path, false) == TILEDB_OK);
  CHECK(TileDBUtils::workspace_exists(workspace_path));
  
  CHECK(TileDBUtils::create_workspace(workspace_path, false) == TILEDB_ERR);
  CHECK(TileDBUtils::workspace_exists(workspace_path));
  
  CHECK(TileDBUtils::create_workspace(workspace_path, true) == TILEDB_OK);
  CHECK(TileDBUtils::workspace_exists(workspace_path));

  std::string test_str("TESTING");
  std::string test_file(workspace_path+"/test");
  CHECK(TileDBUtils::write_file(test_file, test_str.data(), 4) == TILEDB_OK);
  CHECK(TileDBUtils::is_file(test_file));

  CHECK(TileDBUtils::create_workspace(workspace_path, true) == TILEDB_OK);
  CHECK(TileDBUtils::workspace_exists(workspace_path));

  // test file should not exist as the existing workspace was overwritten
  CHECK(!TileDBUtils::is_file(test_file));
  CHECK(!TileDBUtils::is_dir(test_file));

  // Use defaults
  CHECK(TileDBUtils::create_workspace(workspace_path) == TILEDB_ERR);
  CHECK(TileDBUtils::delete_dir(workspace_path) == TILEDB_OK);

  CHECK(TileDBUtils::create_workspace(workspace_path) == TILEDB_OK);
}

TEST_CASE_METHOD(TempDir, "Test array exists", "[array_exists]") {
  std::string workspace_path = get_temp_dir()+"/"+workspace;
  std::string non_existent_array = std::string("non_existent_array");

  // No workspace or array
  CHECK(!TileDBUtils::array_exists(workspace_path, non_existent_array));
  
  CHECK(TileDBUtils::create_workspace(workspace_path, false) == TILEDB_OK);
  CHECK(TileDBUtils::workspace_exists(workspace_path));

  // workspace exists but no array
  CHECK(!TileDBUtils::array_exists(workspace_path, non_existent_array));

  std::string input_ws = std::string(TILEDB_TEST_DIR)+"/inputs/compatibility_gdb_pre100_ws";
  std::string array_name("t0_1_2");
  CHECK(TileDBUtils::workspace_exists(input_ws));
  CHECK(TileDBUtils::array_exists(input_ws, array_name));
}

TEST_CASE_METHOD(TempDir, "Test get fragment names", "[get_fragment_names]") {
  std::string workspace_path = get_temp_dir()+"/"+workspace;

  // No workspace or array or fragments
  CHECK(TileDBUtils::get_fragment_names(workspace_path).size() == 0);

  CHECK(TileDBUtils::create_workspace(workspace_path, false) == TILEDB_OK);
  CHECK(TileDBUtils::workspace_exists(workspace_path));
  
  // No array or fragments
  CHECK(TileDBUtils::get_fragment_names(workspace_path).size() == 0);

  std::string input_ws = std::string(TILEDB_TEST_DIR)+"/inputs/compatibility_gdb_pre100_ws";
  std::string array_name("t0_1_2");
  CHECK(TileDBUtils::workspace_exists(input_ws));
  CHECK(TileDBUtils::array_exists(input_ws, array_name));

  // No fragments
  CHECK(TileDBUtils::get_fragment_names(input_ws).size() == 0);

  // TODO: Add input with one fragment
}

TEST_CASE_METHOD(TempDir, "Test file operations", "[file_ops]") {
  std::string filename = get_temp_dir()+"/"+"test_file";
  char buffer[1024];
  memset(buffer, 'H', 1024);
  CHECK(TileDBUtils::write_file(filename, buffer, 1024) == TILEDB_OK);

  void *read_buffer = NULL;
  size_t length;
  CHECK(TileDBUtils::read_entire_file(filename, &read_buffer, &length) == TILEDB_OK);
  CHECK(read_buffer);
  CHECK(length == 1024);
  for (auto i=0; i<1024; i++) {
    char *ptr = (char *)(read_buffer)+i;
    CHECK(*ptr == 'H');
  }
  free(read_buffer);

  memset(buffer, 0, 1024);
  CHECK(TileDBUtils::read_file(filename, 256, buffer, 256) == TILEDB_OK);
  for (auto i=0; i<256; i++) {
    CHECK(buffer[i] == 'H');
  }

  // read offset > filesize
  memset(buffer, 0, 1024);
  CHECK(TileDBUtils::read_file(filename, 1025, buffer, 256) == TILEDB_ERR);

  // TODO: Should investigate why the hdfs java io exceptions are not being propagated properly.
  if (TileDBUtils::is_cloud_path(filename)) {
    return;
  }

  // read past filesize
  memset(buffer, 0, 1024);
  CHECK(TileDBUtils::read_file(filename, 256, buffer, 1024) == TILEDB_ERR);

  // read non-existent file
  CHECK(TileDBUtils::read_entire_file(filename+".1", &read_buffer, &length) == TILEDB_ERR);
  CHECK(TileDBUtils::read_file(filename+".1", 256, buffer, 256) == TILEDB_ERR);
}

TEST_CASE("Test create temp file", "[create_temp_file]") {
  char path[PATH_MAX];
  CHECK(TileDBUtils::create_temp_filename(path, PATH_MAX) == TILEDB_OK);
  CHECK(TileDBUtils::delete_file(path) == TILEDB_OK);
  CHECK(!TileDBUtils::is_file(path));
}

TEST_CASE_METHOD(TempDir, "Test move across filesystems", "[move_across_filesystems]") {
  std::string filename = get_temp_dir()+"/"+"test_file";
  char buffer[1024];
  memset(buffer, 'H', 1024);
  CHECK(TileDBUtils::write_file(filename, buffer, 1024) == TILEDB_OK);
  CHECK(TileDBUtils::is_file(filename));

  char path[PATH_MAX];
  CHECK(TileDBUtils::create_temp_filename(path, PATH_MAX) == TILEDB_OK);

  CHECK(TileDBUtils::move_across_filesystems(filename, path) == TILEDB_OK);
  CHECK(TileDBUtils::is_file(path));
  CHECK(TileDBUtils::is_file(filename)); // src should not be deleted!

  CHECK(TileDBUtils::delete_file(path) == TILEDB_OK);
  CHECK(TileDBUtils::delete_file(filename) == TILEDB_OK);

  CHECK(!TileDBUtils::is_file(path));
  CHECK(!TileDBUtils::is_file(filename));
}

