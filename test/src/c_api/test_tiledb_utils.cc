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

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "tiledb.h"
#include "tiledb_storage.h"
#include "tiledb_utils.h"

#include <string.h>
#include <fcntl.h>

const std::string& workspace("WORKSPACE");
std::string g_test_dir = "";

class TempDir {
 public:
  TempDir() {
    create_temp_directory();
  }

  ~TempDir() {
    TileDBUtils::delete_dir(get_temp_dir());

    if (!delete_test_dir_in_destructor_.empty()) {
      TileDBUtils::delete_dir(delete_test_dir_in_destructor_);
    }
  }

  const std::string& get_temp_dir() {
    return tmp_dirname_;
  }
 private:
  std::string tmp_dirname_;
  std::string delete_test_dir_in_destructor_;

  std::string get_pathname(std::string  path) {
    const size_t last_slash_idx = path.rfind('/');
    if (last_slash_idx != std::string::npos) {
      return path.substr(last_slash_idx+1);
    } else {
      return path;
    }
  }
  std::string append_slash(std::string path) {
    if (path[path.size()]!='/') {
      return path+"/";
    } else {
      return path;
    }
  }
  void create_temp_directory() {
    std::string dirname_pattern("TileDBTestXXXXXX");
    if (g_test_dir.empty()) { // Posix Case. Use mkdtemp() here
      const char *tmp_dir = getenv("TMPDIR");
      if (tmp_dir == NULL) {
        tmp_dir = P_tmpdir; // defined in stdio
      }
      assert(tmp_dir != NULL);
      tmp_dirname_ = mkdtemp(const_cast<char *>((append_slash(tmp_dir)+dirname_pattern).c_str()));
    } else {
      tmp_dirname_ = append_slash(g_test_dir)+mktemp(const_cast<char *>(dirname_pattern.c_str()));
      if (!TileDBUtils::is_dir(g_test_dir)) {
        CHECK(TileDBUtils::create_dir(g_test_dir) == 0);
        delete_test_dir_in_destructor_ = g_test_dir;
      }
      if (!TileDBUtils::is_dir(tmp_dirname_)) {
        CHECK(TileDBUtils::create_dir(tmp_dirname_) == TILEDB_OK);
      }
    }
  }
};

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

int main( int argc, char* argv[] )
{
  Catch::Session session; // There must be exactly one instance

  // Build a new parser on top of Catch's
  using namespace Catch::clara;
  auto cli
    = session.cli() // Get Catch's composite command line parser
    | Opt( g_test_dir, "Specify test dir, default is $TMPDIR" ) // bind variable to a new option, with a hint string
     ["--test-dir"] // the option names it will respond to
    ("Specify test dir, default is $TMPDIR if not specified");        // description string for the help output

  // Now pass the new composite back to Catch so it uses that
  session.cli(cli);

  // Let Catch (using Clara) parse the command line
  int rc = session.applyCommandLine( argc, argv );
  if(rc != 0) // Indicates a command line error
    return rc;

  return session.run();
}
