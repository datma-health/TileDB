/**
 * @file  catch.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 Omics Data Automation, Inc.
 * @copyright Copyright (c) 2023 dātma, inc™
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
 * Catch Unit Testing Header
 */

#ifndef __TILEDB_CATCH_H__
#define __TILEDB_CATCH_H__

#if CATCH2_MAJOR_VERSION == 3

#include <catch2/catch_all.hpp>
using Catch::Matchers::Equals;
using Catch::Matchers::Matches;
using Catch::startsWith;
using Catch::endsWith;
using Catch::contains;

using namespace Catch::Clara;

#elif CATCH2_MAJOR_VERSION == 2

#define CATCH_CONFIG_RUNNER
#include <catch2/catch.hpp>
using Catch::Equals;
using Catch::StartsWith;
using Catch::EndsWith;
using Catch::Contains;
using Catch::Matches;

using namespace Catch::clara;

#else
#  error Could not figure out CATCH2_MAJOR_VERSION
#endif

#include "tiledb_utils.h"

#include "regex"

#define CHECK_RC(rc, expected) CHECK(rc == expected)

std::string g_test_dir = "";
std::string g_benchmark_config = std::string(TILEDB_TEST_DIR)+"/inputs/benchmark.config";

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
    if(path[path.size()]!='/') {
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
      std::string temp;
      // initialize random seed
      srand(time(NULL));
      for (auto i=0; i<6; i++) temp = temp+char(rand()%26+'a');
      tmp_dirname_ = TileDBUtils::append_path(
          g_test_dir, std::regex_replace(dirname_pattern, std::regex("XXXXXX"), temp));
      if (!TileDBUtils::is_dir(g_test_dir)) {
        REQUIRE(TileDBUtils::create_dir(g_test_dir) == 0);
        delete_test_dir_in_destructor_ = g_test_dir;
      }
      if (!TileDBUtils::is_dir(tmp_dirname_)) {
        CHECK(TileDBUtils::create_dir(tmp_dirname_) == TILEDB_OK);
      }
    }
  }
};

const std::string get_test_dir() {
  return g_test_dir;
}
std::string append(const std::string &temp_dir, const std::string &append) {
  std::size_t find = temp_dir.find('?');
  if (find == std::string::npos)
    return temp_dir + append;
  else {
    return temp_dir.substr(0, find) + append + temp_dir.substr(find);
  }
}

int main( int argc, char* argv[] )
{
  Catch::Session session; // There must be exactly one instance

  // Build a new parser on top of Catch's
  auto cli
    = session.cli() // Get Catch's composite command line parser
      | Opt( g_test_dir, "test-dir-url" ) // bind variable to a new option, with a hint string
      ["--test-dir"] // the option names it will respond to
      ("Specify test dir, default is $TMPDIR if not specified") // description string for the help output
      | Opt ( g_benchmark_config, "/path/to/TileDB/test/inputs/benchmark.config")
      ["--benchmark-config"]
      ("Specify config for benchmmarking, default is /path/to/TileDB/test/inputs/benchmark.config");

  // Now pass the new composite back to Catch so it uses that
  session.cli(cli);

  // Let Catch (using Clara) parse the command line
  int rc = session.applyCommandLine( argc, argv );
  if(rc != 0) // Indicates a command line error
    return rc;

  return session.run();
}

#endif // __TILEDB_CATCH_H__
