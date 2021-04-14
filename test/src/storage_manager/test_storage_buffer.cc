/**
 * @file   test_storage_buffer.cc
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
 * Tests for the StorageBuffer class
 */

#include "catch.h"
#include "storage_buffer.h"
#include "storage_posixfs.h"

TEST_CASE_METHOD(TempDir, "Test Storage Buffer Basic" "[basic]") {
  std::string filename = get_temp_dir()+"/test-file";
  std::vector<char> buffer(10);
  std::generate(buffer.begin(), buffer.end(), std::rand);
  
  PosixFS fs;
  StorageBuffer *storage_buffer = new StorageBuffer(&fs, filename);
  CHECK_RC(storage_buffer->append_buffer(buffer.data(), 10), TILEDB_BF_ERR); // No upload buffer size set
  fs.upload_buffer_size_ = 100;
  CHECK_RC(storage_buffer->append_buffer(buffer.data(), 10), TILEDB_BF_OK);
  CHECK_RC(storage_buffer->finalize(), TILEDB_BF_OK);
  delete storage_buffer;

  std::vector<char> check_buffer(10);
  CHECK(fs.file_size(filename) == 10);
  CHECK_RC(fs.read_from_file(filename, 0, check_buffer.data(), 10), TILEDB_FS_OK);
  CHECK(memcmp(buffer.data(), check_buffer.data(), 10) == 0);

  std::vector<char> read_buffer(10);  
  storage_buffer = new StorageBuffer(&fs, filename+"-non-existent", /*is_read*/true);
  CHECK_RC(storage_buffer->append_buffer(buffer.data(), 10), TILEDB_BF_ERR); // Cannot write in read_only mode
  CHECK_RC(storage_buffer->read_buffer(0, read_buffer.data(), 10), TILEDB_BF_ERR); // Non existent file
  delete storage_buffer;

  storage_buffer = new StorageBuffer(&fs, filename, /*is_read*/true);
  CHECK_RC(storage_buffer->read_buffer(0, read_buffer.data(), 10), TILEDB_BF_ERR); // No download buffer size
  fs.download_buffer_size_ = 100;
  CHECK_RC(storage_buffer->read_buffer(0, read_buffer.data(), 10), TILEDB_BF_OK);
  CHECK(memcmp(buffer.data(), read_buffer.data(), 10) == 0);
  CHECK_RC(storage_buffer->read_buffer(0, read_buffer.data(), 100), TILEDB_BF_ERR); // Reading past file
  delete storage_buffer;
}

TEST_CASE_METHOD(TempDir, "Test Storage Buffer with buffered reading/writing of large files", "[large-file]") {
  size_t size = 1024*1024*10; // 10M
  std::vector<char> buffer(size);
  std::generate(buffer.begin(), buffer.end(), std::rand);

  PosixFS fs;
  fs.upload_buffer_size_ = 1024*1024; // 1M chunk
  fs.download_buffer_size_ = 1024*512; // 0.5M chunk

  std::string filename = get_temp_dir()+"/buffered_file";

  // Buffered write
  StorageBuffer storage_buffer(&fs, filename, /*is_read*/false);
  size_t unprocessed = size;
  do {
    size_t bytes_to_process = unprocessed<1024?unprocessed:std::rand()%1024;
    CHECK_RC(storage_buffer.append_buffer(buffer.data()+size-unprocessed, bytes_to_process), TILEDB_BF_OK);
    unprocessed -= bytes_to_process;
  } while(unprocessed);
  CHECK_RC(storage_buffer.finalize(), TILEDB_BF_OK);
  CHECK(fs.file_size(filename) == size);

  // Check buffered write results with unbuffered read
  std::vector<char> read_buffer(size);
  CHECK_RC(fs.read_from_file(filename, 0, read_buffer.data(), size), TILEDB_BF_OK);
  CHECK_RC(fs.close_file(filename), TILEDB_BF_OK);
  CHECK(memcmp(buffer.data(), read_buffer.data(), size) == 0);

  // Buffered read
  StorageBuffer read_storage_buffer(&fs, filename, /*is_read*/true);
  unprocessed = size;
  memset(read_buffer.data(), 0, size);
  unprocessed = size;
  do {
    CHECK(size-unprocessed >= 0);
    size_t bytes_to_process = unprocessed<1024?unprocessed:std::rand()%1024;
    CHECK_RC(read_storage_buffer.read_buffer(size-unprocessed, read_buffer.data()+size-unprocessed, bytes_to_process), TILEDB_BF_OK);
    unprocessed -= bytes_to_process;
  } while(unprocessed);
  CHECK_RC(read_storage_buffer.finalize(), TILEDB_BF_OK);
  CHECK(memcmp(buffer.data(), read_buffer.data(), size) == 0);
}

