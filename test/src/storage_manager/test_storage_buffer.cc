/**
 * @file   test_storage_buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2022 Omics Data Automation, Inc.
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
 * Tests for the StorageBuffer class
 */

#include "catch.h"

#include "storage_azure_blob.h"
#include "storage_buffer.h"
#include "storage_gcs.h"
#include "storage_posixfs.h"
#include "storage_s3.h"
#include "utils.h"

TEST_CASE_METHOD(TempDir, "Test Storage Buffer Basic", "[basic]") {
  if (get_temp_dir().find("://") != std::string::npos) {
    return;
  }

  std::string filename = get_temp_dir()+"/test-file";
  std::vector<char> buffer(10);
  std::generate(buffer.begin(), buffer.end(), std::rand);

  size_t chunk_size = 0;
  
  PosixFS fs;
  StorageBuffer storage_buffer(&fs, filename, chunk_size);
  CHECK_RC(storage_buffer.append_buffer(buffer.data(), 10), TILEDB_BF_ERR); // No upload buffer size set

  chunk_size = 100;
  StorageBuffer storage_buffer1(&fs, filename, chunk_size);
  CHECK_RC(storage_buffer1.append_buffer(buffer.data(), 10), TILEDB_BF_OK);
  CHECK_RC(storage_buffer1.finalize(), TILEDB_BF_OK);

  std::vector<char> check_buffer(10);
  CHECK(fs.file_size(filename) == 10);
  CHECK_RC(fs.read_from_file(filename, 0, check_buffer.data(), 10), TILEDB_FS_OK);
  CHECK(memcmp(buffer.data(), check_buffer.data(), 10) == 0);

  std::vector<char> read_buffer(10);  
  StorageBuffer storage_buffer2(&fs, filename+"-non-existent", chunk_size, /*is_read*/true);
  CHECK_RC(storage_buffer2.read_buffer(0, read_buffer.data(), 10), TILEDB_BF_ERR); // Non existent file

  StorageBuffer storage_buffer3(&fs, filename, 0, /*is_read*/true);
  CHECK_RC(storage_buffer3.read_buffer(0, read_buffer.data(), 10), TILEDB_BF_ERR); // No download buffer size

  chunk_size = 100;
  StorageBuffer storage_buffer4(&fs, filename, chunk_size, /*is_read*/true);
  CHECK_RC(storage_buffer4.read_buffer(0, read_buffer.data(), 10), TILEDB_BF_OK);
  CHECK(memcmp(buffer.data(), read_buffer.data(), 10) == 0);
  CHECK_RC(storage_buffer4.read_buffer(0, read_buffer.data(), 100), TILEDB_BF_ERR); // Reading past file
}

int write_to_file_after_compression(StorageFS *fs, const std::string& filename, const char *str, size_t size,
                                     int compression_type) {
  size_t chunk_size = 1024;
  CompressedStorageBuffer *buffer = new CompressedStorageBuffer(fs, filename, chunk_size, false, compression_type);
  if (buffer->append_buffer(str, size)) {
    delete buffer;
    return TILEDB_ERR;
  }
  if (buffer->finalize()) {
    delete buffer;
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

int read_from_file_after_decompression(StorageFS *fs, const std::string& filename,
                                       void *bytes, size_t length, int compression_type) {
  size_t chunk_size = 1024;
  CompressedStorageBuffer *buffer = new CompressedStorageBuffer(fs, filename, chunk_size, true, compression_type);
  if (buffer->read_buffer(bytes, length)) {
    delete buffer;
    return TILEDB_ERR;
  }
  int rc = buffer->finalize();
  delete buffer;
  return rc;
}

TEST_CASE_METHOD(TempDir, "Test Storage Buffer with compression", "[compr]") {
  if (get_temp_dir().find("://") != std::string::npos) {
    return;
  }

  PosixFS test_fs;
  PosixFS *fs = &test_fs;
  char *buffer = NULL;
  size_t buffer_length = 0;

  CHECK(write_to_file_after_compression(fs, "/non-existent-file", "Hello", 6, TILEDB_NO_COMPRESSION) == TILEDB_ERR);
  CHECK(write_to_file_after_compression(fs, "/non-existent-file", "Hello", 6, TILEDB_GZIP) == TILEDB_ERR);
  // OK since buffer and/or buffer_length is 0 and nothing to do
  CHECK(read_from_file_after_decompression(fs, "/non-existent-file", buffer, 30, TILEDB_NO_COMPRESSION) == TILEDB_OK); 
  CHECK(read_from_file_after_decompression(fs, "/non-existent-file", buffer, buffer_length, TILEDB_GZIP) == TILEDB_OK);

  buffer=(char *)malloc(10);
  buffer_length = 6; // length of "Hello"
  CHECK(read_from_file_after_decompression(fs, "/non-existent-file", buffer, 30, TILEDB_NO_COMPRESSION) == TILEDB_ERR); 
  CHECK(read_from_file_after_decompression(fs, "/non-existent-file", buffer, buffer_length, TILEDB_GZIP) == TILEDB_ERR);

  const std::string compressed_file = get_temp_dir()+"/compressed_foo";
  CHECK(write_to_file_after_compression(fs, "/non-existent-file", "Hello", 6, 1000) == TILEDB_ERR); // Unsupported type
  REQUIRE(!write_to_file_after_compression(fs, compressed_file, "Hello", 6, TILEDB_GZIP));

  CHECK(read_from_file_after_decompression(fs, compressed_file, buffer, buffer_length, 1000) == TILEDB_ERR); // Unsupported type
  CHECK(!read_from_file_after_decompression(fs, compressed_file, buffer, buffer_length, TILEDB_GZIP));
  CHECK(buffer_length == 6);
  CHECK(!strcmp(buffer, "Hello"));
  CHECK(read_from_file_after_decompression(fs, compressed_file, buffer, 10, TILEDB_GZIP) == TILEDB_ERR); // Reading past decompressed filesize
  free(buffer);
}

class TestBufferedWrite : public TempDir {
 public:
  void write(const std::string& filename, StorageBuffer *storage_buffer, char *buffer, size_t size) {
     size_t unprocessed = size;
     do {
       size_t bytes_to_process = unprocessed<1024?unprocessed:std::rand()%1024;
       CHECK_RC(storage_buffer->append_buffer(buffer+size-unprocessed, bytes_to_process), TILEDB_BF_OK);
       unprocessed -= bytes_to_process;
     } while(unprocessed);
     CHECK_RC(storage_buffer->finalize(), TILEDB_BF_OK);
  }

  void read(const std::string& filename, StorageBuffer *storage_buffer, char *buffer, size_t size) {
    size_t unprocessed = size;
    do {
      CHECK(size-unprocessed >= 0);
      size_t bytes_to_process = unprocessed<1024?unprocessed:std::rand()%1024;
      CHECK_RC(storage_buffer->read_buffer(size-unprocessed, buffer+size-unprocessed, bytes_to_process), TILEDB_BF_OK);
      unprocessed -= bytes_to_process;
    } while(unprocessed > 0);
    CHECK_RC(storage_buffer->finalize(), TILEDB_BF_OK);
  }

  void read_with_implicit_offset(const std::string& filename, CompressedStorageBuffer *storage_buffer, char *buffer, size_t size) {
    size_t unprocessed = size;
    do {
      CHECK(size-unprocessed >= 0);
      size_t bytes_to_process = unprocessed<1024?unprocessed:std::rand()%1024;
      CHECK_RC(storage_buffer->read_buffer(buffer+size-unprocessed, bytes_to_process), TILEDB_BF_OK);
      unprocessed -= bytes_to_process;
    } while(unprocessed > 0);
    CHECK_RC(storage_buffer->finalize(), TILEDB_BF_OK);
  }
};

TEST_CASE_METHOD(TestBufferedWrite, "Test Storage Buffer with buffered reading/writing of large files", "[large-file]") {
  size_t size = 1024*1024*20 + 1024; // 20M + 1024
  std::vector<char> buffer(size);
  std::generate(buffer.begin(), buffer.end(), std::rand);

  std::shared_ptr<StorageFS> fs;
  bool is_posix = false;

  // Buffered write
  std::string filename = get_temp_dir()+"/buffered_file";
  if (is_gcs_path(filename)) {
    fs = std::make_shared<GCS>(filename);
  } else if (is_azure_blob_storage_path(filename)) {
    fs = std::make_shared<AzureBlob>(filename);
  } else if (is_s3_storage_path(filename)) {
    fs = std::make_shared<S3>(filename);
  } else {
    fs = std::make_shared<PosixFS>();
    is_posix = true;
  }

  auto write_chunk_size = fs->get_upload_buffer_size()?fs->get_upload_buffer_size():10240; // default chunk
  auto read_chunk_size = fs->get_download_buffer_size()?fs->get_download_buffer_size():5120; // default chunk

  // Buffered write
  StorageBuffer storage_buffer(fs.get(), filename, write_chunk_size, /*is_read*/false);
  write(filename, &storage_buffer, buffer.data(), size);
  CHECK((size_t)fs->file_size(filename) == size);

  // Check buffered write results with unbuffered read
  std::vector<char> read_buffer(size);
  CHECK_RC(fs->read_from_file(filename, 0, read_buffer.data(), size), TILEDB_BF_OK);
  CHECK_RC(fs->close_file(filename), TILEDB_BF_OK);
  CHECK(memcmp(buffer.data(), read_buffer.data(), size) == 0);

  // Buffered read
  StorageBuffer read_storage_buffer(fs.get(), filename, read_chunk_size, /*is_read*/true);
  memset(read_buffer.data(), 0, size);
  read(filename, &read_storage_buffer, read_buffer.data(), size);
  CHECK(memcmp(buffer.data(), read_buffer.data(), size) == 0);

  // Buffered read with implicit offset
  CompressedStorageBuffer read_storage_buffer_1(fs.get(), filename, write_chunk_size, /*is_read*/true);
  memset(read_buffer.data(), 0, size);
  read_with_implicit_offset(filename, &read_storage_buffer_1, read_buffer.data(), size);
  CHECK(memcmp(buffer.data(), read_buffer.data(), size) == 0);

  // Buffered write with compression
  filename += ".compress";
  CompressedStorageBuffer storage_buffer_with_compression(fs.get(), filename, write_chunk_size, false, TILEDB_GZIP, TILEDB_COMPRESSION_LEVEL_GZIP);
  write(filename, &storage_buffer_with_compression, buffer.data(), size);

  // Buffered read with decompression
  CompressedStorageBuffer read_storage_buffer_with_compression(fs.get(), filename, read_chunk_size, true, TILEDB_GZIP);
  memset(read_buffer.data(), 0, size);
  read_with_implicit_offset(filename, &read_storage_buffer_with_compression, read_buffer.data(), size);
  CHECK(memcmp(buffer.data(), read_buffer.data(), size) == 0);

  // Buffered write with compression and buffer set to all one's
  filename += ".simple";
  memset(buffer.data(), 'R', size);
  if (is_posix) {
    // This will test out all paths to write out compressed bytes even for Posix
    fs->set_upload_buffer_size(write_chunk_size*2);
  }
  CompressedStorageBuffer simple_storage_buffer_with_compression(fs.get(), filename, write_chunk_size, false, TILEDB_GZIP, TILEDB_COMPRESSION_LEVEL_GZIP);
  write(filename, &simple_storage_buffer_with_compression, buffer.data(), size);

  // Buffer read with decompression for buffer set to one's
  CompressedStorageBuffer read_simple_storage_buffer_with_compression(fs.get(), filename, read_chunk_size, true, TILEDB_GZIP);
  memset(read_buffer.data(), 0, size);
  read_with_implicit_offset(filename, &read_simple_storage_buffer_with_compression, read_buffer.data(), size);
  CHECK(memcmp(buffer.data(), read_buffer.data(), size) == 0);

  
}


