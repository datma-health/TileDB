/**
 * @file storage_buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 Omics Data Automation Inc.
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
 * This file implements the StorageBuffer class that buffers writes/reads to/from files
 */

#include "storage_buffer.h"

#include <assert.h>
#include <iostream>
#include <string>
#include <string.h>

#ifdef TILEDB_VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_BF_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif

int StorageBuffer::read_buffer(off_t offset, void *bytes, size_t size) {
    if (bytes == NULL) {
    std::string errmsg = "Arguments not specified correctly";
    PRINT_ERROR(errmsg);
    tiledb_bf_errmsg = TILEDB_BF_ERRMSG + errmsg;
    return TILEDB_BF_ERR;
  }

  // Nothing to do
  if (size == 0) {
    return TILEDB_BF_OK;
  }

  if (buffer == NULL && fs_ == NULL) {
    std::string errmsg = "Buffer is null, may not have been initialized correctly";
    PRINT_ERROR(errmsg);
    tiledb_bf_errmsg = TILEDB_BF_ERRMSG + errmsg;
    return TILEDB_BF_ERR;
  }

  size_t filesize = fs_->file_size(filename_);
  size_t chunk_size = fs_->get_download_buffer_size();
  if (buffer == NULL) {
    buffer = malloc(filesize);
    if (buffer == NULL) {
      std::string errmsg = "Cannot read to buffer; Mem allocation error";
      PRINT_ERROR(errmsg);
      tiledb_bf_errmsg = TILEDB_BF_ERRMSG + errmsg;
      return TILEDB_BF_ERR;
    }
    num_blocks_ = filesize/chunk_size+1;
    blocks_read_.resize(num_blocks_);
    std::fill(blocks_read_.begin(), blocks_read_.end(), false);
  }

  if (offset + size > filesize) {
    std::string errmsg = "Cannot read past the filesize from buffer; ";
    PRINT_ERROR(errmsg);
    tiledb_bf_errmsg = TILEDB_BF_ERRMSG + errmsg;
    return TILEDB_BF_ERR;  
  }

  for (auto i=offset/chunk_size; i<=(offset+size)/chunk_size; i++) {
    assert(i < blocks_read_.size());
    if (!blocks_read_[i]) {
      fs_->read_from_file(filename_, i*chunk_size, (char *)buffer+i*chunk_size, i==(num_blocks_-1)?filesize%chunk_size:chunk_size);
      blocks_read_[i] = true;
    }
  }

  void *pmem = memcpy(bytes, (char *)buffer+offset, size);
  assert(pmem == bytes);

  buffer_offset = offset + size;
  
  return TILEDB_BF_OK;
}

#define CHUNK 1024
int StorageBuffer::append_buffer(const void *bytes, size_t size) {
  if (read_only) {
    std::string errmsg = "Cannot append buffer to read-only buffers";
    PRINT_ERROR(errmsg);
    tiledb_bf_errmsg = TILEDB_BF_ERRMSG + errmsg;
    return TILEDB_BF_ERR;
  }

  if (bytes == NULL) {
    std::string errmsg = "Arguments not specified correctly";
    PRINT_ERROR(errmsg);
    tiledb_bf_errmsg = TILEDB_BF_ERRMSG + errmsg;
    return TILEDB_BF_ERR;
  }

  if (size == 0) {
    return TILEDB_BF_OK;
  }

  size_t chunk_size = fs_->get_upload_buffer_size();

  if (buffer_size >= chunk_size) {
    int rc = write_buffer();
    if (rc) {
      return rc;
    }
  }
  
  if (buffer == NULL || buffer_size+size > allocated_buffer_size) {
    int64_t alloc_size = allocated_buffer_size + ((size/CHUNK)+1)*CHUNK;
    buffer = realloc(buffer, alloc_size);
    if (buffer == NULL) {
      free_buffer();
      std::string errmsg = "Cannot write to buffer; Mem allocation error";
      PRINT_ERROR(errmsg);
      tiledb_bf_errmsg = TILEDB_BF_ERRMSG + errmsg;
      return TILEDB_BF_ERR;
    }
    allocated_buffer_size = alloc_size;
  }

  void *pmem = memcpy((char *)buffer+buffer_size, bytes, size);
  assert(pmem == (char *)buffer+buffer_size);
  
  buffer_size += size;

  return TILEDB_BF_OK;
}

int StorageBuffer::write_buffer() {
  if (buffer_size > 0) {
    if (fs_->write_to_file(filename_, buffer, buffer_size)) {
      std::string errmsg = "Cannot write bytes for file=" + filename_;
      PRINT_ERROR(errmsg);
      tiledb_bf_errmsg = TILEDB_BF_ERRMSG + errmsg;
      return TILEDB_BF_ERR;
    }
  }
  buffer_size = 0;
  return TILEDB_BF_OK;
}

int StorageBuffer::finalize() {
  int rc = TILEDB_BF_OK;
  if (!read_only) {
    rc =  write_buffer();
    fs_->close_file(filename_);
  }
  return rc;
}
