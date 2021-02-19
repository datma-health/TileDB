/**
 * @file storage_buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2021 Omics Data Automation Inc.
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

#include "error.h"
#include "storage_buffer.h"

#include <assert.h>
#include <iostream>
#include <string>
#include <string.h>

#define BUFFER_PATH_ERROR(MSG, PATH) SYSTEM_ERROR(TILEDB_FS_ERRMSG, MSG, PATH, tiledb_fs_errmsg)
#define BUFFER_ERROR_WITH_ERRNO(MSG) TILEDB_ERROR_WITH_ERRNO(TILEDB_FS_ERRMSG, MSG, tiledb_fs_errmsg)
#define BUFFER_ERROR(MSG) TILEDB_ERROR(TILEDB_FS_ERRMSG, MSG, tiledb_fs_errmsg)

int StorageBuffer::read_buffer(off_t offset, void *bytes, size_t size) {
  // Nothing to do
  if (bytes == NULL || size == 0) {
    return TILEDB_BF_OK;
  }

  size_t filesize = fs_->file_size(filename_);
  size_t chunk_size = fs_->get_download_buffer_size();
  if (offset + size > filesize) {
    BUFFER_PATH_ERROR("Cannot read past the filesize from buffer", filename_);
    return TILEDB_BF_ERR;  
  }

  if (buffer_ == NULL) {
    num_blocks_ = filesize/chunk_size+1;
    blocks_read_.resize(num_blocks_);
    std::fill(blocks_read_.begin(), blocks_read_.end(), false);
  }

  if (buffer_ == NULL || !(offset>=buffer_offset_ && size<=buffer_size_)) {
    buffer_offset_ = offset - offset%chunk_size;
    buffer_size_ = ((buffer_offset_%chunk_size+size)/chunk_size+1)*chunk_size;
    // Factor in last chunk
    if (buffer_offset_+buffer_size_ > filesize) {
      buffer_size_ = filesize-buffer_offset_;
    }
    if (buffer_size_ > allocated_buffer_size_) {
      buffer_ = realloc(buffer_, buffer_size_);
      if (buffer_ == NULL) {
	BUFFER_ERROR_WITH_ERRNO("Cannot read to buffer; Mem allocation error");
	return TILEDB_BF_ERR;
      }
      allocated_buffer_size_ = buffer_size_;
    }
    
    if (fs_->read_from_file(filename_, buffer_offset_, (char *)buffer_, buffer_size_)) {
      free_buffer();
      BUFFER_PATH_ERROR("Cannot read to buffer", filename_);
      return TILEDB_BF_ERR;
    }
  }
  
  assert(offset >= buffer_offset_);
  assert(size <= buffer_size_);
  
  void *pmem = memcpy(bytes, (char *)buffer_+offset-buffer_offset_, size);
  assert(pmem == bytes);

  return TILEDB_BF_OK;
}

#define CHUNK 1024
int StorageBuffer::append_buffer(const void *bytes, size_t size) {
  if (read_only_) {
    BUFFER_ERROR("Cannot append buffer to read-only buffers");
    return TILEDB_BF_ERR;
  }

  // Nothing to do
  if (bytes == NULL || size == 0) {
    return TILEDB_BF_OK;
  }

  size_t chunk_size = fs_->get_upload_buffer_size();

  if (buffer_size_ >= chunk_size) {
    assert(buffer_ != NULL);
    int rc = write_buffer();
    if (rc) {
      return rc;
    }
  }
  
  if (buffer_ == NULL || buffer_size_+size > allocated_buffer_size_) {
    size_t alloc_size = allocated_buffer_size_ + ((size/CHUNK)+1)*CHUNK;
    buffer_ = realloc(buffer_, alloc_size);
    if (buffer_ == NULL) {
      free_buffer();
      BUFFER_ERROR_WITH_ERRNO("Cannot write to buffer; Mem allocation error");
      return TILEDB_BF_ERR;
    }
    allocated_buffer_size_ = alloc_size;
  }

  void *pmem = memcpy((char *)buffer_+buffer_size_, bytes, size);
  assert(pmem == (char *)buffer_+buffer_size_);
  
  buffer_size_ += size;

  return TILEDB_BF_OK;
}

int StorageBuffer::write_buffer() {
  if (buffer_size_ > 0) {
    if (fs_->write_to_file(filename_, buffer_, buffer_size_)) {
      free_buffer();
      BUFFER_PATH_ERROR("Cannot write bytes", filename_);
      return TILEDB_BF_ERR;
    }
  }
  buffer_size_ = 0;
  return TILEDB_BF_OK;
}

int StorageBuffer::finalize() {
  int rc = TILEDB_BF_OK;
  if (!read_only_) {
    rc =  write_buffer();
  }
  rc = fs_->close_file(filename_) || rc;
  if (rc) {
    // error is logged in write_buffer or close_file
    return TILEDB_BF_ERR;
  } else {
    return TILEDB_BF_OK;
  }
}
