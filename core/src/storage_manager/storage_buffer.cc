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
#include "utils.h"

#include <assert.h>
#include <iostream>
#include <string>
#include <string.h>

#define BUFFER_PATH_ERROR(MSG, PATH) free_buffer();SYSTEM_ERROR(TILEDB_BF_ERRMSG, MSG, PATH, tiledb_fs_errmsg)
#define BUFFER_ERROR_WITH_ERRNO(MSG) free_buffer();TILEDB_ERROR_WITH_ERRNO(TILEDB_BF_ERRMSG, MSG, tiledb_fs_errmsg)
#define BUFFER_ERROR(MSG) free_buffer();TILEDB_ERROR(TILEDB_BF_ERRMSG, MSG, tiledb_fs_errmsg)

// Using a typical page size for chunks
#define CHUNK 4096

StorageBuffer::StorageBuffer(StorageFS *fs, const std::string& filename, const bool is_read) :
    fs_(fs), filename_(filename), read_only_(is_read) {
  if (read_only_) {
    if (fs_->file_size(filename) == TILEDB_FS_ERR) {
      BUFFER_PATH_ERROR("File does not seem to exist or is of zero length", filename_);
      is_error_ = true;
    } else {
      filesize_ = (size_t)fs_->file_size(filename);
    }
    if (!(chunk_size_ = fs->get_download_buffer_size())) {
      BUFFER_PATH_ERROR("Cannot perform buffered reads as there is no download buffer size set", filename_);
      is_error_ = true;
    }
  } else {
    if (!(chunk_size_ = fs->get_upload_buffer_size())) {
      BUFFER_PATH_ERROR("Cannot perform buffered writes as there is no upload buffer size set", filename_);
      is_error_ = true;
    }
  }
}

int StorageBuffer::read_buffer(void *bytes, size_t size) {
  // Nothing to do
  if (bytes == NULL || size == 0) {
    return TILEDB_BF_OK;
  }
  int rc = read_buffer(file_offset_, bytes, size);
  file_offset_ += size;
  return rc;
}

int StorageBuffer::read_buffer(off_t offset, void *bytes, size_t size) {
  // Nothing to do
  if (bytes == NULL || size == 0) {
    return TILEDB_BF_OK;
  }

  assert(read_only_);
  if (is_error_) {
    return TILEDB_BF_ERR;
  }

  if (offset + size > filesize_) {
    BUFFER_PATH_ERROR("Cannot read past the filesize from buffer", filename_);
    return TILEDB_BF_ERR;  
  }

  if (buffer_ == NULL || !(offset>=buffer_offset_ && (offset+size)<=(buffer_offset_+buffer_size_))) {
    buffer_offset_ = (offset/CHUNK)*CHUNK;
    buffer_size_ = ((size/chunk_size_)+1)*chunk_size_ + (offset%CHUNK);
    // Factor in last chunk
    if (buffer_offset_+buffer_size_ > filesize_) {
      buffer_size_ = filesize_-buffer_offset_;
    }
    if (buffer_size_ > allocated_buffer_size_) {
      buffer_ = realloc(buffer_, buffer_size_);
      if (buffer_ == NULL) {
	BUFFER_ERROR_WITH_ERRNO("Cannot read to buffer; Mem allocation error");
	return TILEDB_BF_ERR;
      }
      allocated_buffer_size_ = buffer_size_;
    }
    if (read_buffer()) {
      return TILEDB_BF_ERR;
    }
  }
  
  assert(offset >= buffer_offset_);
  assert(size <= buffer_size_);
  assert(size_t(offset-buffer_offset_) <= buffer_size_);
  
  void *pmem = memcpy(bytes, (char *)buffer_+offset-buffer_offset_, size);
  assert(pmem == bytes);

  return TILEDB_BF_OK;
}

int StorageBuffer::read_buffer() {
    if (fs_->read_from_file(filename_, buffer_offset_, (char *)buffer_, buffer_size_)) {
      BUFFER_PATH_ERROR("Cannot read to buffer", filename_);
      return TILEDB_BF_ERR;
    }
    return TILEDB_BF_OK;
  }

int StorageBuffer::append_buffer(const void *bytes, size_t size) {
  assert(!read_only_);

  // Nothing to do
  if (bytes == NULL || size == 0) {
    return TILEDB_BF_OK;
  }

  if (is_error_) {
    return TILEDB_BF_ERR;
  }

  if (buffer_size_ >= chunk_size_) {
    assert(buffer_ != NULL);
    if (write_buffer()) {
      return TILEDB_BF_ERR;
    }
  }
  
  if (buffer_ == NULL || buffer_size_+size > allocated_buffer_size_) {
    size_t alloc_size = allocated_buffer_size_ + ((size/CHUNK)+1)*CHUNK;
    buffer_ = realloc(buffer_, alloc_size);
    if (buffer_ == NULL) {
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
  if (fs_->write_to_file(filename_, buffer_, buffer_size_)) {
    BUFFER_PATH_ERROR("Cannot write bytes", filename_);
    return TILEDB_BF_ERR;
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
    free_buffer();
    return TILEDB_BF_ERR;
  } else {
    return TILEDB_BF_OK;
  }
}

int CompressedStorageBuffer::read_buffer(void *bytes, size_t size) {
  // Nothing to do
  if (bytes == NULL || size == 0) {
    return TILEDB_BF_OK;
  }
  assert(read_only_);
  if (is_error_) {
    return TILEDB_BF_ERR;
  }
  if (!buffer_) {
    switch (compression_type_) {
      case TILEDB_GZIP:
        if (gzip_read_buffer()) {
          BUFFER_PATH_ERROR("Cannot decompress and/or read bytes", filename_);
          return TILEDB_BF_ERR;
        }
        break;
      case TILEDB_NO_COMPRESSION:
        break;
      default:
        BUFFER_ERROR("Compression type=" + std::to_string(compression_type_) + " for read_buffer not supported for CompressedStorageBuffer");
        return TILEDB_BF_ERR;
    }
  }
 
  int rc = StorageBuffer::read_buffer(file_offset_, bytes, size);
  file_offset_ += size;
  return rc;
}

int CompressedStorageBuffer::write_buffer() {
  if (buffer_size_ > 0) {
    switch (compression_type_) {
      case TILEDB_GZIP:
        if (gzip_write_buffer()) {
          BUFFER_PATH_ERROR("Cannot compress and/or write bytes", filename_);
          return TILEDB_BF_ERR;
        }
        break;
      case TILEDB_NO_COMPRESSION:
        return StorageBuffer::write_buffer();
      default:
        BUFFER_ERROR("Compression type=" + std::to_string(compression_type_) + " not supported in StorageBuffer");
        return TILEDB_BF_ERR;
    }
  }
  buffer_size_ = 0;
  return TILEDB_BF_OK;
}

#define windowBits 15
#define GZIP_ENCODING 16

void gzip_handle_error(int rc, const std::string& message);

int CompressedStorageBuffer::initialize_gzip_stream(z_stream *strm) {
  memset(strm, 0, sizeof(z_stream));
  strm->zalloc = Z_NULL;
  strm->zfree = Z_NULL;
  strm->opaque = Z_NULL;
  strm->next_in = Z_NULL;
  int rc = TILEDB_BF_OK;
  if ((rc = inflateInit2(strm, (windowBits + 32))) != Z_OK) {
    gzip_handle_error(rc, std::string("Could not initialize decompression for file ")+filename_);
    free_buffer();
    return TILEDB_BF_ERR;
  }
  return rc;
}

int CompressedStorageBuffer::gzip_read_buffer() {
  // gzip_read is done only once for a given file as we need all the bytes to decompress into buffer
  assert(!buffer_);

  /* allocate inflate state */
  z_stream strm;
  if (initialize_gzip_stream(&strm)) return TILEDB_BF_ERR;

  unsigned char *in = (unsigned char *)malloc(chunk_size_);
  if (in == NULL) {
    BUFFER_ERROR_WITH_ERRNO( "Cannot read file into buffer; Mem allocation error for " + filename_
                             + " filesize=" + std::to_string(chunk_size_));
    return TILEDB_BF_ERR;
  }
  unsigned char out[TILEDB_GZIP_CHUNK_SIZE];

  int rc = TILEDB_BF_OK;
  size_t processed = 0;
  do {
    // decompress in chunks until input is exhausted
    memset(in, 0, chunk_size_);
    size_t read_size = filesize_-processed>chunk_size_?chunk_size_:filesize_-processed;
    if (fs_->read_from_file(filename_, processed, in, read_size) == TILEDB_UT_ERR) {
      free(in);
      BUFFER_PATH_ERROR("Could not read from file for decompression", filename_);
      return TILEDB_BF_ERR;
    }

    strm.avail_in = read_size;
    strm.next_in = in;
    // for next iteration, bump processed
    processed += read_size;
    // run inflate() on input until output buffer not full
    unsigned have;
    do {
      strm.avail_out = TILEDB_GZIP_CHUNK_SIZE;
      strm.next_out = out;
      rc = inflate(&strm, Z_NO_FLUSH);
      assert(rc != Z_STREAM_ERROR);  // state not clobbered
      switch (rc) {
        case Z_NEED_DICT:
          rc = Z_DATA_ERROR;     // and fall through
        case Z_DATA_ERROR:
        case Z_STREAM_ERROR:
        case Z_MEM_ERROR:
          free(in);
          inflateEnd(&strm);
          close_file(fs_, filename_);
          gzip_handle_error(rc, std::string("Error encountered during inflate with ")+filename_);
          return TILEDB_BF_ERR;
      }

      have = TILEDB_GZIP_CHUNK_SIZE - strm.avail_out;
      buffer_ =  realloc(buffer_, buffer_size_+have);
      if (buffer_ == NULL) {
        BUFFER_ERROR_WITH_ERRNO("Cannot read to buffer; Mem allocation error");
        return TILEDB_BF_ERR;
      }
      void *pmem = memcpy((char *)buffer_+buffer_size_, out, have);
      assert(pmem);

      buffer_size_ += have;
      if (strm.avail_in && strm.avail_out) {
        inflateEnd(&strm);
        // Start a new read for the start of a new segment
        size_t unprocessed_in = strm.avail_in;
        if (initialize_gzip_stream(&strm)) return TILEDB_BF_ERR;
        strm.avail_in = unprocessed_in;
        strm.next_in = in + (read_size-unprocessed_in);
      }
    } while (strm.avail_out == 0);
  }  while (rc !=  Z_STREAM_END || processed < filesize_);

  // clean up before return
  free(in);
  inflateEnd(&strm);

  // All bytes have been decompressed
  assert(rc == Z_STREAM_END);
  assert(processed == filesize_);

  // adjust filesize_ to decompressed size as the reading will only be from memory now on
  filesize_ = buffer_size_;
  allocated_buffer_size_ = buffer_size_;

  return TILEDB_BF_OK;
}

int CompressedStorageBuffer::gzip_write_buffer() {
  unsigned have;
  z_stream strm;
  unsigned char out[TILEDB_GZIP_CHUNK_SIZE];

  /* allocate deflate state */
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;

  int rc = TILEDB_BF_OK;
  if ((rc = deflateInit2 (&strm, compression_level_, Z_DEFLATED, windowBits | GZIP_ENCODING, 8, Z_DEFAULT_STRATEGY)) != Z_OK) {
    gzip_handle_error(rc, std::string("Could not initialize compression for ")+filename_);
    return TILEDB_BF_ERR;
  }

  /* compress entire buffer */
  strm.avail_in = buffer_size_;
  strm.next_in = (unsigned char *)buffer_;
  size_t processed = 0;
  do {
    strm.avail_out = TILEDB_GZIP_CHUNK_SIZE;
    strm.next_out = out;

    if ((rc = deflate(&strm, Z_FINISH)) == Z_STREAM_ERROR) {
      deflateEnd(&strm);
      BUFFER_PATH_ERROR("Encountered Z_STREAM_ERROR; Could not compress file", filename_);
      return TILEDB_UT_ERR;
    }

    have = TILEDB_GZIP_CHUNK_SIZE - strm.avail_out;
    if (compress_buffer_size_ < processed+have) {
      compress_buffer_ = realloc(compress_buffer_, processed+have);
      if (compress_buffer_ == NULL) {
        deflateEnd(&strm);
        BUFFER_ERROR_WITH_ERRNO("Cannot write to compress buffer; Mem allocation error");
        return TILEDB_BF_ERR;
      }
      compress_buffer_size_ = processed+have;
    }
    void *pmem = memcpy((char *)compress_buffer_+processed, out, have);
    assert(pmem == (char *)compress_buffer_+processed);
    processed += have;
  } while (strm.avail_out == 0);

  deflateEnd(&strm);

  if (rc != Z_STREAM_END || strm.avail_in != 0) { // All input has not been compressed
    BUFFER_PATH_ERROR("All input could not be compressed: deflate error", filename_);
    return TILEDB_BF_ERR;
  }

  if (fs_->write_to_file(filename_, compress_buffer_, processed)) {
    BUFFER_PATH_ERROR("Cannot write bytes", filename_);
    return TILEDB_BF_ERR;
  }

  return TILEDB_BF_OK;
}
