/**
 * @file storage_buffer.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2022 Omics Data Automation Inc.
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
 * This file defines the StorageBuffer class that buffers writes/reads to/from files
 */

#pragma once

#include "storage_fs.h"
#include "storage_posixfs.h"
#include "tiledb_constants.h"

#include <memory>
#include <zlib.h>

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_BF_OK        0
#define TILEDB_BF_ERR      -1
/**@}*/

/** Default error message. */
#define TILEDB_BF_ERRMSG std::string("[TileDB::StorageBuffer] Error: ")

/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Stores potential error messages. */
extern std::string tiledb_bf_errmsg;

class StorageBuffer {
 public:
  /**
   * Constructor that accepts StorageFS and the filename minimally. StorageBuffer is a no-op
   * if the upload/download limits are not set in StorageFS.
   */
  StorageBuffer(StorageFS *fs, const std::string& filename, size_t chunk_size, const bool is_read=false);

  virtual ~StorageBuffer() {
    free_buffer();
  }

  /**
   * Reads data from the cached buffer with file offsets maintained implicitly.
   * If the cached buffer does not contain the segment, its read from the file into
   * the cache. The size of the cache is guarded by StorageFS download size limit.
   * @param bytes The buffer into which the data will be written.
   * @param size The size of the data to be read from the cached buffer.
   */
  virtual int read_buffer(void *bytes, size_t size);

  /**
   * Reads the data from the cached buffer from the offset into bytes.
   * If the cached buffer does not contain the segment for the offset and size,
   * its read from the file into the cache. The size of the cache is guarded by
   * StorageFS download size limit.
   * @param offset The offset from which the data in the buffer will be read from.
   * @param bytes The buffer into which the data will be written.
   * @param size The size of the data to be read from the cached buffer.
   */
  int read_buffer(off_t offset, void *bytes, size_t size);

  /**
   * Appends given data from the buffer backed by bytes into the file. The buffer will
   * actually be written out only after reaching the StorageFS upload size limit. Ownership
   * of the buffer is relinquished to the caller on return.
   * @param bytes The buffer from which data will be read to append to file
   */
  int append_buffer(const void *bytes, size_t size);

  /**
   * Persist bytes in the buffer to the FileSystem. It is the responsibilty of the client to
   * check upload file size thresholds for Cloud Storage as only the last block to be uploaded 
   * can be lower than the threshold. See
   *     https://cloud.google.com/storage/docs/performing-resumable-uploads#chunked-uploads
   *     https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html
   * Currently used internally only with CompressedStorageBuffer.
   */
  inline int flush() {
    return read_only_?TILEDB_FS_OK:write_buffer();
  }

  /**
   * Finalize flushes existing buffers and releases any allocated memory.
   */
  virtual int finalize();
  
 protected:
  void *buffer_ = NULL;
  size_t buffer_size_ = 0;
  off_t buffer_offset_ = 0;
  size_t allocated_buffer_size_ = 0;

  StorageFS *fs_ = NULL;
  const std::string filename_;
  size_t filesize_ = -1; // Relevant for only read_only
  size_t file_offset_ = 0;

  const bool read_only_;

  size_t chunk_size_ = 0;

  bool is_error_ = false;

  /**
   * Frees the allocated cached buffers and reinitializes all associated variables.
   */
  virtual void free_buffer() {
    if (buffer_) free(buffer_);
    buffer_ = NULL;
    buffer_offset_ = 0;
    buffer_size_ = 0;
  }

  int read_buffer();
  virtual int write_buffer();
};

class CompressedStorageBuffer : public StorageBuffer {
 public:
  CompressedStorageBuffer(StorageFS *fs, const std::string& filename, size_t chunk_size, const bool is_read=false,
                          const int compression_type=TILEDB_NO_COMPRESSION, const int compression_level=0)
      : StorageBuffer(fs, filename, chunk_size, is_read),
        compression_type_(compression_type), compression_level_(compression_level) {
  }

  ~CompressedStorageBuffer() {
    free_buffer();
  }

  int read_buffer(void *bytes, size_t size);
  int write_buffer();
  int finalize();

  void free_buffer() {
    if (compress_buffer_) free(compress_buffer_);
    compress_buffer_ = NULL;
    compress_buffer_size_ = 0;
    StorageBuffer::free_buffer();
  }

 private:
  const int compression_type_;
  const int compression_level_;
  void *compress_buffer_ = NULL;
  size_t compress_buffer_size_ = 0;

  std::shared_ptr<StorageBuffer> compressed_write_buffer_ = 0;
  size_t compressed_write_buffer_size_ = 0;

  int initialize_gzip_stream(z_stream *strm);
  int gzip_read_buffer();
  int gzip_write_buffer();
};
