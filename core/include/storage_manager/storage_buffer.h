/**
 * @file storage_buffer.h
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
 * This file defines the StorageBuffer class that buffers writes/reads to/from files
 */

#pragma once

#include "buffer.h"
#include "storage_fs.h"

class StorageBuffer : public Buffer {
 public:
  StorageBuffer(StorageFS *fs, const std::string& filename) {
    fs_ = fs;
    filename_ = filename;
  }

  /**
   * Reads the data from the cached buffer from the offset into bytes.
   * @param offset The offset from which the data in the buffer will be read from.
   * @param bytes The buffer into which the data will be written.
   * @param length The size of the data to be read from the cached buffer.
   */
  int read_buffer(off_t offset, void *bytes, size_t size);

  int append_buffer(const void *bytes, size_t size);

  int finalize();
  
 private:
  int write_buffer();
  
  StorageFS *fs_ = NULL;
  std::string filename_;
  uint32_t num_blocks_ = 0;
  std::vector<bool> blocks_read_;
};
