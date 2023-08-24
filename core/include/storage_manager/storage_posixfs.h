/**
 * @file local_fs.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 Omics Data Automation, Inc.
 * @copyright Copyright (C) 2023 dātma, inc™
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
 * LocalFS derived from StorageFS for Posix Filesystem
 *
 */

#ifndef __STORAGE_POSIXFS_H__
#define  __STORAGE_POSIXFS_H__

#include "storage_fs.h"

#include <mutex>
#include <unordered_map>

class PosixFS : public StorageFS {
 public:
  ~PosixFS();

  std::string current_dir();
  int set_working_dir(const std::string& dir);

  bool is_dir(const std::string& dir);
  bool is_file(const std::string& file);
  std::string real_dir(const std::string& dir);
               
  int create_dir(const std::string& dir);
  int delete_dir(const std::string& dir);

  std::vector<std::string> get_dirs(const std::string& dir);
  std::vector<std::string> get_files(const std::string& dir);

  int create_file(const std::string& filename, int flags, mode_t mode);
  int delete_file(const std::string& filename);

  ssize_t file_size(const std::string& filename);

  int read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length);
  int write_to_file(const std::string& filename, const void *buffer, size_t buffer_size);
  
  int move_path(const std::string& old_path, const std::string& new_path);
    
  int sync_path(const std::string& path);

  bool locking_support();

  int close_file(const std::string& filename);

  void set_keep_write_file_handles_open(const bool val);

  bool keep_write_file_handles_open();

  void set_disable_file_locking(const bool val);

  bool disable_file_locking();

  private:
  std::mutex write_map_mtx_;
  std::unordered_map<std::string, int> write_map_;

  bool keep_write_file_handles_open_set_ = false;
  bool keep_write_file_handles_open_ = false;

  bool is_disable_file_locking_set = false;
  bool disable_file_locking_ = false;

  int write_to_file_keep_file_handles_open(const std::string& filename, const void *buffer, size_t buffer_size);
};

#endif /* __STORAGE_POSIXFS_H__ */

