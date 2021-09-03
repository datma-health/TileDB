/**
 * @ storage_gcs.h
 *
 * @section LICENSE
 *
 * The MIT License
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
 * GCS derived from StorageFS for Google Cloud Services
 *
 */

#ifndef __STORAGE_GCS_H__
#define  __STORAGE_GCS_H__

#include "tiledb_constants.h"

#include <mutex>
#include <string>
#include <unordered_map>

#ifdef USE_HDFS

#include "hdfs.h"

hdfsFS gcs_connect(struct hdfsBuilder *builder, const std::string& working_dir);

#endif /* USE_HDFS */

#include "storage_fs.h"

#include "google/cloud/storage/client.h"
#include <iostream>

namespace gcs = google::cloud::storage;
using ::google::cloud::StatusOr;

class GCS : public StorageCloudFS {

 public:
  GCS(const std::string& home);
  ~GCS();

  std::string get_path(const std::string& path) {
    return StorageCloudFS::get_path(path);
  }

  std::string current_dir();
  int set_working_dir(const std::string& dir);

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

 protected:
  std::string bucket_name_;
  StatusOr<gcs::Client> client_;

  std::mutex write_map_mtx_;
  typedef struct multipart_upload_info_t {
   public:
    size_t part_number_ = 0;
    size_t last_uploaded_size_ = 0;
  } multipart_upload_info_t;
  std::unordered_map<std::string, multipart_upload_info_t> write_map_;

  bool path_exists(const std::string& path);
  int create_path(const std::string& path);
  int delete_path(const std::string& path);
  int commit_file(const std::string& filename);
};

#endif /* __STORAGE_GCS_H__ */

