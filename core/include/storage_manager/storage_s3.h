/**
 * @file   storage_s3.h
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
 * AWS S3 Support for StorageFS
 */


#ifndef __STORAGE_S3_H__
#define  __STORAGE_S3_H__

#include "storage_fs.h"

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>

class S3 : public StorageCloudFS {

 public:
  S3(const std::string& home);
  ~S3();

  std::string get_path(const std::string& path) {
    return StorageCloudFS::get_path(path);
  }

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

  int commit_file(const std::string& filename);

  // Helper functions
  inline Aws::String to_aws_string(const std::string& s) const {
    return Aws::String(s.begin(), s.end());
  }

 protected:
  std::string bucket_name_;
 
  Aws::SDKOptions options_;
  std::shared_ptr<Aws::S3::S3Client> client_;
  std::mutex write_map_mtx_;
  typedef struct multipart_upload_info_t {
   public:
    multipart_upload_info_t(const std::string& upload_id) : upload_id_(upload_id) {
      completed_parts_ = std::make_shared<Aws::S3::Model::CompletedMultipartUpload>();
    }
    std::string upload_id_;
    size_t part_number_ = 0;
    size_t last_uploaded_size_ = 0;
    std::shared_ptr<Aws::S3::Model::CompletedMultipartUpload> completed_parts_;
    bool abort_upload_ = false;
  } multipart_upload_info_t;
  std::unordered_map<std::string, multipart_upload_info_t> write_map_;

  bool path_exists(const std::string& path);
  int create_path(const std::string& path);
  int delete_path(const std::string& path);
};

#endif /*  __STORAGE_S3_H__ */
