/**
 * @file   storage_s3.cc
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

#include "error.h"
#include "storage_s3.h"
#include "uri.h"
#include "utils.h"

#include <aws/s3/model/Bucket.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#define S3_ERROR(MSG, PATH) SYSTEM_ERROR(TILEDB_FS_ERRMSG, "S3: "+MSG, PATH, tiledb_fs_errmsg)

S3::S3(const std::string& home) {
  s3_uri path_uri(home);

  if (path_uri.protocol().compare("s3") != 0) {
    throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "S3 FS only supports s3:// URI protocols");
  }

  if (path_uri.bucket().size() == 0) {
    throw std::system_error(EPROTO, std::generic_category(), "S3 URI does not seem to have a bucket specified");
  }

  // TODO: Set up options_ wrt logging
  Aws::InitAPI(options_);
  //  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider = setup_credentials_provider();

  Aws::Client::ClientConfiguration client_config;
  client_config.scheme = Aws::Http::Scheme::HTTPS; // SSL/TLS only for HIPPA/PHI Compliance
  client_ = std::make_shared<Aws::S3::S3Client>(client_config);

  bool bucket_exists = false;
  Aws::S3::Model::ListBucketsOutcome outcome = client_->ListBuckets();
  if (outcome.IsSuccess()) {
    Aws::Vector<Aws::S3::Model::Bucket> buckets = outcome.GetResult().GetBuckets();
    for (Aws::S3::Model::Bucket& bucket : buckets) {
      if (bucket.GetName().compare(path_uri.bucket()) == 0) {
        bucket_exists = true;
        break;
      }
    }
  }
  if (!bucket_exists) {
    throw std::system_error(EIO, std::generic_category(), "S3 FS only supports already existing buckets. Create bucket from either the aws CLI or the aws storage portal before restarting operation");
  }

  bucket_name_ = path_uri.bucket();
  working_dir_ = get_path(path_uri.path());
}

S3::~S3() {
  Aws::ShutdownAPI(options_);
}

std::string S3::current_dir() {
  return working_dir_;
}

int S3::set_working_dir(const std::string& dir) {
  working_dir_ = get_path(dir);
  return TILEDB_FS_OK;
}
  
bool S3::is_dir(const std::string& dir) {
  auto dir_path = to_aws_string(get_path(slashify(dir)));
                         
  // For empty directories
  Aws::S3::Model::HeadObjectRequest headObjectRequest;
  headObjectRequest.SetBucket(bucket_name_);
  headObjectRequest.SetKey(dir_path);
  auto outcome = client_->HeadObject(headObjectRequest);
  if (outcome.IsSuccess()) {
    return true;
  }
  
  // For non-empty directories
  Aws::S3::Model::ListObjectsV2Request listObjectsV2Request;
  listObjectsV2Request.SetBucket(bucket_name_);
  listObjectsV2Request.SetPrefix(dir_path);
  listObjectsV2Request.SetMaxKeys(1);
  auto list_outcome = client_->ListObjectsV2(listObjectsV2Request);
  if (list_outcome.IsSuccess()) {
    return list_outcome.GetResult().GetKeyCount() > 0;
  }
}

bool S3::is_file(const std::string& file) {
  return false;
}

std::string S3::real_dir(const std::string& dir) {
  return "";
}

int S3::create_dir(const std::string& dir) {
  Aws::S3::Model::PutObjectRequest request;
  request.SetBucket(bucket_name_);
  request.SetKey(get_path(to_aws_string(slashify(dir))));
  auto outcome = client_->PutObject(request);
  if (!outcome.IsSuccess()) {
    S3_ERROR("Could not create empty dir", dir);
    return TILEDB_FS_ERR;
  }
  return TILEDB_FS_OK;
}

int S3::delete_dir(const std::string& dir) {
  if (!is_dir(dir)) {
    S3_ERROR("Cannot delete non-existent dir", dir);
    return TILEDB_FS_ERR;
  }

  std::vector<std::string> paths;
  Aws::S3::Model::ListObjectsV2Request request;
  request.SetBucket(bucket_name_);
  request.SetPrefix(get_path(to_aws_string(slashify(dir))));
  // request.SetDelimiter(to_aws_string(DELIMITER));
  auto outcome = client_->ListObjectsV2(request);
  if (outcome.IsSuccess()) {
   std::cout << "Objects in bucket '" << bucket_name_ << "':"
             << std::endl << std::endl;
        
   Aws::Vector<Aws::S3::Model::Object> objects =
       outcome.GetResult().GetContents();

   for (Aws::S3::Model::Object& object : objects) {   
     std::cout << object.GetKey() << std::endl;
   }
  } else {
    std::cout << "Could not list objects" << std::endl;
  }

  
  
  return TILEDB_FS_OK;
}

std::vector<std::string> S3::get_dirs(const std::string& dir) {
}

std::vector<std::string> S3::get_files(const std::string& dir) {
}
  
int S3::create_file(const std::string& filename, int flags, mode_t mode) {
}

int S3::delete_file(const std::string& filename) {
}

ssize_t S3::file_size(const std::string& filename) {
}

int S3::read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length) {
}

int S3::write_to_file(const std::string& filename, const void *buffer, size_t buffer_size) {
}
  
int S3::move_path(const std::string& old_path, const std::string& new_path) {
}
    
int S3::sync_path(const std::string& path) {
}

int S3::close_file(const std::string& filename) {
}
