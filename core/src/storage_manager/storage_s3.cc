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

#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/model/Bucket.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

// errno is not relevant with aws sdk, so setting to 0
#define S3_ERROR(MSG, PATH) PATH_ERROR(TILEDB_FS_ERRMSG, "S3: "+MSG, PATH, tiledb_fs_errmsg)
#define S3_ERROR1(MSG, OUTCOME, PATH) PATH_ERROR(TILEDB_FS_ERRMSG, "S3: "+MSG+" "+OUTCOME.GetError().GetExceptionName()+" "+OUTCOME.GetError().GetMessage(), PATH, tiledb_fs_errmsg)
#define CLASS_TAG "TILEDB_STORAGE_S3"

// ugly, as we cannot call Aws::ShutdownAPI(memory leak??) but no other option for now -
// See https://github.com/aws/aws-sdk-cpp/issues/456 and
//     https://github.com/aws/aws-sdk-cpp/issues/1067
std::once_flag awssdk_init_api_flag;
std::shared_ptr<Aws::SDKOptions> awssdk_options;
void awssdk_init_api() {
  awssdk_options = std::make_shared<Aws::SDKOptions>();
  //  awssdk_options->loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
  Aws::InitAPI(*awssdk_options.get());
}

S3::S3(const std::string& home) {
  s3_uri path_uri(home);

  if (path_uri.protocol().compare("s3") != 0) {
    throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "S3 FS only supports s3:// URI protocols");
  }

  if (path_uri.bucket().size() == 0) {
    throw std::system_error(EPROTO, std::generic_category(), "S3 URI does not seem to have a bucket specified");
  }

  std::call_once(awssdk_init_api_flag, awssdk_init_api);

  Aws::Client::ClientConfiguration client_config;
  client_config.scheme = Aws::Http::Scheme::HTTPS; // SSL/TLS only for HIPPA/PHI Compliance
  // Check for endpoint_override. This environment variable is not exposed by aws sdk and is specific to TileDB.
  auto env_var = getenv("AWS_ENDPOINT_OVERRIDE");
  bool useVirtualAddressing = true; // default
  if (env_var) {
    client_config.endpointOverride = Aws::String(env_var);
    useVirtualAddressing = false;
  }
  std::shared_ptr<Aws::Client::DefaultRetryStrategy> retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(15, 2);
  client_config.retryStrategy = retryStrategy;
  std::string ca_certs_location = locate_ca_certs();
  if (!ca_certs_location.empty()) {
    client_config.caFile = ca_certs_location.c_str();
  }
  client_ = std::make_shared<Aws::S3::S3Client>(client_config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, useVirtualAddressing);

  Aws::S3::Model::HeadBucketRequest head_request;
  head_request.WithBucket(path_uri.bucket());
  auto outcome = client_->HeadBucket(head_request);
  if (!outcome.IsSuccess()) {
    S3_ERROR1("Failed to locate bucket", outcome, home);
    throw std::system_error(EIO, std::generic_category(), "S3 FS only supports already existing buckets. Create bucket from either the aws CLI or the aws storage portal before restarting operation");
  }

  bucket_name_ = path_uri.bucket();
  working_dir_ = get_path(path_uri.path());

  // Set default buffer sizes, overridden with env vars TILEDB_DOWNLOAD_BUFFER_SIZE and TILEDB_UPLOAD_BUFFER_SIZE
  // Minimum size for each part of a multipart upload, except for the last part, using the same value for both uploads and downloads for now
  // see https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html
  download_buffer_size_ = 5*1024*1024; // 5M
  upload_buffer_size_ = 5*1024*1024; // 5M
}

S3::~S3() {
  // Check for all the files that have not been committed
  std::vector<std::string> uncommitted_files;
  for (auto it=write_map_.begin(); it!=write_map_.end(); ++it) {
    uncommitted_files.push_back(it->first);
  }
  for (auto filename : uncommitted_files) {
    commit_file(filename);
  }
}

std::string S3::current_dir() {
  return working_dir_;
}

int S3::set_working_dir(const std::string& dir) {
  working_dir_ = get_path(dir);
  return TILEDB_FS_OK;
}

bool S3::path_exists(const std::string& path) {
  auto aws_path = to_aws_string(get_path(path));
  Aws::S3::Model::HeadObjectRequest request;
  request.SetBucket(bucket_name_);
  request.SetKey(aws_path);
  if (client_->HeadObject(request).IsSuccess()) {
    return true;
  } else if (path.back() == '/') {
    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(bucket_name_);
    request.SetPrefix(aws_path);
    request.SetDelimiter(to_aws_string(DELIMITER));
    request.SetMaxKeys(1);
    auto outcome = client_->ListObjectsV2(request);
    if (outcome.IsSuccess()) {
      return outcome.GetResult().GetContents().size() ||
          outcome.GetResult().GetCommonPrefixes().size();
    }
  }
  return false;
}

std::string S3::real_dir(const std::string& dir) {
  if (dir.find("://") != std::string::npos) {
    s3_uri path_uri(dir);
    if (path_uri.bucket().compare(bucket_name_)) {
      throw std::runtime_error("Credentialed account during instantiation does not match the uri passed to real_dir. Aborting");
    }
  }
  return get_path(dir);
}

int S3::create_path(const std::string& path) {
  Aws::S3::Model::PutObjectRequest request;
  request.SetBucket(bucket_name_);
  request.SetKey(to_aws_string(get_path(path)));
  auto outcome = client_->PutObject(request);
  if (!outcome.IsSuccess()) {
    S3_ERROR1("Could not create path", outcome, path);
    return TILEDB_FS_ERR;
  }
  return TILEDB_FS_OK;
}

int S3::create_dir(const std::string& dir) {
  if (is_dir(dir) || is_file(dir)) {
    S3_ERROR("Path already exists", dir);
    return TILEDB_FS_ERR;
  }
  /*  if (working_dir_.compare(dir) && !is_dir(parent_dir(NULL, dir))) {
    S3_ERROR("Parent directory to path does not exist", dir);
    return TILEDB_FS_ERR;
    } */
  return create_path(slashify(dir));
}

int S3::delete_path(const std::string& path) {
  auto aws_path = to_aws_string(get_path(path));
  Aws::S3::Model::DeleteObjectRequest request;
  request.SetBucket(bucket_name_);
  request.SetKey(aws_path);
  auto outcome = client_->DeleteObject(request);
  if (!outcome.IsSuccess()) {
    S3_ERROR1("Could not delete path", outcome, path);
    return TILEDB_FS_ERR;
  }
  return TILEDB_FS_OK;
}

int S3::delete_dir(const std::string& dir) {
  if (is_file(dir)) {
    S3_ERROR("Cannot delete dir as it seems to be a file", dir);
    return TILEDB_FS_ERR;
  }
  if (!is_dir(dir)) {
    S3_ERROR("Cannot delete non-existent dir", dir);
    return TILEDB_FS_ERR;
  }

  auto path = to_aws_string(slashify(get_path(dir)));
  Aws::S3::Model::ListObjectsV2Request request;
  request.SetBucket(bucket_name_);
  request.SetPrefix(path);
  request.SetDelimiter(to_aws_string(DELIMITER));
  Aws::String continuation_token = "";
  int rc = TILEDB_FS_OK;
  do {
    if (continuation_token.length() > 0) {
      request.SetContinuationToken(continuation_token);
    }
    auto outcome = client_->ListObjectsV2(request);
    if (!outcome.IsSuccess()) {
      rc = TILEDB_FS_ERR;
      break;
    } else {
      continuation_token = outcome.GetResult().GetNextContinuationToken();
      // Delete all files in dirs
      for (const auto& object : outcome.GetResult().GetContents()) {
        if (is_file(object.GetKey())) {
          rc |= delete_path(object.GetKey());
        }
      }

      // Recursively delete children dirs
      // TODO: Remove recursion for performance
      for (const auto& object : outcome.GetResult().GetCommonPrefixes()) {
        rc |= delete_dir(object.GetPrefix());
      }
    }
  } while (continuation_token.length() > 0);

  // Check if dir exists again as some object stores(e.g. minio) remove the
  // directory after removing all content
  if (is_dir(dir)) {
     rc |= delete_path(slashify(dir));
  }

  return rc;
}

std::vector<std::string> S3::get_dirs(const std::string& dir) {
  std::vector<std::string> dirs;
  auto path = to_aws_string(slashify(get_path(dir)));
  Aws::S3::Model::ListObjectsV2Request request;
  request.SetBucket(bucket_name_);
  request.SetPrefix(path);
  request.SetDelimiter(to_aws_string(DELIMITER));
  Aws::String continuation_token = "";
  do {
    if (continuation_token.length() > 0) {
      request.SetContinuationToken(continuation_token);
    }
    auto outcome = client_->ListObjectsV2(request);
    if (outcome.IsSuccess()) {
      continuation_token = outcome.GetResult().GetNextContinuationToken();
      for (const auto& object : outcome.GetResult().GetCommonPrefixes()) {
        dirs.push_back(unslashify(object.GetPrefix()));
      }
    }
  } while (continuation_token.length() > 0);
  return dirs;
}

std::vector<std::string> S3::get_files(const std::string& dir) {
  std::vector<std::string> files;
  auto path = to_aws_string(slashify(get_path(dir)));
  Aws::S3::Model::ListObjectsV2Request request;
  request.SetBucket(bucket_name_);
  request.SetPrefix(path);
  request.SetDelimiter(to_aws_string(DELIMITER));
  Aws::String continuation_token = "";
  do {
    if (continuation_token.length() > 0) {
      request.SetContinuationToken(continuation_token);
    }
    auto outcome = client_->ListObjectsV2(request);
    if (outcome.IsSuccess()) {
      continuation_token = outcome.GetResult().GetNextContinuationToken();
      for (const auto& object : outcome.GetResult().GetContents()) {
        if (is_file(object.GetKey())) {
          files.push_back(object.GetKey());
        }
      }
    }
  } while (continuation_token.length() > 0);
  return files;
}
  
int S3::create_file(const std::string& filename, int flags, mode_t mode) {
  if (is_dir(filename) || is_file(filename)) {
    S3_ERROR("Cannot create path as it already exists", filename);
    return TILEDB_FS_ERR;
  }
  return create_path(filename);
}

int S3::delete_file(const std::string& filename) {
  if (!is_file(filename)) {
    S3_ERROR("Cannot delete non-existent or non-file path", filename);
    return TILEDB_FS_ERR;
  }
  return delete_path(filename);
}

ssize_t S3::file_size(const std::string& filename) {
  auto aws_path = to_aws_string(get_path(filename));
  Aws::S3::Model::HeadObjectRequest request;
  request.SetBucket(bucket_name_);
  request.SetKey(aws_path);
  auto outcome = client_->HeadObject(request);
  if (outcome.IsSuccess()) {
    return outcome.GetResult().GetContentLength();
  }
  return TILEDB_FS_ERR;
}

int S3::read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length) {
  if (length == 0) {
    return TILEDB_FS_OK; // Nothing to read
  }

  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucket_name_);
  request.SetKey(to_aws_string(get_path(filename)));
  request.SetRange(to_aws_string("bytes=" + std::to_string(offset) + "-" + std::to_string(offset+length-1)));
  request.SetResponseStreamFactory([buffer, length]() {
      unsigned char* buf = reinterpret_cast<unsigned char*>(const_cast<void*>(buffer));
      auto stream_buffer = Aws::New<Aws::Utils::Stream::PreallocatedStreamBuf>(CLASS_TAG, buf, length);
      return Aws::New<Aws::IOStream>(CLASS_TAG, stream_buffer);
    });

  auto outcome = client_->GetObject(request);
  if (!outcome.IsSuccess()) {
    S3_ERROR1("Failed to get object", outcome, filename);
    return TILEDB_FS_ERR;
  }

  auto content_length = outcome.GetResult().GetContentLength();
  if (content_length < 0 || (size_t)content_length < length) {
    S3_ERROR("Could not read the file for bytes of length=" + std::to_string(length) + " from offset=" + std::to_string(offset), filename);
    return TILEDB_FS_ERR;
  }

  return TILEDB_FS_OK;
}

int S3::write_to_file(const std::string& filename, const void *buffer, size_t buffer_size) {
  if (buffer_size == 0) {
    return create_file(filename, 0, 0);
  }

  // Initialize upload
  std::string filepath = get_path(filename);
  auto aws_filename = to_aws_string(filepath);
  std::string upload_id;
  size_t part_number = 0;
  std::shared_ptr<Aws::S3::Model::CompletedMultipartUpload> completed_parts;
  {
    const std::lock_guard<std::mutex> lock(write_map_mtx_);

    auto search = write_map_.find(filepath);
    if (search == write_map_.end()) {
      // Filepath not in write_map, so start by initiating the request
      Aws::S3::Model::CreateMultipartUploadRequest create_request;
      create_request.SetBucket(bucket_name_);
      create_request.SetKey(aws_filename);

      auto createMultipartUploadOutcome = client_->CreateMultipartUpload(create_request);
      std::string upload_id = createMultipartUploadOutcome.GetResult().GetUploadId();

      auto update_info = multipart_upload_info_t(upload_id);
      write_map_.insert({filepath, std::move(update_info)});
    }
    auto found = write_map_.find(filepath);
    assert(found != write_map_.end());
    found->second.part_number_++;
    upload_id = found->second.upload_id_;
    part_number = found->second.part_number_;
    completed_parts = found->second.completed_parts_;
    // Verify that the previous uploaded part was at least 5M - see https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html
    // S3 throws error only when committing, so it is better to check in write_to_file and abort here. Note that cleanup occurs
    // in commit_file that is called from the destructor if there was an issue.
    auto last_uploaded_size = found->second.last_uploaded_size_;
    if (found->second.abort_upload_ || (last_uploaded_size != 0 && last_uploaded_size < 5*1024*1024)) {
      S3_ERROR("Only the last of the uploadable parts can be less than 5MB", filepath);
      found->second.abort_upload_ = true;
      return TILEDB_FS_ERR;
    } else {
      found->second.last_uploaded_size_ = buffer_size;
    }
  }

  assert(!upload_id.empty());
  assert(part_number > 0);

  // Start upload
  Aws::S3::Model::UploadPartRequest request;
  request.SetBucket(bucket_name_);
  request.SetKey(aws_filename);
  request.SetPartNumber(part_number);
  request.SetUploadId(to_aws_string(upload_id));

  unsigned char* buf = reinterpret_cast<unsigned char*>(const_cast<void*>(buffer));
  auto stream_buffer = Aws::New<Aws::Utils::Stream::PreallocatedStreamBuf>(CLASS_TAG, buf, buffer_size);
  request.SetBody(Aws::MakeShared<Aws::IOStream>(CLASS_TAG, stream_buffer));
  request.SetContentLength(buffer_size);

  auto outcome = client_->UploadPartCallable(request);
  auto etag = outcome.get().GetResult().GetETag();
  if (etag.empty()) {
    S3_ERROR("UploadPartCallable not successful as etag is empty", filename);
    return TILEDB_FS_ERR;
  }
  
  // Add to list of uploaded parts
  Aws::S3::Model::CompletedPart completed_part;
  completed_part.SetPartNumber(part_number);
  completed_part.SetETag(etag);

  completed_parts->AddParts(std::move(completed_part));

  return TILEDB_FS_OK;
}

int S3::commit_file(const std::string& filename) {
  std::string filepath = get_path(filename);
  auto aws_filename = to_aws_string(filepath);
  std::string upload_id;
  std::shared_ptr<Aws::S3::Model::CompletedMultipartUpload> completed_parts;
  bool abort_upload = false;

  int rc = TILEDB_FS_OK;
  {
    const std::lock_guard<std::mutex> lock(write_map_mtx_);
    auto found = write_map_.find(filepath);
    if (found != write_map_.end()) {
      auto multipart_update_info = found->second;
      upload_id = multipart_update_info.upload_id_;
      completed_parts = multipart_update_info.completed_parts_;
      abort_upload = multipart_update_info.abort_upload_;
      write_map_.erase(filepath);
    }
  }

  if (!upload_id.empty()) {
    if (!abort_upload) {
      Aws::S3::Model::CompleteMultipartUploadRequest finish_upload_request;
      finish_upload_request.SetBucket(bucket_name_);
      finish_upload_request.SetKey(aws_filename);
      finish_upload_request.SetUploadId(to_aws_string(upload_id));
      finish_upload_request.WithMultipartUpload(*completed_parts);
      auto finish_upload_outcome = client_->CompleteMultipartUpload(finish_upload_request);
      if (!finish_upload_outcome.IsSuccess()) {
        std::string msg = "Could not upload successfully for upload_id=" + upload_id;
        S3_ERROR1(msg, finish_upload_outcome, filename);
        abort_upload = true;
        rc = TILEDB_FS_ERR;
      }
    }

    if (abort_upload) {
      Aws::S3::Model::AbortMultipartUploadRequest abort_upload_request;
      abort_upload_request.SetBucket(bucket_name_);
      abort_upload_request.SetKey(aws_filename);
      abort_upload_request.SetUploadId(to_aws_string(upload_id));
      auto abort_upload_outcome = client_->AbortMultipartUpload(abort_upload_request);
      if (!abort_upload_outcome.IsSuccess()) {
        S3_ERROR1("Could not abort upload successfullt", abort_upload_outcome, filename);
        rc = TILEDB_FS_ERR;
      }
    }
  }

  return rc;
}
