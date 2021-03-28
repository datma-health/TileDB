/**
 * @file storage_gcs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018,2021 Omics Data Automation, Inc.
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
 * GCS Support for StorageFS
 */

#ifdef USE_HDFS

#include "error.h"
#include "storage_gcs.h"
#include "storage_posixfs.h"

#include "google/cloud/storage/client_options.h"

#include <cerrno>
#include <cstring>
#include <clocale>
#include <iostream>
#include <memory>
#include <cstdlib>
#include <unistd.h>
#include <system_error>

#include <string>

#define GCS_ERROR(MSG, PATH) PATH_ERROR(TILEDB_FS_ERRMSG, "GCS: "+MSG, PATH, tiledb_fs_errmsg)
#define GCS_ERROR1(MSG, STATUS, PATH) PATH_ERROR(TILEDB_FS_ERRMSG, "GCS: "+MSG+" "+STATUS.message(), PATH, tiledb_fs_errmsg)

char *trim(char *value) {
  while (value[0] == '\"' || value[0] == ' ' || value[0] == '\n') {
    value++;
  } 
  while (value[strlen(value)-1] == '\"' || value[strlen(value)-1] == ' ' || value[strlen(value)-1] == '\n') {
    value[strlen(value)-1] = 0;
  }
  if (strcmp(value, "{") == 0 || strcmp(value, "}") == 0) {
    return NULL;
  }
  return value;
}

char *parse_json(char *filename, const char *key) {
  PosixFS *fs = new PosixFS();
  size_t size;

  if (!fs->is_file(filename) || (size = fs->file_size(filename)) <= 0) {
    return NULL;
  }

  char *buffer = (char *)malloc(size + 1);
  if (!buffer) {
    PRINT_ERROR("Failed to allocate buffer");
    return NULL;
  }
  memset((void *)buffer, 0, size+1);

  int rc = fs->read_from_file(filename, 0, buffer, size);
  if (rc) {
    PRINT_ERROR("Failed to read file " << filename);
  }

  delete fs;
  
  char *value = NULL;
  char *saveptr;
  char *token = strtok_r(buffer, ",\n ", &saveptr);
  while (token) {
    token = trim(token);
    if (token) {
      char *sub_saveptr;
      char *sub_token = strtok_r(token, ":", &sub_saveptr);
      if (strcmp(trim(sub_token), key) == 0) {
	value = strtok_r(NULL, ":", &sub_saveptr);
	free(buffer);
	value = trim(value);
	if (value) {
	  return strdup(value);
	} else {
	  return NULL;
	}
      }
    }
    token = strtok_r(NULL, ",", &saveptr);
  }
  free(buffer);
  return NULL;
}

#define GCS_PREFIX "gs://"

hdfsFS gcs_connect(struct hdfsBuilder *builder, const std::string& working_dir) {
  char *value = NULL;

  char *gcs_creds = getenv("GOOGLE_APPLICATION_CREDENTIALS");
  if (gcs_creds) {
    hdfsBuilderConfSetStr(builder, "google.cloud.auth.service.account.enable", "true");
    hdfsBuilderConfSetStr(builder, "google.cloud.auth.service.account.json.keyfile", gcs_creds);
    
    value = parse_json(gcs_creds, "project_id"); // free value after hdfsBuilderConnect as it is shallow copied.
    if (value) {
      hdfsBuilderConfSetStr(builder, "fs.gs.project.id", value);
    }
  }

  if (working_dir.empty()) {
    hdfsBuilderConfSetStr(builder, "fs.gs.working.dir", "/");
  } else {
    hdfsBuilderConfSetStr(builder, "fs.gs.working.dir", working_dir.c_str());
  }

  // Default buffer sizes are huge in the GCS connector. TileDB reads/writes in smaller chunks,
  // so the buffer size can be made a little smaller.
  hdfsBuilderConfSetStr(builder, "fs.gs.outputstream.upload.chunk.size", "262144");

  // This is deprecated in the newer versions. But, gatk uses a very old version of the gcs connector.
  hdfsBuilderConfSetStr(builder, "fs.gs.io.buffersize.write", "262144");

  hdfsFS hdfs_handle = hdfsBuilderConnect(builder);
  free(value);
  return hdfs_handle;
}

#endif /* USE_HDFS */


GCS::GCS(const std::string& home) {
  gcs_uri path_uri(home);

  if (path_uri.protocol().compare("gs") != 0) {
    throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "GCS FS only supports gs:// URI protocols");
  }

  if (path_uri.bucket().size() == 0) {
    throw std::system_error(EPROTO, std::generic_category(), "GS URI does not seem to have a bucket specified");
  }

#ifdef __linux__
  gcs::ChannelOptions channel_options;
  std::string ca_certs_location = locate_ca_certs();
  if (ca_certs_location.empty()) {
    std::cerr << "CA Certs path not located" << std::endl;
  } else {
    channel_options.set_ssl_root_path(ca_certs_location);
  }
  std::cerr << "CA Certs path=" << ca_certs_location << std::endl;
  auto client_options = gcs::ClientOptions::CreateDefaultClientOptions(channel_options);
#else
  auto client_options = gcs::ClientOptions::CreateDefaultClientOptions();
#endif
  if (!client_options) {
    throw std::system_error(EIO, std::generic_category(), "Failed to create default GCS Client Options. "+
                            client_options.status().message());
  }
  // TODO: All the Policies seem to be internal with internal visibility. Leaving them as defaults for now
  // client_ = gcs::Client(*client_options, gcs::StrictIdempotencyPolicy(),
  //                                        gcs::AlwaysRetryIdempotencyPolicy(),
  //                                        gcs::LimitedErrorCountRetryPolicy(20));
  client_ = gcs::Client(*client_options);
  if (!client_) {
    throw std::system_error(EIO, std::generic_category(), "Failed to create GCS Client"+client_.status().message());
  }

  StatusOr<gcs::BucketMetadata> bucket_metadata = client_->GetBucketMetadata(path_uri.bucket());
  if (!bucket_metadata) {
    throw std::system_error(EIO, std::generic_category(),
                            "GCS FS only supports already existing buckets. Failed to locate bucket="
                            +path_uri.bucket()+" "+bucket_metadata.status().message());
  }

  bucket_name_ = path_uri.bucket();
  working_dir_ = get_path(path_uri.path());

  // Set default buffer sizes, overridden with env vars TILEDB_DOWNLOAD_BUFFER_SIZE and TILEDB_UPLOAD_BUFFER_SIZE
  // Minimum size for each part of a multipart upload, except for the last part, using the same value for both uploads and downloads for now
  download_buffer_size_ = 5*1024*1024; // 5M
  upload_buffer_size_ = 5*1024*1024; // 5M
}
  
GCS::~GCS() {
  // Nothing yet
}

std::string GCS::current_dir() {
  return working_dir_;
}

int GCS::set_working_dir(const std::string& dir) {
  working_dir_ = get_path(dir);
  return TILEDB_FS_OK;
}

bool GCS::path_exists(const std::string& path) {
  return client_->GetObjectMetadata(bucket_name_, get_path(path)).status().ok();
}

int GCS::create_path(const std::string& path) {
  StatusOr<gcs::ObjectMetadata> object_metadata =
      client_->InsertObject(bucket_name_, get_path(path), "");
  if (!object_metadata) {
    std::cerr << "Error inserting object " << path << " in bucket "
              << bucket_name_ << ", status=" << object_metadata.status()
              << std::endl;
    return TILEDB_FS_ERR;
  } else {
    return TILEDB_FS_OK;
  }
}

int GCS::delete_path(const std::string& path) {
  google::cloud::Status status = client_->DeleteObject(bucket_name_, get_path(path));
  if (!status.ok()) {
    GCS_ERROR1("Could not delete path", status, path);
    return TILEDB_FS_ERR;
  } else {
    return TILEDB_FS_OK;
  }
}

bool GCS::is_dir(const std::string& dir) {
  if (get_path(dir).length() == 0) {
    // This must be the container - OK
    return true;
  }
  return path_exists(slashify(dir));
}

bool GCS::is_file(const std::string& file) {
  return path_exists(unslashify(file));
}

std::string GCS::real_dir(const std::string& dir) {
   if (dir.find("://") != std::string::npos) {
    gcs_uri path_uri(dir);
    if (path_uri.bucket().compare(bucket_name_)) {
      throw std::runtime_error("Credentialed account during instantiation does not match the uri passed to real_dir. Aborting");
    }
  }
  return get_path(dir);
}

int GCS::create_dir(const std::string& dir) {
  if (is_dir(dir) || is_file(dir)) {
    GCS_ERROR("Cannot create path as it already exists", dir);
    return TILEDB_FS_ERR;
  }
  return create_path(slashify(dir));
}

int GCS::delete_dir(const std::string& dir) {
   if (is_file(dir)) {
    GCS_ERROR("Cannot delete dir as it seems to be a file", dir);
    return TILEDB_FS_ERR;
  }
  if (!is_dir(dir)) {
    GCS_ERROR("Cannot delete non-existent dir", dir);
    return TILEDB_FS_ERR;
  }
  gcs::DeleteByPrefix(*client_, bucket_name_, slashify(get_path(dir)));
  return TILEDB_FS_OK;
}

std::vector<std::string> GCS::get_dirs(const std::string& dir) {
  std::vector<std::string> dirs;
  for (auto&& object : client_->ListObjectsAndPrefixes(bucket_name_, gcs::Prefix(slashify(get_path(dir))),
                                                       gcs::Delimiter("/"))) {
    if (!object) {
      GCS_ERROR1("Error listing objects and prefixes", object.status(), dir);
      break;
    }
    auto result = *std::move(object);
    if (absl::holds_alternative<std::string>(result)) {
      dirs.push_back(unslashify(absl::get<std::string>(result)));
    }
  }
  return dirs;
}

std::vector<std::string> GCS::get_files(const std::string& dir) {
  std::vector<std::string> files;
  for (auto&& object : client_->ListObjects(bucket_name_, gcs::Prefix(slashify(get_path(dir))),
                                            gcs::Delimiter("/"))) {
    if (!object) {
      GCS_ERROR1("Error listing objects", object.status(), dir);
      break;
    } else {
      if (object->name().back() != '/') {
        files.push_back(object->name());
      }
    }
  }
  return files;
}

int GCS::create_file(const std::string& filename, int flags, mode_t mode) {
  if (is_dir(filename) || is_file(filename)) {
    GCS_ERROR("Cannot create path as it already exists", filename);
    return TILEDB_FS_ERR;
  }
  return create_path(filename);
}

int GCS::delete_file(const std::string& filename) {
  if (!is_file(filename)) {
    GCS_ERROR("Cannot delete non-existent or non-file path", filename);
    return TILEDB_FS_ERR;
  }
  return delete_path(filename);
}

ssize_t GCS::file_size(const std::string& filename) {
  google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>
      object_metadata = client_->GetObjectMetadata(bucket_name_, get_path(filename));
  if (object_metadata.ok()) {
    return object_metadata->size();
  } else {
    return TILEDB_FS_ERR;
  }
}

int GCS::read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length) {
  if (length == 0) {
    return TILEDB_FS_OK; // Nothing to read
  }

  gcs::ObjectReadStream stream = client_->ReadObject(bucket_name_, get_path(filename), gcs::ReadRange(offset, offset+length));
  if (!stream.status().ok()) {
    GCS_ERROR1("Failed to get object", stream.status(), filename);
    return TILEDB_FS_ERR;
  }
  
  stream.read(static_cast<char *>(buffer), length);
  if ((size_t)stream.gcount() < length) {
    GCS_ERROR("Could not read the file for bytes of length=" + std::to_string(length) + " from offset=" + std::to_string(offset), filename);
    return TILEDB_FS_ERR;
  }
  
  return TILEDB_FS_OK;
}

#define CHUNK_SUFFIX "__tiledb__"

int GCS::write_to_file(const std::string& filename, const void *buffer, size_t buffer_size) {
  if (buffer_size == 0) {
    return create_file(filename, 0, 0);
  }
  std::string filepath = get_path(filename);
  size_t part_number = 0;
  {
    // Separate block to limit mutex scope
    const std::lock_guard<std::mutex> lock(write_map_mtx_);
    auto search = write_map_.find(filepath);
    if (search == write_map_.end()) {
      auto update_info = multipart_upload_info_t();
      update_info.last_uploaded_size_ = buffer_size;
      write_map_.insert({filepath, std::move(update_info)});
    } else {
      if (search->second.last_uploaded_size_ < 5*1024*1024) {
        GCS_ERROR("Only the last of the uploadable parts can be less than 5MB, try increasing TILEDB_UPLOAD_BUFFER_SIZE to at least 5MB", filepath);
        return TILEDB_FS_ERR;
      }
      part_number = ++search->second.part_number_;
      search->second.last_uploaded_size_ = buffer_size;
    }
  }
  std::string part = filepath + CHUNK_SUFFIX + std::to_string(part_number);
  std::string buf;
  buf.assign(static_cast<const char *>(buffer), buffer_size);
  StatusOr<gcs::ObjectMetadata> object_metadata = client_->InsertObject(bucket_name_, part, std::move(buf));
  if (!object_metadata) {
    GCS_ERROR1("Error writing part during InsertObject", object_metadata.status(), part);
    return TILEDB_FS_ERR;
  };
  return TILEDB_FS_OK;
}

int GCS::commit_file(const std::string& filename) {
  std::string filepath = get_path(filename);
  std::vector<gcs::ComposeSourceObject> parts;
  size_t num_parts = 0;
  {
    // Separate block to limit mutex scope
    const std::lock_guard<std::mutex> lock(write_map_mtx_);
    auto found = write_map_.find(filepath);
    if (found == write_map_.end()) {
      return TILEDB_FS_OK;
    }
    num_parts = found->second.part_number_;
    write_map_.erase(filepath);
  }
  for(auto i=0ul; i<=num_parts; i++) {
    gcs::ComposeSourceObject part;
    part.object_name = filepath + CHUNK_SUFFIX + std::to_string(i);
    parts.emplace_back(std::move(part));
  }
  std::string prefix = gcs::CreateRandomPrefixName(".tmpfiles");
  StatusOr<gcs::ObjectMetadata> object_metadata = gcs::ComposeMany(*client_, bucket_name_, parts, prefix, filepath, /*ignore_cleanup_failures*/true);
  int rc = TILEDB_FS_OK;
  if (!object_metadata) {
    GCS_ERROR1("Error committing object during ComposeMany", object_metadata.status(), filepath);
    rc = TILEDB_FS_ERR;
  }
  // Cleanup parts and other temp files leftover by ComposeMany()
  gcs::DeleteByPrefix(*client_, bucket_name_, filepath+CHUNK_SUFFIX);
  gcs::DeleteByPrefix(*client_, bucket_name_, prefix);
  return rc;
}
