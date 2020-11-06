/**
 * @file   storage_azure_blob.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019-2020 Omics Data Automation, Inc.
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
 * Azure Blob Support for StorageFS
 */

#include "storage_azure_blob.h"

#include "error.h"
#include "url.h"
#include "utils.h"

#include <algorithm>
#include <array>
#include <assert.h>
#include <cerrno>
#include <clocale>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <future>
#include <memory>
#include <stdlib.h>
#include <thread>
#include <unistd.h>


#ifdef __APPLE__
#if !defined(ELIBACC)
#define ELIBACC -1
#endif
#endif

#define AZ_BLOB_ERROR(MSG, PATH) SYSTEM_ERROR(TILEDB_FS_ERRMSG, "Azure: "+MSG, PATH, tiledb_fs_errmsg)

using namespace azure::storage_lite;

static std::string get_account_key(const std::string& account_name) {
  std::string key;
  // Get enviroment variables AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY
  char *az_storage_ac_name = getenv("AZURE_STORAGE_ACCOUNT");
  if (az_storage_ac_name && account_name.compare(az_storage_ac_name) == 0) {
    key = getenv("AZURE_STORAGE_KEY");
  }  
  return key;
}

static std::string get_access_token() {
  // Invoke `az account get-access-token --resource https://storage.azure.com/ -o tsv --query accessToken`
  std::array<char, 2048> buffer;
  std::string token;
  const char *cmd = "az account get-access-token --resource https://storage.azure.com/ -o tsv --query accessToken";
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
  if (pipe) {
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
      token += buffer.data();
    }
  }
  return token;
}

static std::string slashify(const std::string& path) {
  if (path.empty()) {
    return "/";
  } else if (path.back() != '/') {
    return path + '/';
  } else {
    return path;
  }
}

static std::string unslashify(const std::string& path) {
  if (!path.empty() && path.back() == '/') {
    return path.substr(0, path.size()-1);
  } else {
    return path;
  }
}

std::string AzureBlob::get_path(const std::string& path) {
  std::string pathname(path);
  if (path.find("://") != std::string::npos) {
    azure_url url_path(path);
    pathname = url_path.path();
     // This is the container
    if (pathname.empty()) {
      return "";
    }
  }
  if (pathname[0] == '/') {
    return pathname.substr(1);
  }

  if (pathname.empty()) {
    return working_dir;
  } else if (starts_with(pathname, working_dir)) {
    // TODO: this is a hack for now, but should work fine with GenomicsDB.
    return pathname;
  } else {
    return working_dir + '/' + pathname;
  }
}

// TODO: REMOVE: CMD: ./test_tiledb_utils --test-dir wasbs://build@oda.blob.core.windows.net/nalini_test
// TODO: Add robust error checking
AzureBlob::AzureBlob(const std::string& home) {
  azure_url path_url(home);

  // wasbs://<container_name>@<blob_storage_account_name>.blob.core.windows.net/<path>
  // e.g. wasbs://test@mytest.blob.core.windows.net/ws
  if (path_url.protocol().compare("az") != 0) {
    throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "Azure Blob FS only supports wasbs:// and wasb:// URL protocols");
  }

  if (path_url.account().size() == 0 || path_url.container().size() == 0) {
    throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "Azure Blob URL does not seem to have either an account or a container");
  }

  std::shared_ptr<storage_credential> cred = nullptr;
  std::string azure_account = path_url.account();  
  std::string azure_account_key = get_account_key(azure_account);
  if (azure_account_key.size() > 0) {
    cred = std::make_shared<shared_key_credential>(azure_account, azure_account_key);
  } else {
    std::string token = get_access_token();
    if (token.size() > 0) {
      cred = std::make_shared<token_credential>(token);
    }
  }
  
  if (cred == nullptr) {
    throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "Could not get credentials for azure storage account=" + azure_account);
  }

  std::shared_ptr<storage_account> account = std::make_shared<storage_account>(azure_account, cred, /* use_https */ true);
  bC = std::make_shared<blob_client>(account, std::thread::hardware_concurrency());
  bc_wrapper = std::make_shared<blob_client_wrapper>(bC);
  bc = reinterpret_cast<blob_client_wrapper *>(bc_wrapper.get());

  account_name = path_url.account();
  container_name = path_url.container();

  working_dir = get_path(path_url.path());
}

std::string AzureBlob::current_dir() {
  return working_dir;
}

int AzureBlob::set_working_dir(const std::string& dir) {
  working_dir = get_path(dir);
  return TILEDB_FS_OK;
}

#define MARKER ".dir.marker"

bool AzureBlob::is_dir(const std::string& dir) {
  if (dir.empty() || get_path(dir).empty() || is_file(slashify(dir)+MARKER)) {
    return true;
  }
  std::string continuation_token = "";
  list_blobs_segmented_response response;
  do {
    std::string path=slashify(get_path(dir));
    response = bc->list_blobs_segmented(container_name, "/", continuation_token, path);
    if (response.blobs.size() > 0) {
      return true;
    }
    continuation_token = response.next_marker;
  } while (!continuation_token.empty());
  return false;
}

bool AzureBlob::is_file(const std::string& file) {
  return bc->blob_exists(container_name, get_path(file));
}

std::string AzureBlob::real_dir(const std::string& dir) {
  if (dir.find("://") != std::string::npos) {
    azure_url path_url(dir);
    if (path_url.account().compare(account_name) || path_url.container().compare(container_name)) {
      throw std::runtime_error("Credentialed account during instantiation does not match the url passed to real_dir. Aborting");
    }
    // This is absolute path, so return the entire path
    return get_path(path_url.path());
  }
  return get_path(dir);
}
  
int AzureBlob::create_dir(const std::string& dir) {
  if (get_path(dir).empty()) {
    return TILEDB_FS_OK;
  }
  if (is_dir(dir)) {
    AZ_BLOB_ERROR("Directory already exists", dir);
    return TILEDB_FS_ERR;
  }
  std::string slashified_dir = slashify(dir);
  if (slashified_dir.find("://") != std::string::npos) {
    azure_url dir_url(slashified_dir);
    if (dir_url.path().empty()) {
      // This is the container and assuming it is already created for now
      return TILEDB_FS_OK;
    }
  }
  if (working_dir.compare(dir) && !is_dir(parent_dir(NULL, dir))) {
    AZ_BLOB_ERROR("Parent directory to path does not exist", dir);
    return TILEDB_FS_ERR;
  }
  if (write_to_file(slashified_dir+MARKER, NULL, 0) == TILEDB_FS_OK) {
    if (is_dir(dir)) {
      return TILEDB_FS_OK;
    }
  }
  AZ_BLOB_ERROR("Cannot create directory", dir);
  return TILEDB_FS_ERR;
}

int AzureBlob::delete_dir(const std::string& dir) {
  if (!is_dir(dir)) {
    AZ_BLOB_ERROR("Cannot delete non-existent dir", dir);
    return TILEDB_FS_ERR;
  }

  auto response = bc->list_blobs_segmented(container_name, "/", "", slashify(get_path(dir)), INT_MAX);
  if (!response.next_marker.empty()) {
    throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "TBD: No support for continuation tokens");
  }
  if (response.blobs.size() > 0) {
    for (auto i=0u; i<response.blobs.size(); i++) {
      if (response.blobs[i].is_directory) {
        delete_dir(response.blobs[i].name);
      } else {
        bc->delete_blob(container_name, response.blobs[i].name);
        if (errno) {
          AZ_BLOB_ERROR("Could not delete file", response.blobs[i].name);
        } 
        if (bc->blob_exists(container_name, response.blobs[i].name)) {
          AZ_BLOB_ERROR("File still exists after deletion", response.blobs[i].name);
        }
      }
    }
  }
  return TILEDB_FS_OK;
}

std::vector<std::string> AzureBlob::get_dirs(const std::string& dir) {
  std::vector<std::string> dirs;
  if (is_dir(dir)) {
    std::string continuation_token = "";
    auto response = bc->list_blobs_segmented(container_name, "/",  continuation_token, slashify(get_path(dir)), INT_MAX);
    do {
      for (auto i=0u; i<response.blobs.size(); i++) {
        if (response.blobs[i].is_directory) {
          dirs.push_back(unslashify(response.blobs[i].name));
        }
      }
    } while (!continuation_token.empty());
  }
  return dirs;
}
    
std::vector<std::string> AzureBlob::get_files(const std::string& dir) {
  std::vector<std::string> files;
  if (is_dir(dir)) {
    std::string continuation_token = "";
    auto response = bc->list_blobs_segmented(container_name, "/",  continuation_token, slashify(get_path(dir)), INT_MAX);
    do {
      for (auto i=0u; i<response.blobs.size(); i++) {
        if (!response.blobs[i].is_directory) {
          if (response.blobs[i].name.compare(MARKER) != 0) {
            files.push_back(response.blobs[i].name);
          }
        }
      }
    } while (!continuation_token.empty());
  }
  return files;
}

int AzureBlob::create_file(const std::string& filename, int flags, mode_t mode) {
  if (is_file(filename) || is_dir(filename)) {
    AZ_BLOB_ERROR("Cannot create path as it already exists", filename);
    return TILEDB_FS_ERR;
  }
  return write_to_file(filename, NULL, 0);
}

int AzureBlob::delete_file(const std::string& filename) {
  if (!is_file(filename)) {
    AZ_BLOB_ERROR("Cannot delete non-existent or non-file path", filename);
    return TILEDB_FS_ERR;
  }
  bc->delete_blob(container_name, get_path(filename));
  return TILEDB_FS_OK;
}

size_t AzureBlob::file_size(const std::string& filename) {
  auto blob_property = bc->get_blob_property(container_name, get_path(filename));
  if (blob_property.valid()) {
    return blob_property.size;
  } else {
    AZ_BLOB_ERROR("Could not get file properties", filename);
    return TILEDB_FS_ERR;
  }
  return 0;
}

#define GRAIN_SIZE (4*1024*1024)

// TODO: Implement reading in chunks of MAX_BYTES like in write_to_file(), but asynchronously.
int AzureBlob::read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length) {
  if (!is_file(filename)) {
    AZ_BLOB_ERROR("File does not exist", filename);
    return TILEDB_FS_ERR;
  }
  std::string path = get_path(filename);
  auto bclient = reinterpret_cast<blob_client *>(bC.get());
  auto size = file_size(filename);
  if (size >= (length + offset)) {
    storage_outcome<void> read_result;
    if (size < GRAIN_SIZE) {
      omemstream os_buf(buffer, length);
      read_result = bclient->download_blob_to_stream(container_name, path, offset, length, os_buf).get();
    } else {
      read_result = bclient->download_blob_to_buffer(container_name, path, offset, length, reinterpret_cast<char *>(buffer), std::thread::hardware_concurrency()/2).get();
    }
    if (!read_result.success()) {
      AZ_BLOB_ERROR(read_result.error().message, filename);
      return TILEDB_FS_ERR;
    } else {
      return TILEDB_FS_OK;
    }
  } else {
   AZ_BLOB_ERROR("Cannot read past the file size", filename);
   return TILEDB_FS_ERR;
  }
}

// This method is based on upload_block_blob_from_buffer from the SDK except for the put_block_list stage which happens in the commit_path() now
std::future<storage_outcome<void>> AzureBlob::upload_block_blob(const std::string &blob, uint64_t block_size, int num_blocks, std::vector<std::string> block_ids,
                                                                const char* buffer, uint64_t bufferlen, uint parallelism) {
  auto bclient = reinterpret_cast<blob_client *>(bC.get());
  parallelism = std::min(parallelism, bclient->concurrency());

  struct concurrent_task_info {
    std::string blob;
    const char* buffer;
    uint64_t blob_size;
    uint64_t block_size;
    int num_blocks;
    std::vector<std::pair<std::string, std::string>> metadata;
  };

  struct concurrent_task_context {
    std::atomic<int> num_workers{ 0 };
    std::atomic<int> block_index{ 0 };
    std::atomic<bool> failed{ false };
    storage_error failed_reason;

    std::promise<storage_outcome<void>> task_promise;
    std::vector<std::future<void>> task_futures;
  };

  auto info = std::make_shared<concurrent_task_info>(concurrent_task_info{ blob, buffer, bufferlen, block_size, num_blocks, empty_metadata });
  auto context = std::make_shared<concurrent_task_context>();
  context->num_workers = parallelism;

  auto thread_upload_func = [this, block_ids, info, context]() {
    while (true) {
      int i = context->block_index.fetch_add(1);
      if (i >= info->num_blocks || context->failed) {
        break;
      }
      const char* block_buffer = info->buffer + info->block_size * i;
      uint64_t block_size = std::min(info->block_size, info->blob_size - info->block_size * i);
      auto result = this->bC.get()->upload_block_from_buffer(container_name, info->blob, block_ids[i], block_buffer, block_size).get();

      if (!result.success() && !context->failed.exchange(true)) {
        context->failed_reason = result.error();
      }
    }
    if (context->num_workers.fetch_sub(1) == 1) {
      // I'm the last worker thread
      context->task_promise.set_value(context->failed ? storage_outcome<void>(context->failed_reason) : storage_outcome<void>());
    }
  };

  for (uint i = 0; i < parallelism; ++i) {
    context->task_futures.emplace_back(std::async(std::launch::async, thread_upload_func));
  }

  return context->task_promise.get_future();
}

int AzureBlob::write_to_file(const std::string& filename, const void *buffer, size_t buffer_size) {
  std::string path = get_path(filename);
  auto bclient = reinterpret_cast<blob_client *>(bC.get());
  if (buffer_size == 0) {
    const std::lock_guard<std::mutex> lock(write_map_mtx_);
    if (!bc->blob_exists(container_name, path)) {
      auto result = bclient->create_append_blob(container_name, path).get();
      if (!result.success()) {
        AZ_BLOB_ERROR("Could not create zero length file", path);
        return TILEDB_FS_ERR;
      } else {
        return TILEDB_FS_OK;
      }
    }
  }

  if (!is_dir(parent_dir(NULL, filename))) {
    AZ_BLOB_ERROR("Parent dir does not seem to exist", path);
    return TILEDB_FS_ERR;
  }

  if (buffer_size > constants::max_num_blocks * constants::max_block_size) {
    AZ_BLOB_ERROR("Buffer size too large for azure upload", path);
    return TILEDB_FS_ERR;
  }

  uint64_t block_size = buffer_size/constants::max_num_blocks;
  block_size = (block_size + GRAIN_SIZE-1)/GRAIN_SIZE*GRAIN_SIZE;
  block_size = std::min(block_size, constants::max_block_size);
  block_size = std::max(block_size, constants::default_block_size);
  int num_blocks = int((buffer_size + block_size-1)/block_size);

  auto res = upload_block_blob(path, block_size, num_blocks, generate_block_ids(path, num_blocks),
                               reinterpret_cast<const char *>(buffer), buffer_size, std::thread::hardware_concurrency()/2).get();
  if (!res.success()) {
    AZ_BLOB_ERROR(res.error().message, path);
    return TILEDB_FS_ERR;
  }

  return TILEDB_FS_OK;
}

int AzureBlob::move_path(const std::string& old_path, const std::string& new_path) {
  throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "TBD: No support for moving path");
}

int AzureBlob::commit_file(const std::string& path) {
  auto bclient = reinterpret_cast<blob_client *>(bC.get());
  int rc = TILEDB_FS_OK;
  std::string filepath = get_path(path);

  const std::lock_guard<std::mutex> lock(write_map_mtx_);

  auto search = write_map_.find(filepath);
  if (search != write_map_.end()) {
    std::vector<std::pair<std::string, std::string>> empty_metadata;
    auto put_result = bclient->put_block_list(container_name, filepath, search->second, empty_metadata).get();
    if (!put_result.success()) {
      AZ_BLOB_ERROR("Could not sync path with put_block_list", filepath);
      rc = TILEDB_FS_ERR;
    }
    write_map_.erase(search->first);
  }
  return rc;
}

int AzureBlob::sync_path(const std::string& path) {
  return commit_file(path);
}

int AzureBlob::close_file(const std::string& filename) {
  return commit_file(filename);
}

bool AzureBlob::locking_support() {
  // No file locking available for distributed file systems
  return false;
}
