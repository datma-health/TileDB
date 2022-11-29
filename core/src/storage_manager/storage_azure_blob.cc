/**
 * @file   storage_azure_blob.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019-2021 Omics Data Automation, Inc.
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
#include "uri.h"
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

static std::string run_command(const std::string& command) {
  std::string output;
  std::array<char, 2048> buffer;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(command.data(), "r"), pclose);
  if (pipe) {
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
      output += buffer.data();
    }
  }
  return output;
}

static std::string get_account_key(const std::string& account_name) {
  // Try environment variables AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY first
  char *az_storage_ac_name = getenv("AZURE_STORAGE_ACCOUNT");
  if (!az_storage_ac_name || (az_storage_ac_name && account_name.compare(az_storage_ac_name) == 0)) {
    char *key = getenv("AZURE_STORAGE_KEY");
    if (key) {
      return key;
    }
  }

  // Try via az CLI `az storage account keys list -o tsv --account-name <account_name>`
  std::string keys = run_command("az storage account keys list -o tsv --account-name " + account_name);
  std::string account_key("");

  if (keys.length() > 1) {
    // Get the first key available
    std::string first_key("key1\tFULL\t");
    auto start_pos = first_key.length();
    auto end_pos = keys.find("\n");
    if (keys.find(first_key) != std::string::npos) {
      if (end_pos == std::string::npos) {
        account_key = keys.substr(start_pos);
      } else {
        account_key = keys.substr(start_pos, end_pos-start_pos);
      }
    }
  }

  return account_key;
}

static std::string get_sas_token(const std::string& account_name) {
  // Try environment variables AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY first
  char *az_storage_ac_name = getenv("AZURE_STORAGE_ACCOUNT");
  if (!az_storage_ac_name || (az_storage_ac_name && account_name.compare(az_storage_ac_name) == 0)) {
    char *key = getenv("AZURE_STORAGE_SAS_TOKEN");
    if (key) {
      return key;
    }
  }
  return "";
}

static std::string get_blob_endpoint() {
  // Get enviroment variable for AZURE_BLOB_ENDPOINT
  std::string az_blob_endpoint("");
  char *az_blob_endpoint_env = getenv("AZURE_BLOB_ENDPOINT");
  if (az_blob_endpoint_env) {
    az_blob_endpoint = az_blob_endpoint_env;
  }
  return az_blob_endpoint;
}

static std::string get_access_token(const std::string& account_name, const std::string& path) {
  // Invoke `az account get-access-token --resource https://<account>.blob.core.windows.net -o tsv --query accessToken`
  // Can probably use `az account get-access-token --resource https://storage.azure.com/ -o tsv --query accessToken` too
  std::string token;
  std::string resource_url = "https://" + account_name + ".blob.core.windows.net";
  std::string command =  "az account get-access-token --resource " + resource_url + " -o tsv --query accessToken";
  return run_command(command);
}

std::string AzureBlob::get_path(const std::string& path) {
  std::string pathname(path);
  if (path.find("://") != std::string::npos) {
    azure_uri uri_path(path);
    pathname = uri_path.path();
     // This is the container
    if (pathname.empty()) {
      return "";
    }
  }
  if (pathname[0] == '/') {
    return pathname.substr(1);
  }
  if (pathname.empty()) {
    return working_dir_;
  } else if (starts_with(pathname, working_dir_)) {
    // TODO: this is a hack for now, but should work fine with GenomicsDB.
    return pathname;
  } else {
    return working_dir_ + '/' + pathname;
  }
}

AzureBlob::AzureBlob(const std::string& home) {
  azure_uri path_uri(home);

  // az://<container_name>@<blob_storage_account_name>.blob.core.windows.net/<path>
  // e.g. az://test@mytest.blob.core.windows.net/ws
  if (path_uri.protocol().compare("az") != 0) {
    throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "Azure Blob FS only supports az:// URI protocols");
  }

  if (path_uri.account().size() == 0 || path_uri.container().size() == 0) {
    throw std::system_error(EPROTO, std::generic_category(), "Azure Blob URI does not seem to have either an account or a container");
  }

  // Algorithm to get azure storage credentials. Try AZURE_STORAGE_ACCOUNT_KEY first, followed by AZURE_STORAGE_SAS_TOKEN and last
  // try getting an access token directly from CLI
  std::shared_ptr<storage_credential> cred = nullptr;
  std::string azure_account = path_uri.account();
  std::string azure_account_key = get_account_key(azure_account);
  if (!azure_account_key.empty()) {
    cred = std::make_shared<shared_key_credential>(azure_account, azure_account_key);
  } else {
    std::string azure_sas_token = get_sas_token(azure_account);
    if (!azure_sas_token.empty()) {
      cred = std::make_shared<shared_access_signature_credential>(azure_sas_token);
    } else {
      // Last ditch effort, try getting an access token directly from CLI.
      AZ_BLOB_ERROR("Could not authenticate via AZURE_STORAGE_KEY or AZURE_STORAGE_SAS_TOKEN env vars. " +
               "Trying to get access token directly via CLI", home);
      std::string token = get_access_token(azure_account, home);
      if (token.size() > 0) {
        cred = std::make_shared<token_credential>(token);
      }
    }
  }
  
  if (cred == nullptr) {
    throw std::system_error(EIO, std::generic_category(), "Could not get credentials for azure storage account=" + azure_account + ". " +
                            "Try setting environment variables AZURE_STORAGE_KEY or AZURE_STORAGE_SAS_TOKEN before restarting operation");
  }

  std::shared_ptr<storage_account> account = std::make_shared<storage_account>(azure_account, cred, /* use_https */true, get_blob_endpoint());

  std::string ca_certs_location = locate_ca_certs();
  if (ca_certs_location.empty()) {
    blob_client_ = std::make_shared<blob_client>(account, std::thread::hardware_concurrency());
  } else {
    blob_client_ = std::make_shared<blob_client>(account, std::thread::hardware_concurrency(), ca_certs_location);
  }
  
  bc_wrapper_ = std::make_shared<blob_client_wrapper>(blob_client_);
  blob_client_wrapper_ = reinterpret_cast<blob_client_wrapper *>(bc_wrapper_.get());

  if (!blob_client_wrapper_->container_exists(path_uri.container())) {
      AZ_BLOB_ERROR("Container does not seem to exist", path_uri.container());
      throw std::system_error(EIO, std::generic_category(), "AzureBlobFS only supports accessible and already existing containers");
  }

  account_name_ = path_uri.account();
  container_name_ = path_uri.container();

  working_dir_ = get_path(path_uri.path());

  adls_client_ = std::make_shared<azure::storage_adls::adls_client>(account, std::thread::hardware_concurrency()/2, false);

  // Set default buffer sizes, overridden with env vars TILEDB_DOWNLOAD_BUFFER_SIZE and TILEDB_UPLOAD_BUFFER_SIZE
  download_buffer_size_ = constants::default_block_size; // 8M
  upload_buffer_size_ = constants::default_block_size; // 8M

  auto max_stream_size_var = getenv("TILEDB_MAX_STREAM_SIZE");
  if (max_stream_size_var) {
    max_stream_size = std::stoll(max_stream_size_var);
  }
}

std::string AzureBlob::current_dir() {
  return working_dir_;
}

int AzureBlob::set_working_dir(const std::string& dir) {
  working_dir_ = get_path(dir);
  return TILEDB_FS_OK;
}

bool AzureBlob::path_exists(const std::string& path) {
  auto blob_property = blob_client_wrapper_->get_blob_property(container_name_, get_path(path));
  if (blob_property.valid()) {
    if (blob_property.content_type.empty() && path.back() == '/') {
      return true;
    } else if (!blob_property.content_type.empty() && path.back() != '/') {
      return true;
    }
  } else if (path.back() == '/') {
    // Check directories in non-hierarchical namespaces by checking for children as they are not explicitly
    // created as in hierarchical namespaces
    auto response = blob_client_wrapper_->list_blobs_segmented(container_name_, "/",  "", get_path(path), 1);
    return response.blobs.size() > 0;
  }
  return false;
}

std::string AzureBlob::real_dir(const std::string& dir) {
  if (dir.find("://") != std::string::npos) {
    azure_uri path_uri(dir);
    if (path_uri.account().compare(account_name_) || path_uri.container().compare(container_name_)) {
      throw std::runtime_error("Credentialed account during instantiation does not match the uri passed to real_dir. Aborting");
    }
  }
  return get_path(dir);
}

int AzureBlob::create_path(const std::string& path) {
  return write_to_file(path, NULL, 0);
}
  
int AzureBlob::create_dir(const std::string& dir) {
  if (is_file(dir)) {
    AZ_BLOB_ERROR("Path already exists", dir);
    return TILEDB_FS_ERR;
  }
  // No support in the azure lite client to create a folder
  // no-op for this case for now
  return TILEDB_FS_OK;
}

int AzureBlob::delete_dir(const std::string& dir) {
  int rc = TILEDB_FS_OK;
  adls_client_->delete_directory(container_name_, get_path(dir));
  if (errno > 0) {
    // Try again using the blob client directly for non-hierarchical filesystems
    std::string continuation_token = "";
    auto bclient = reinterpret_cast<blob_client *>(blob_client_.get());
    auto response = blob_client_wrapper_->list_blobs_segmented(container_name_, "/",  continuation_token,
                                                               slashify(get_path(dir)), INT_MAX);
    do {
      for (auto i=0u; i<response.blobs.size(); i++) {
        if (response.blobs[i].is_directory) {
          delete_dir(response.blobs[i].name);
        } else {
          auto result = bclient->delete_blob(container_name_, response.blobs[i].name, false).get();
          if (!result.success()) {
            AZ_BLOB_ERROR(result.error().message, response.blobs[i].name);
            rc = TILEDB_FS_ERR;
          }
        }
      }
    } while (!continuation_token.empty());
  }
  return rc;
}

std::vector<std::string> AzureBlob::get_dirs(const std::string& dir) {
  std::vector<std::string> dirs;
  std::string continuation_token = "";
  auto response = blob_client_wrapper_->list_blobs_segmented(container_name_, "/",  continuation_token, slashify(get_path(dir)), INT_MAX);
  do {
    for (auto i=0u; i<response.blobs.size(); i++) {
        if (response.blobs[i].is_directory) {
          dirs.push_back(unslashify(response.blobs[i].name));
        }
    }
  } while (!continuation_token.empty());
  return dirs;
}
    
std::vector<std::string> AzureBlob::get_files(const std::string& dir) {
  std::vector<std::string> files;
  std::string continuation_token = "";
  auto response = blob_client_wrapper_->list_blobs_segmented(container_name_, "/",  continuation_token, slashify(get_path(dir)), INT_MAX);
  do {
    for (auto i=0u; i<response.blobs.size(); i++) {
      if (!response.blobs[i].is_directory) {
        files.push_back(response.blobs[i].name);
      }
    }
  } while (!continuation_token.empty());
  return files;
}

int AzureBlob::create_file(const std::string& filename, int flags, mode_t mode) {
  if (is_file(filename)) {
    AZ_BLOB_ERROR("Cannot create path as it already exists", filename);
    return TILEDB_FS_ERR;
  }
  return create_path(filename);
}

int AzureBlob::delete_file(const std::string& filename) {
  if (!is_file(filename)) {
    AZ_BLOB_ERROR("Cannot delete non-existent or non-file path", filename);
    return TILEDB_FS_ERR;
  }
  blob_client_wrapper_->delete_blob(container_name_, get_path(filename));
  return TILEDB_FS_OK;
}

ssize_t AzureBlob::file_size(const std::string& filename) {
  auto blob_property = blob_client_wrapper_->get_blob_property(container_name_, get_path(filename));
  if (blob_property.valid() && blob_property.content_type == "application/octet-stream") {
#ifdef DEBUG
    if (filename.find_last_of(".json") != std::string::npos) {
      std::cerr << "Blob " << filename << " md5=" << blob_property.content_md5 << " size=" << blob_property.size<< std::endl;
    }
#endif
    return blob_property.size;
  } else {
#ifdef DEBUG
    std::cerr << "No blob properties found for file=" << filename << std::endl;
#endif
    return TILEDB_FS_ERR;
  }
  return TILEDB_FS_OK;
}

int AzureBlob::read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length) {
  if (length == 0) {
    return TILEDB_FS_OK; // Nothing to read
  }
  std::string path = get_path(filename);
  auto bclient = reinterpret_cast<blob_client *>(blob_client_.get());
  storage_outcome<void> read_result;
  // Heuristic: if the file can be contained in a block use download_blob_to_stream(), otherwise use the parallel download_blob_to_buffer()
  if (length <= max_stream_size) {
    omemstream os_buf(buffer, length);
    read_result = bclient->download_blob_to_stream(container_name_, path, offset, length, os_buf).get();
  } else {
    read_result = bclient->download_blob_to_buffer(container_name_, path, offset, length, reinterpret_cast<char *>(buffer), std::thread::hardware_concurrency()/2).get();
  }
  if (!read_result.success()) {
    AZ_BLOB_ERROR(read_result.error().message, filename);
    return TILEDB_FS_ERR;
  } else {
    return TILEDB_FS_OK;
  }
}

#define GRAIN_SIZE (4*1024*1024)
// This method is based on upload_block_blob_from_buffer from the SDK except for the put_block_list stage which happens in commit_path() now
std::future<storage_outcome<void>> AzureBlob::upload_block_blob(const std::string &blob, uint64_t block_size,
                                                                int num_blocks, std::vector<std::string> block_ids,
                                                                const char* buffer, uint64_t bufferlen, uint parallelism) {
  auto bclient = reinterpret_cast<blob_client *>(blob_client_.get());
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
      auto result = this->blob_client_.get()->upload_block_from_buffer(container_name_, info->blob, block_ids[i], block_buffer, block_size).get();

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
  auto bclient = reinterpret_cast<blob_client *>(blob_client_.get());
  if (buffer_size == 0) {
    const std::lock_guard<std::mutex> lock(write_map_mtx_);
    if (!blob_client_wrapper_->blob_exists(container_name_, path)) {
      auto result = bclient->create_append_blob(container_name_, path).get();
      if (!result.success()) {
        AZ_BLOB_ERROR("Could not create zero length file", path);
        return TILEDB_FS_ERR;
      } else {
        return TILEDB_FS_OK;
      }
    }
    return TILEDB_FS_OK;
  }

  if (buffer_size > constants::max_num_blocks * constants::max_block_size) {
    AZ_BLOB_ERROR("Buffer size too large for azure upload", path);
    return TILEDB_FS_ERR;
  }

#ifdef DEBUG
  update_expected_filesizes_map(path, buffer_size);
#endif

  uint64_t block_size = buffer_size/constants::max_num_blocks;
  block_size = (block_size + GRAIN_SIZE-1)/GRAIN_SIZE*GRAIN_SIZE;
  block_size = std::min(block_size, constants::max_block_size);
  block_size = std::max(block_size, constants::default_block_size);
  int num_blocks = int((buffer_size + block_size-1)/block_size);

  auto block_ids = generate_block_ids(path, num_blocks);
  if (block_ids.size() == 0) {
    AZ_BLOB_ERROR("Could not get block_ids for upload_block_blob", path);
    return TILEDB_FS_ERR;
  } 
  auto res = upload_block_blob(path, block_size, num_blocks, block_ids,
                               reinterpret_cast<const char *>(buffer), buffer_size,
                               std::thread::hardware_concurrency()/2).get();
  if (!res.success()) {
    AZ_BLOB_ERROR(res.error().message, path);
    return TILEDB_FS_ERR;
  }

  return TILEDB_FS_OK;
}

int AzureBlob::commit_file(const std::string& path) {
  auto bclient = reinterpret_cast<blob_client *>(blob_client_.get());
  int rc = TILEDB_FS_OK;
  std::string filepath = get_path(path);

  const std::lock_guard<std::mutex> lock(write_map_mtx_);

  auto search = write_map_.find(filepath);
  if (search != write_map_.end()) {
    std::vector<std::pair<std::string, std::string>> empty_metadata;
    auto put_result = bclient->put_block_list(container_name_, filepath, search->second, empty_metadata).get();
    if (!put_result.success()) {
      AZ_BLOB_ERROR("Could not sync path with put_block_list", filepath);
      rc = TILEDB_FS_ERR;
    }
    write_map_.erase(search->first);
 #ifdef DEBUG
    if (!rc) {
      // Checks after commit, should we wait for propogation?
      if (!is_file(filepath)) {
        AZ_BLOB_ERROR("Could not find file after commit", path);
        rc = TILEDB_FS_ERR;
      } else {
        ssize_t expected = expected_filesize_from_map(filepath);
        ssize_t actual = file_size(path);
        if (expected != actual) {
          AZ_BLOB_ERROR("filesize after commit does not match with the expected filesize", path);
          std::string msg = std::string("expected=") + std::to_string(expected) + " actual=" + std::to_string(actual);
          AZ_BLOB_ERROR(msg, path);
          rc = TILEDB_FS_ERR;
        }
      }
    }
    filesizes_map_.erase(filepath);
#endif
  }
  
  return rc;
}

