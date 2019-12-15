/**
 * @file   storage_azure_blob.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 Omics Data Automation, Inc.
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

#include <array>
#include <assert.h>
#include <cerrno>
#include <clocale>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdlib.h>
#include <unistd.h>

#ifdef __APPLE__
#if !defined(ELIBACC)
#define ELIBACC -1
#endif
#endif

#define AZ_BLOB_ERROR(MSG, PATH) SYSTEM_ERROR(TILEDB_FS_ERRMSG, MSG, PATH, tiledb_fs_errmsg)

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
  }
  if (pathname[0] == '/') {
    pathname = pathname.substr(1);
  }

  if (!starts_with(pathname, working_dir)) {
    return working_dir + '/' + pathname;
  } else {
    return pathname;
  }
}

// TODO: REMOVE: CMD: ./test_tiledb_utils --test-dir wasbs://build@oda.blob.core.windows.net/nalini_test
// TODO: Add robust error checking
AzureBlob::AzureBlob(const std::string& home) {
  azure_url path_url(home);

  // wasbs://<container_name>@<blob_storage_account_name>.blob.core.windows.net/<path>
  // e.g. wasbs://test@mytest.blob.core.windows.net/ws
  if (path_url.protocol().compare("wasbs") != 0 && path_url.protocol().compare("wasb") != 0) {
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
  bC = std::make_shared<blob_client>(account, 10);
  bc_wrapper = std::make_shared<blob_client_wrapper>(bC);
  bc = reinterpret_cast<blob_client_wrapper *>(bc_wrapper.get());

  account_name = path_url.account();
  container_name = path_url.container();
  working_dir = get_path(path_url.path());
  if (!is_dir(working_dir)) {
    create_dir(working_dir);
  }
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
  /*  auto response = bc->list_blobs_segmented(container_name, "/", "", slashify(dir), INT_MAX);
  if (!response.next_marker.empty()) {
    throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "TBD: No support for continuation tokens");
  }
  if (response.blobs.size() > 0) {
    for (auto i=0u; i<response.blobs.size(); i++) {
      if (response.blobs[i].name.compare(dir) == 0) {
        return response.blobs[i].is_directory;
      }
    }
    return true;
  } else {
    return false;
    } */
  // TODO: is_dir will look for a marker file for now
  return is_file(slashify(get_path(dir))+MARKER);
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
  // TODO: Marker file for now with metadata is_dir=true
  return write_to_file(slashify(get_path(dir))+MARKER, NULL, 0);
}

int AzureBlob::delete_dir(const std::string& dir) {
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
  auto response = bc->list_blobs_segmented(container_name, "/", "", slashify(get_path(dir)), INT_MAX);
  if (!response.next_marker.empty()) {
    throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "TBD: No support for continuation tokens");
  }
  std::vector<std::string> dirs;
  for (int i=0; i<response.blobs.size(); i++) {
    if (response.blobs[i].is_directory) {
      dirs.push_back(unslashify(response.blobs[i].name));
    }
  }
  return dirs;
}
    
std::vector<std::string> AzureBlob::get_files(const std::string& dir) {
  auto response = bc->list_blobs_segmented(container_name, "/", "", slashify(get_path(dir)), INT_MAX);
  if (!response.next_marker.empty()) {
    throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "TBD: No support for continuation tokens");
  }
  std::vector<std::string> files;
  for (int i=0; i<response.blobs.size(); i++) {
    if (!response.blobs[i].is_directory) {
      if (response.blobs[i].name.compare(MARKER) != 0) {
        files.push_back(response.blobs[i].name);
      }
    }
  }
  return files;
}

int AzureBlob::create_file(const std::string& filename, int flags, mode_t mode) {
  return write_to_file(get_path(filename), NULL, 0);
}

int AzureBlob::delete_file(const std::string& filename) {
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

#define MAX_BYTES 4*1024*1024

// TODO: Implement reading in chunks of MAX_BYTES like in write_to_file(), but asynchronously.
int AzureBlob::read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length) {
  std::string path = get_path(filename);
  auto bclient = reinterpret_cast<blob_client *>(bC.get());
  if (bc->blob_exists(container_name, path)) {
    omemstream os_buf(buffer, length);
    auto read_result = bclient->download_blob_to_stream(container_name, path, offset, length, os_buf).get();
    if (!read_result.success()) {
      AZ_BLOB_ERROR(read_result.error().message, filename);
      return TILEDB_FS_ERR;
    } else {
      return TILEDB_FS_OK;
    }
  } else {
    AZ_BLOB_ERROR("File does not exist", filename);
    return TILEDB_FS_ERR;
  }
}

int AzureBlob::write_to_file_kernel(const std::string& filename, const void *buffer, size_t buffer_size) {
  std::string path = get_path(filename);
  auto bclient = reinterpret_cast<blob_client *>(bC.get());
  imemstream is_buf(buffer, buffer_size);
  auto append_result = bclient->append_block_from_stream(container_name, path, is_buf, buffer_size).get();
  if (!append_result.success()) {
    AZ_BLOB_ERROR(append_result.error().message, filename);
    return TILEDB_FS_ERR;
  } else {
    return TILEDB_FS_OK;
  }
}

int AzureBlob::write_to_file(const std::string& filename, const void *buffer, size_t buffer_size) {
  std::string path = get_path(filename);
  auto bclient = reinterpret_cast<blob_client *>(bC.get());
  if (!bc->blob_exists(container_name, path)) {
    auto result = bclient->create_append_blob(container_name, path).get();
    if (!result.success()) {
      return TILEDB_FS_ERR;
    }
  }

  int rc = TILEDB_FS_OK;
  if (buffer != NULL && buffer_size > 0) {
    size_t nbytes = 0;
    char *pbuf = (char *)buffer;
    do {
      size_t bytes_to_write = (buffer_size - nbytes) > MAX_BYTES ? MAX_BYTES : buffer_size - nbytes;
      rc = write_to_file_kernel(path, pbuf, bytes_to_write);
      pbuf += nbytes;
      nbytes += bytes_to_write;
    } while (!rc && nbytes < buffer_size);
  }

  return rc;
}

int AzureBlob::move_path(const std::string& old_path, const std::string& new_path) {
  throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "TBD: No support for moving path");
}
    
int AzureBlob::sync_path(const std::string& path) {
  return TILEDB_FS_OK;
}

bool AzureBlob::locking_support() {
  // No file locking available for distributed file systems
  return false;
}
