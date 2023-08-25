/**
 * @file storage_fs.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2019 Omics Data Automation Inc. and Intel Corporation
 * @copyright Copyright (c) 2018-2019 Omics Data Automation Inc. and Intel Corporation
 * @copyright Copyright (c) 2023 dātma, inc™
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
 * Storage API exposes some filesystem specific functionality implemented in TileDB.
 *
 */

#ifndef __STORAGE_FS_H__
#define  __STORAGE_FS_H__

#include "uri.h"

#include <cstring>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <system_error>
#include <vector>

/**@{*/
/** Return code. */
#define TILEDB_FS_OK         0
#define TILEDB_FS_ERR       -1
/**@}*/

/** Default error message. */
#define TILEDB_FS_ERRMSG std::string("[TileDB::FileSystem] Error: ")

/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Stores potential error messages. */
extern std::string tiledb_fs_errmsg;

/** Base Class for Filesystems */
class StorageFS {
 public:
  virtual ~StorageFS();

  virtual std::string current_dir() = 0;
  virtual int set_working_dir(const std::string& dir) = 0;
  
  virtual bool is_dir(const std::string& dir) = 0;
  virtual bool is_file(const std::string& file) = 0;
  virtual std::string real_dir(const std::string& dir) = 0;
  
  virtual int create_dir(const std::string& dir) = 0;
  virtual int delete_dir(const std::string& dir) = 0;

  virtual std::vector<std::string> get_dirs(const std::string& dir) = 0;
  virtual std::vector<std::string> get_files(const std::string& dir) = 0;

  virtual int create_file(const std::string& filename, int flags, mode_t mode) = 0;
  virtual int delete_file(const std::string& filename) = 0;

  virtual ssize_t file_size(const std::string& filename) = 0;

  virtual int read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length) = 0;
  virtual int write_to_file(const std::string& filename, const void *buffer, size_t buffer_size) = 0;

  virtual int move_path(const std::string& old_path, const std::string& new_path) = 0;
    
  virtual int sync_path(const std::string& path) = 0;

  virtual int close_file(const std::string& filename);

  virtual bool locking_support();

  size_t get_download_buffer_size() {
    auto env_var = getenv("TILEDB_DOWNLOAD_BUFFER_SIZE");
    if (env_var) {
      return std::stoull(env_var);
    } else {
      return download_buffer_size_;
    }
  }
  
  size_t get_upload_buffer_size() {
    auto env_var = getenv("TILEDB_UPLOAD_BUFFER_SIZE");
    if (env_var) {
      return std::stoull(env_var);
    } else {
      return upload_buffer_size_;
    }
  }

  void set_download_buffer_size(size_t buffer_size) {
    download_buffer_size_ = buffer_size;
  }

  void set_upload_buffer_size(size_t buffer_size) {
    upload_buffer_size_ = buffer_size;
  }

  static inline std::string slashify(const std::string& path) {
    if (path.empty()) {
      return "/";
    } else if (path.back() != '/') {
      return path + '/';
    } else {
      return path;
    }
  }

  static inline std::string unslashify(const std::string& path) {
    if (!path.empty() && path.back() == '/') {
      return path.substr(0, path.size()-1);
    } else {
      return path;
    }
  }

  static inline std::string append_paths(const std::string& path1, const std::string& path2) {
    return slashify(path1) + path2;
  }

  size_t download_buffer_size_ = 0;
  size_t upload_buffer_size_ = 0;
};

class StorageCloudFS : public virtual StorageFS {

#define DELIMITER "/"

 public:
  int create_dir(const std::string& dir) {
    // no-op
    return TILEDB_FS_OK;
  }

  bool is_dir(const std::string& dir) {
    if (get_path(dir).length() == 0) {
      // This must be the container - OK
      return true;
    }
    return path_exists(slashify(dir));
  }
  
  bool is_file(const std::string& file) {
    return path_exists(unslashify(file));
  }

  int move_path(const std::string& old_path, const std::string& new_path) {
    throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "TBD: No support for moving path");
  }

  int sync_path(const std::string& path);

  int close_file(const std::string& filename);

 protected:
  std::string get_path(const std::string& path);

  virtual bool path_exists(const std::string& path) = 0;
  virtual int create_path(const std::string& path) = 0;

  virtual int commit_file(const std::string& filename) = 0;

#ifdef __linux__
  // Possible certificate files; stop after finding one.
  // https://github.com/golang/go/blob/f0e940ebc985661f54d31c8d9ba31a553b87041b/src/crypto/x509/root_linux.go#L7
  const std::vector<std::string> possible_ca_certs_locations = {
    "/etc/ssl/certs/ca-certificates.crt",                // Debian/Ubuntu/Gentoo etc.
    "/etc/pki/tls/certs/ca-bundle.crt",                  // Fedora/RHEL 6
    "/etc/ssl/ca-bundle.pem",                            // OpenSUSE
    "/etc/pki/tls/cacert.pem",                           // OpenELEC
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", // CentOS/RHEL 7
    "/etc/ssl/cert.pem",                                 // Alpine Linux
  };

  std::string locate_ca_certs() {
    for (const std::string& location : possible_ca_certs_locations) {
      struct stat st;
      memset(&st, 0, sizeof(struct stat));
      if (!stat(location.c_str(), &st) && S_ISREG(st.st_mode)) {
	return location;
      }
    }
#ifdef DEBUG
    std::cerr << "CA Certs path not located. Using system defaults" << std::endl;
#endif
    return "";
  }
#else
  std::string locate_ca_certs() {
    return "";
  }
#endif

 protected:
  std::string working_dir_;
};

#endif /* __STORAGE_FS_H__ */

