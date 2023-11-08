/**
 * @file storage_fs.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2020 Omics Data Automation, Inc.
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
 * Storage FS Base Implementation
 *
 */

#include "storage_fs.h"
#include "utils.h"

#include <stdlib.h>
#include <string.h>
#include <strings.h>

/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_fs_errmsg = "";

StorageFS::~StorageFS() {
  // Default
}

int StorageFS::close_file(const std::string& filename) {
  return TILEDB_FS_OK;
}

bool StorageFS::locking_support() {
  return false;
}

std::string StorageCloudFS::get_path(const std::string& path) {
  std::string pathname(path);
  if (path.find("://") != std::string::npos) {
    uri path_uri(path);
    pathname = path_uri.path();
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
    return append_paths(working_dir_, pathname);
  }
}

int StorageCloudFS::sync_path(const std::string& path) {
  // S3 and GCS allow for only write-once semantics, so committing the file will
  // happen only when the file is closed or when commit_file() is explicitly called
  // Forced write-once semantics for Azure Blob Storage too.
  return TILEDB_FS_OK;
}

int StorageCloudFS::close_file(const std::string& filename) {
  return commit_file(filename);
}

