/**
 * @file tiledb_utils.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 Omics Data Automation Inc. and Intel Corporation
 * @copyright Copyright (c) 2019-2021 Omics Data Automation Inc.
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
 * Utility functions to access TileDB functionality in an opaque fashion. These 
 * convenience functions do not require TileDB_CTX handle.
 *
 */

#include "tiledb_utils.h"

#include "codec.h"
#include "tiledb_storage.h"
#include "storage_fs.h"

#include <cstring>
#include <error.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <trace.h>

namespace TileDBUtils {

static int setup(TileDB_CTX **ptiledb_ctx, const std::string& home,
                 const bool enable_shared_posixfs_optimizations=false)
{
  int rc;
  TileDB_Config tiledb_config = {};
  tiledb_config.home_ = strdup(home.c_str());
  tiledb_config.enable_shared_posixfs_optimizations_ = enable_shared_posixfs_optimizations;
  rc = tiledb_ctx_init(ptiledb_ctx, &tiledb_config);
  free(const_cast<char*>(tiledb_config.home_));
  return rc;
}

static int finalize(TileDB_CTX *tiledb_ctx)
{
  return tiledb_ctx_finalize(tiledb_ctx);
}

bool is_cloud_path(const std::string& path) {
  return path.find("://") != std::string::npos;
}

std::string get_path(const std::string &workspace) {
  std::size_t check_cloud = workspace.find("://");
  if (check_cloud != std::string::npos &&
      workspace.substr(0, check_cloud).compare("hdfs") != 0)
    return uri(workspace).path();
  return workspace;
}

std::string append_path(std::string dir, std::string path) {
  std::size_t query_pos = dir.find('?');
  if (query_pos == std::string::npos) return StorageFS::slashify(dir) + path;

  return StorageFS::slashify(dir.substr(0, query_pos)) + path +
         dir.substr(query_pos);
}

/**
 * Returns 0 when workspace is created
 *        -1 when path is not a directory
 *        -2 when workspace could not be created
 *         1 when workspace exists and nothing is changed
 */
#define OK 0
#define NOT_DIR -1
#define NOT_CREATED -2
#define UNCHANGED 1
int initialize_workspace(TileDB_CTX **ptiledb_ctx, const std::string& workspace, const bool replace,
                         const bool enable_shared_posixfs_optimizations)
{
  *ptiledb_ctx = NULL;
  int rc;
  rc = setup(ptiledb_ctx, workspace, enable_shared_posixfs_optimizations);
  std::string workspace_path = get_path(workspace);
  if (rc) {
    return NOT_CREATED;
  }

  if (is_file(*ptiledb_ctx, workspace_path)) {
    return NOT_DIR;
  }

  if (is_workspace(*ptiledb_ctx, workspace_path)) {
    if (replace) {
      if (is_dir(*ptiledb_ctx, workspace_path) && delete_dir(*ptiledb_ctx, workspace_path) ) {
        return NOT_CREATED;
      }
    } else {
      return UNCHANGED;
    }
  }

  rc = tiledb_workspace_create(*ptiledb_ctx, workspace_path.c_str());
  if (rc != TILEDB_OK) {
    rc = NOT_CREATED;
  } else {
    rc = OK;
  }

  return rc;
}

#define FINALIZE            \
  do {                      \
    if (tiledb_ctx) {       \
      finalize(tiledb_ctx); \
    }                       \
  } while(false)

int create_workspace(const std::string& workspace, bool replace)
{
  TileDB_CTX *tiledb_ctx;
  int rc = initialize_workspace(&tiledb_ctx, workspace, replace);
  FINALIZE;
  return rc;
}

bool workspace_exists(const std::string& workspace)
{
  bool exists = false;
  TileDB_CTX *tiledb_ctx;
  int rc = setup(&tiledb_ctx, workspace);
  exists = !rc && is_workspace(tiledb_ctx, get_path(workspace));
  FINALIZE;
  return exists;
}

bool array_exists(const std::string& workspace, const std::string& array_name)
{
  bool exists = false;
  TileDB_CTX *tiledb_ctx;
  int rc = setup(&tiledb_ctx, workspace);
  exists = !rc && is_array(tiledb_ctx, StorageFS::append_paths(get_path(workspace), array_name));
  FINALIZE;
  return exists;
}

std::vector<std::string> get_array_names(const std::string& workspace)
{
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, workspace)) {
    FINALIZE;
    return std::vector<std::string>{};
  }
  std::vector<std::string> array_names;
  std::vector<std::string> dirs = get_dirs(tiledb_ctx, workspace);
  if (!dirs.empty()) {
    for (std::vector<std::string>::iterator dir = dirs.begin() ; dir != dirs.end(); dir++) {
      std::string path(*dir);
      if (is_array(tiledb_ctx, path)) {
        size_t pos = path.find_last_of("\\/");
        if (pos == std::string::npos) {
          array_names.push_back(path);
        } else {
          array_names.push_back(path.substr(pos+1));
        }
      }
    }
  }
  finalize(tiledb_ctx);
  return array_names;
}

std::vector<std::string> get_fragment_names(const std::string& workspace)
{
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, workspace)) {
    FINALIZE;
    return std::vector<std::string>{};
  }
  std::vector<std::string> fragment_names;
  std::vector<std::string> dirs = get_dirs(tiledb_ctx, workspace);
  if (!dirs.empty()) {
    for (std::vector<std::string>::iterator dir = dirs.begin() ; dir != dirs.end(); dir++) {
      std::string path(*dir);
      if (is_array(tiledb_ctx, path)) {
        std::vector<std::string> fragment_dirs = get_dirs(tiledb_ctx, path);
        if (!fragment_dirs.empty()) {
          for (std::vector<std::string>::iterator fragment_dir = fragment_dirs.begin(); 
               fragment_dir != fragment_dirs.end(); fragment_dir++) {
            std::string fragment_path(*fragment_dir);
            if (is_fragment(tiledb_ctx, fragment_path)) {
              size_t pos = fragment_path.find_last_of("\\/");
              if (pos == std::string::npos) {
                fragment_names.push_back(fragment_path);
              } else {
                fragment_names.push_back(fragment_path.substr(pos+1));
              }
            }
          }
        }
      }
    }
  }
  finalize(tiledb_ctx);
  return fragment_names;
}

bool is_dir(const std::string& dirpath)
{
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(dirpath))) {
    FINALIZE;
    return false;
  }
  bool check = is_dir(tiledb_ctx, dirpath);
  finalize(tiledb_ctx);
  return check;
}

std::string real_dir(const std::string& dirpath) {
  if (is_cloud_path(dirpath)) {
    return dirpath;
  } else {
    TileDB_CTX *tiledb_ctx;
    if (setup(&tiledb_ctx, parent_dir(dirpath))) {
      FINALIZE;
      return dirpath;
    }
    std::string real_dirpath = real_dir(tiledb_ctx, dirpath);
    finalize(tiledb_ctx);
    return real_dirpath;
  }
}

int create_dir(const std::string& dirpath)
{
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(dirpath))) {
    FINALIZE;
    return TILEDB_ERR;
  }
  int rc = create_dir(tiledb_ctx, dirpath);
  finalize(tiledb_ctx);
  return rc;
}

int delete_dir(const std::string& dirpath)
{
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(dirpath))) {
    FINALIZE;
    return TILEDB_ERR;
  }
  int rc = delete_dir(tiledb_ctx, dirpath);
  finalize(tiledb_ctx);
  return rc;
}

bool is_file(const std::string& filepath) {
  PRINT_ERROR("is file tiledb utils");
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(filepath))) {
    FINALIZE;
    return false;
  }
  PRINT_ERROR("is file tiledb utils after setup");
  bool check = is_file(tiledb_ctx, filepath);
  finalize(tiledb_ctx);
  return check;
}

ssize_t file_size(const std::string& filepath) {
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(filepath))) {
    FINALIZE;
    return false;
  }
  ssize_t filesize = file_size(tiledb_ctx, filepath);
  finalize(tiledb_ctx);
  return filesize;
}

std::vector<std::string> get_dirs(const std::string& dirpath) {
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(dirpath))) {
    FINALIZE;
    return {};
  }
  auto dirs = get_dirs(tiledb_ctx, dirpath);
  finalize(tiledb_ctx);
  return dirs;
}

std::vector<std::string> get_files(const std::string& dirpath) {
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(dirpath))) {
    FINALIZE;
    return {};
  }
  auto files = get_files(tiledb_ctx, dirpath);;
  finalize(tiledb_ctx);
  return files;
}

static int check_file(TileDB_CTX *tiledb_ctx, std::string filename) {
  if (is_dir(tiledb_ctx, filename)) {
    snprintf(tiledb_errmsg, TILEDB_ERRMSG_MAX_LEN, "File path=%s exists as a directory\n", filename.c_str());
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

static int check_file_for_read(TileDB_CTX *tiledb_ctx, std::string filename) {
  if (check_file(tiledb_ctx, filename)) {
    return TILEDB_ERR;
  }
  if (!is_file(tiledb_ctx, filename) || file_size(tiledb_ctx, filename) == 0) {
    snprintf(tiledb_errmsg, TILEDB_ERRMSG_MAX_LEN, "File path=%s does not exist or is empty\n", filename.c_str());
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}


#include "tiledb_openssl_shim.h"
void print_md5_hash(unsigned char* buffer, size_t length) {
  unsigned char md[MD5_DIGEST_LENGTH];

  if(OpenSSL_version_num() < 0x30000000L) {
    MD5(buffer, length, md);
  } else {
    EVP_MD_CTX* mdctx;
    mdctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(mdctx, EVP_md5(), NULL);
    EVP_DigestUpdate(mdctx, buffer, length);
    EVP_DigestFinal_ex(mdctx, md, NULL);
    EVP_MD_CTX_free(mdctx);
  }
  for(auto i=0; i <MD5_DIGEST_LENGTH; i++) {
    fprintf(stderr, "%02x",md[i]);
  }
}

/** Allocates buffer, responsibility of caller to release buffer */
int read_entire_file(const std::string& filename, void **buffer, size_t *length) {
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(filename)) || check_file_for_read(tiledb_ctx, filename)) {
    FINALIZE;
    return TILEDB_ERR;
  }
  auto size = file_size(tiledb_ctx, filename);
  *buffer = (char *)malloc(size+1);
  if (*buffer == NULL) {
    finalize(tiledb_ctx);
    snprintf(tiledb_errmsg, TILEDB_ERRMSG_MAX_LEN, "Out-of-memory exception while allocating memory\n");
    return TILEDB_ERR;
  }
  memset(*buffer, 0, size+1);
  int rc = read_file(tiledb_ctx, filename, 0, *buffer, size);
  if (!rc) {
    *length = size;
#if 0
    // Calculate md5 hash for buffer
    std::cerr << "MD5 for buffer after reading : ";
    print_md5_hash((unsigned char *)(*buffer), size);
    std::cerr << std::endl;
#endif
    rc = close_file(tiledb_ctx, filename);
  } else {
    memset(*buffer, 0, size+1);
    free(buffer);
    *length = 0;
    rc = TILEDB_ERR;
  }
  finalize(tiledb_ctx);
  return rc;
}

int read_file(const std::string& filename, off_t offset, void *buffer, size_t length)
{
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(filename)) || check_file_for_read(tiledb_ctx, filename)) {
    FINALIZE;
    return TILEDB_ERR;
  }
  int rc = read_file(tiledb_ctx, filename, offset, buffer, length);
  rc |= close_file(tiledb_ctx, filename);
  finalize(tiledb_ctx);
  return rc;
}

int write_file(const std::string& filename, const void *buffer, size_t length, const bool overwrite)
{
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(filename)) || check_file(tiledb_ctx, filename)) {
    FINALIZE;
    return TILEDB_ERR;
  }
  int rc = TILEDB_OK;
  if (overwrite && is_file(tiledb_ctx, filename) && delete_file(tiledb_ctx, filename)) {
    finalize(tiledb_ctx);
    snprintf(tiledb_errmsg, TILEDB_ERRMSG_MAX_LEN, "File %s exists and could not be deleted for writing\n", filename.c_str());
    return TILEDB_ERR;
  }
  rc = write_file(tiledb_ctx, filename, buffer, length);
  rc |= close_file(tiledb_ctx, filename);

  finalize(tiledb_ctx);
  return rc;
}

int delete_file(const std::string& filename)
{
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(filename)) || check_file(tiledb_ctx, filename)) {
    FINALIZE;
    return TILEDB_ERR;
  }
  int rc = delete_file(tiledb_ctx, filename);
  finalize(tiledb_ctx);
  return rc;
}

int move_across_filesystems(const std::string& src, const std::string& dest)
{
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(src)) || check_file_for_read(tiledb_ctx, src)) {
    FINALIZE;
    return TILEDB_ERR;
  }
  auto size = file_size(tiledb_ctx, src);
  void *buffer = malloc(size);
  if (buffer == NULL) {
    FINALIZE;
    snprintf(tiledb_errmsg, TILEDB_ERRMSG_MAX_LEN, "Out-of-memory exception while allocating memory\n");
    return TILEDB_ERR;
  }
  int rc = read_file(tiledb_ctx, src, 0, buffer, size);
  rc |= close_file(tiledb_ctx, src);
  finalize(tiledb_ctx);
  if (rc) {
    return TILEDB_ERR;
  }

  if (setup(&tiledb_ctx, parent_dir(dest)) || check_file(tiledb_ctx, dest)) {
    FINALIZE;
    return TILEDB_ERR;
  }
  rc = write_file(tiledb_ctx, dest, buffer, size);
  rc |= close_file(tiledb_ctx, dest);
  finalize(tiledb_ctx);
  return rc;
}

int create_temp_filename(char *path, size_t path_length) {
  memset(path, 0, path_length);
  const char *tmp_dir = getenv("TMPDIR");
  if (tmp_dir == NULL) {
    tmp_dir = P_tmpdir; // defined in stdio
  }
  if (tmp_dir == NULL) {
    snprintf(tiledb_errmsg, TILEDB_ERRMSG_MAX_LEN, "Could not get tmp_dir");
    return TILEDB_ERR;
  }
  if (tmp_dir[strlen(tmp_dir)-1]=='/') {
    snprintf(path, path_length, "%sTileDBXXXXXX", tmp_dir);
  } else {
    snprintf(path, path_length, "%s/TileDBXXXXXX", tmp_dir);
  }
  int tmp_fd = mkstemp(path);
  int rc = TILEDB_OK;
#ifdef __APPLE__
  if (fcntl(tmp_fd, F_GETPATH, path) == -1) {
#else
  char tmp_proc_lnk[64];
  sprintf(tmp_proc_lnk, "/proc/self/fd/%d", tmp_fd);
  memset(path, 0, path_length);
  if (readlink(tmp_proc_lnk, path, path_length-1) < 0) {
#endif
    snprintf(tiledb_errmsg, TILEDB_ERRMSG_MAX_LEN, "Could not successfully readlink errno=%d %s", errno, strerror(errno));
    rc = TILEDB_ERR;
  }
  close(tmp_fd);
  return rc;
}

int create_codec(void **handle, int compression_type, int compression_level) {
  return Codec::create(handle, compression_type, compression_level);
}
  
int compress(void *handle, unsigned char* segment, size_t segment_size, void** compressed_segment, size_t& compressed_segment_size) {
  return (reinterpret_cast<Codec *>(handle))->do_compress_tile(segment, segment_size, compressed_segment, compressed_segment_size);
}
  
int decompress(void *handle, unsigned char* compressed_segment,  size_t compressed_segment_size, unsigned char* segment, size_t segment_size) {
  return (reinterpret_cast<Codec *>(handle))->do_decompress_tile(compressed_segment, compressed_segment_size, segment, segment_size);
}
  
void finalize_codec(void *handle) {
  delete reinterpret_cast<Codec *>(handle);
}

}
