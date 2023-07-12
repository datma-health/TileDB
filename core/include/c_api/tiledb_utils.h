/**
 * @file tiledb_utils.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 Omics Data Automation Inc. and Intel Corporation
 * @copyright Copyright (c) 2020-2021 Omics Data Automation Inc.
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
 * Utility prototypes to access TileDB functionality in an opaque, convenient fashion.
 *
 */

#ifndef __TILEDB_UTILS_H__
#define  __TILEDB_UTILS_H__

#include "tiledb.h"
#include <string>
#include <vector>

namespace TileDBUtils {

bool is_cloud_path(const std::string& path);

std::string get_path(const std::string &workspace);

std::string append_path(std::string temp_dir, std::string path);

int initialize_workspace(TileDB_CTX **ptiledb_ctx, const std::string& workspace, const bool overwrite=false,
                         const bool enable_shared_posixfs_optimizations=false);

int create_workspace(const std::string &workspace, bool replace=false);

bool workspace_exists(const std::string& workspace);

bool array_exists(const std::string& workspace, const std::string& array_name);

std::vector<std::string> get_array_names(const std::string& workspace);

std::vector<std::string> get_fragment_names(const std::string& workspace);

bool is_dir(const std::string& dirpath);

std::string real_dir(const std::string& dirpath);

int create_dir(const std::string& dirpath);

int delete_dir(const std::string& dirpath);

bool is_file(const std::string& filepath);

std::vector<std::string> get_dirs(const std::string& dirpath);

std::vector<std::string> get_files(const std::string& dirpath);

ssize_t file_size(const std::string& filepath);

void print_md5_hash(unsigned char* buffer, size_t length);

/**
 * buffer is malloc'ed and has to be freed by calling function
 */
int read_entire_file(const std::string& filename, void **buffer, size_t *length);

int read_file(const std::string& filename, off_t offset, void *buffer, size_t length);

int write_file(const std::string& filename, const void *buffer, size_t length, const bool overwrite=false);

int delete_file(const std::string& filename);

int move_across_filesystems(const std::string& src, const std::string& dest);

int create_temp_filename(char *path, size_t path_length);

/**
 * access to compression algorithms
 */
int create_codec(void **handle, int compression_type, int compression_level);
int compress(void *handle, unsigned char* segment, size_t segment_size, void** compressed_segment, size_t& compressed_segment_size);
int decompress(void *handle, unsigned char* compressed_segment,  size_t compressed_segment_size, unsigned char* segment, size_t segment_size);
void finalize_codec(void *handle);

}

#endif

