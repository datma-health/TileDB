/**
 * @file storage_azure_blob.h
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
 * Azure Blob Storage Interface
 *
 */

#ifndef __STORAGE_AZURE_BLOB_H__
#define  __STORAGE_AZURE_BLOB_H__

#include "storage_fs.h"

#include "base64.h"
#include "blob/blob_client.h"
#include "storage_account.h"
#include "storage_credential.h"
#include "tiledb_constants.h"

#include <assert.h>
#include <unordered_map>
#include <streambuf>

using namespace azure::storage_lite;

class AzureBlob : public StorageCloudFS {
 public:
  AzureBlob(const std::string& home);

  std::string current_dir();
  int set_working_dir(const std::string& dir);
  
  bool is_dir(const std::string& dir);
  bool is_file(const std::string& file);
  std::string real_dir(const std::string& dir);
               
  int create_dir(const std::string& dir);
  int delete_dir(const std::string& dir);

  std::vector<std::string> get_dirs(const std::string& dir);
  std::vector<std::string> get_files(const std::string& dir);
  
  int create_file(const std::string& filename, int flags, mode_t mode);
  int delete_file(const std::string& filename);

  ssize_t file_size(const std::string& filename);

  int read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length);
  int write_to_file(const std::string& filename, const void *buffer, size_t buffer_size);
  
  int sync_path(const std::string& path);

  bool locking_support();

 protected:
  std::shared_ptr<blob_client> blob_client_ = nullptr;
  std::shared_ptr<blob_client_wrapper> bc_wrapper_ = nullptr;
  blob_client_wrapper *blob_client_wrapper_ = nullptr;

  std::string account_name_;
  std::string container_name_;
  std::string working_dir_;

  std::mutex write_map_mtx_;
  std::unordered_map<std::string, std::vector<put_block_list_request_base::block_item>> write_map_;

  std::vector<std::pair<std::string, std::string>> empty_metadata;

 private:
  struct membuf: std::streambuf {
    membuf(const void *buffer, size_t buffer_size, std::ios_base::openmode mode = std::ios_base::in) {
      char *p_buffer(reinterpret_cast<char *>(const_cast<void *>(buffer)));
      if (mode == std::ios_base::in) {
        this->setg(p_buffer, p_buffer, p_buffer+buffer_size);
      } else if (mode == std::ios_base::out) {
        this->setp(p_buffer, p_buffer+buffer_size);
      } else {
        throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "No membuf support for openmodes other than in and out");
      }
    }

    pos_type seekoff(off_type off, std::ios_base::seekdir dir, std::ios_base::openmode mode) override {
      if (mode == std::ios_base::in) {
        if (dir == std::ios_base::cur) {
          this->gbump(off);
        } else if (dir == std::ios_base::end) {
          this->setg(this->eback(), this->egptr() + off, this->egptr()); 
        } else if (dir == std::ios_base::beg) {
          this->setg(this->eback(), this->eback() + off, this->egptr());
        }
        return this->gptr() - this->eback();
      }
      // Support only input modes for now
      return std::streampos(std::streamoff(-1));
    }

    pos_type seekpos(pos_type pos, std::ios_base::openmode mode) override {
      return seekoff(pos - pos_type(off_type(0)), std::ios_base::beg, mode);
    }
  };

  struct imemstream: virtual membuf, std::istream {
    imemstream(const void *buffer, size_t buffer_size)
        : membuf(buffer, buffer_size, std::ios_base::in), std::istream(static_cast<std::streambuf *>(this)) {
    }
  };

  struct omemstream: virtual membuf, std::ostream {
    omemstream(const void *buffer, size_t buffer_size)
        : membuf(buffer, buffer_size, std::ios_base::out), std::ostream(static_cast<std::streambuf *>(this)) {
    }
  };

  std::string get_path(const std::string& path);

  int commit_file(const std::string& filename);

  std::vector<std::string> generate_block_ids(const std::string& path, int num_blocks) {
    std::vector<std::string> block_ids;
    block_ids.reserve(num_blocks);

    int existing_num_blocks = 0;
    const std::lock_guard<std::mutex> lock(write_map_mtx_);
    auto search = write_map_.find(path);
    if (search == write_map_.end()) {
      write_map_.insert({path, std::vector<put_block_list_request_base::block_item>{}});
      search = write_map_.find(path);
      assert (search != write_map_.end());
    } else {
      existing_num_blocks = search->second.size();
    }

    for (int i = existing_num_blocks; i < existing_num_blocks+num_blocks; i++) {
      std::string block_id = std::to_string(i);
      block_id = std::string(12 - block_id.length(), '-') + block_id;
      block_id = to_base64(reinterpret_cast<const unsigned char*>(block_id.data()), block_id.length());
      auto block = put_block_list_request_base::block_item{block_id, put_block_list_request_base::block_type::uncommitted};
      block_ids.emplace_back(block_id);
      search->second.push_back(std::move(block));
    }

    return block_ids;
  }

  std::future<storage_outcome<void>> upload_block_blob(const std::string &blob, uint64_t block_size, int num_blocks,
                                                       std::vector<std::string> block_list,
                                                       const char* buffer, uint64_t bufferlen,
                                                       uint parallelism=1);
  
};
#endif /* __STORAGE_AZURE_BLOB_H__ */
