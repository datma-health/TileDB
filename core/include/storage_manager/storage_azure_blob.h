/**
 * @file storage_azure_blob.h
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
 * Azure Blob Storage Interface
 *
 */

#ifndef __STORAGE_AZURE_BLOB_H__
#define  __STORAGE_AZURE_BLOB_H__

#include "storage_fs.h"

#include "blob/blob_client.h"
#include "storage_account.h"
#include "storage_credential.h"
#include "tiledb_constants.h"

#include <streambuf>

using namespace azure::storage_lite;

class AzureBlob : public StorageFS {
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

  size_t file_size(const std::string& filename);

  int read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length);
  int write_to_file(const std::string& filename, const void *buffer, size_t buffer_size);
  
  int move_path(const std::string& old_path, const std::string& new_path);
    
  int sync_path(const std::string& path);

  bool locking_support();

 protected:
  std::shared_ptr<blob_client> bC = nullptr;
  std::shared_ptr<blob_client_wrapper> bc_wrapper = nullptr;
  blob_client_wrapper *bc = nullptr;

  std::string account_name;
  std::string container_name;
  std::string working_dir;

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
  int write_to_file_kernel(const std::string& filename, const void *buffer, size_t buffer_size);
  
};
#endif /* __STORAGE_AZURE_BLOB_H__ */
