/**
 * @file   local_fs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2019 Omics Data Automation, Inc.
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
 * Default Posix Filesystem Implementation for StorageFS
 */

#include "error.h"
#include "storage_posixfs.h"
#include "utils.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <cstdio>
#include <dirent.h>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>

#define POSIX_ERROR(MSG, PATH) SYSTEM_ERROR(TILEDB_FS_ERRMSG, MSG, PATH, tiledb_fs_errmsg)

std::string PosixFS::current_dir() {
  std::string dir = "";
  char* path = getcwd(NULL, 0);

  if(path != NULL) {
    dir = path;
    free(path);
  }

  return dir;
}
  
bool PosixFS::is_dir(const std::string& dir) {
  struct stat st;
  memset(&st, 0, sizeof(struct stat));
  return !stat(dir.c_str(), &st) && S_ISDIR(st.st_mode);
}

bool PosixFS::is_file(const std::string& file) {
  struct stat st;
  memset(&st, 0, sizeof(struct stat));
  return !stat(file.c_str(), &st) && S_ISREG(st.st_mode);
}

void adjacent_slashes_dedup(std::string& value) {
  value.erase(std::unique(value.begin(), value.end(), both_slashes),
              value.end()); 
}

void purge_dots_from_path(std::string& path) {
  // For easy reference
  size_t path_size = path.size(); 

  // Trivial case
  if(path_size == 0 || path == "/")
    return;

  // It expects an absolute path
  assert(path[0] == '/');

  // Tokenize
  const char* token_c_str = path.c_str() + 1;
  std::vector<std::string> tokens, final_tokens;
  std::string token;

  for(size_t i=1; i<path_size; ++i) {
    if(path[i] == '/') {
      path[i] = '\0';
      token = token_c_str;
      if(token != "")
        tokens.push_back(token); 
      token_c_str = path.c_str() + i + 1;
    }
  }
  token = token_c_str;
  if(token != "")
    tokens.push_back(token); 

  // Purge dots
  int token_num = tokens.size();
  for(int i=0; i<token_num; ++i) {
    if(tokens[i] == ".") { // Skip single dots
      continue;
    } else if(tokens[i] == "..") {
      if(final_tokens.size() == 0) {
        // Invalid path
        path = "";
        return;
      } else {
        final_tokens.pop_back();
      }
    } else {
      final_tokens.push_back(tokens[i]);
    }
  } 

  // Assemble final path
  path = "/";
  int final_token_num = final_tokens.size();
  for(int i=0; i<final_token_num; ++i) 
    path += ((i != 0) ? "/" : "") + final_tokens[i]; 
}

std::string PosixFS::real_dir(const std::string& dir) {
    // Initialize current, home and root
  std::string current = current_dir();
  auto env_home_ptr = getenv("HOME");
  std::string home = env_home_ptr ? env_home_ptr : current;
  std::string root = "/";

  // Easy cases
  if(dir == "" || dir == "." || dir == "./")
    return current;
  else if(dir == "~")
    return home;
  else if(dir == "/")
    return root; 

  // Other cases
  std::string ret_dir;
  if(starts_with(dir, "/"))
    ret_dir = root + dir;
  else if(starts_with(dir, "~/"))
    ret_dir = home + dir.substr(1, dir.size()-1);
  else if(starts_with(dir, "./"))
    ret_dir = current + dir.substr(1, dir.size()-1);
  else 
    ret_dir = current + "/" + dir;

  adjacent_slashes_dedup(ret_dir);
  purge_dots_from_path(ret_dir);

  return ret_dir;
}
  
int PosixFS::create_dir(const std::string& dir) {
  reset_errno();
  
  // Get real directory path
  std::string real_dir = this->real_dir(dir);

  // If the directory does not exist, create it
  if(!is_dir(real_dir)) { 
    if(mkdir(real_dir.c_str(), S_IRWXU)) {
      POSIX_ERROR("Cannot create directory", real_dir);
      return TILEDB_FS_ERR;
    }
  } else { // Error
    POSIX_ERROR("Cannot create directory; Directory already exists", real_dir);
    return TILEDB_FS_ERR;
  }

  return TILEDB_FS_OK;
}

int PosixFS::delete_dir(const std::string& dirname) {
  reset_errno();
  
  // Get real path
  std::string dirname_real = this->real_dir(dirname); 

  // Delete the contents of the directory
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(dirname_real.c_str());

  if(dir == NULL) {
    POSIX_ERROR("Cannot open directory", dirname);
    return TILEDB_FS_ERR;
  }

  std::vector<std::string> all_filenames;
  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, ".."))
      continue;
    filename = dirname_real + "/" + next_file->d_name;
    all_filenames.emplace_back(filename);
  }

  for(const auto& curr_filename : all_filenames) {
    if(remove(curr_filename.c_str())) {
      POSIX_ERROR("Cannot delete file", curr_filename);   
      return TILEDB_FS_ERR;
    }
  } 
 
  // Close directory 
  if(closedir(dir)) {
    POSIX_ERROR("Cannot close directory", dirname);
    return TILEDB_FS_ERR;
  }

  // Remove directory
  if(rmdir(dirname.c_str())) {
    POSIX_ERROR("Cannot delete directory", dirname);
  }

  // Success
  return TILEDB_FS_OK;
}

std::vector<std::string> PosixFS::get_dirs(const std::string& dir) {
  reset_errno();
  
  std::vector<std::string> dirs;
  std::string new_dir; 
  struct dirent *next_file;
  DIR* c_dir = opendir(dir.c_str());

  if(c_dir == NULL) {
    POSIX_ERROR("Cannot open directory", dir);
    return std::vector<std::string>();
  }

  while((next_file = readdir(c_dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, "..") ||
       !is_dir(dir + "/" + next_file->d_name))
      continue;
    new_dir = dir + "/" + next_file->d_name;
    dirs.push_back(new_dir);
  } 

  // Close array directory  
  if (closedir(c_dir)) {
    POSIX_ERROR("Cannot close directory", dir);
  }

  // Return
  return dirs;
}
    
std::vector<std::string> PosixFS::get_files(const std::string& dir) {
  reset_errno();
  
  std::vector<std::string> files;
  std::string filename; 
  struct dirent *next_file;
  DIR* c_dir = opendir(dir.c_str());

  if(c_dir == NULL) {
    POSIX_ERROR("Cannot open directory", dir);
    return std::vector<std::string>();
  }

  while((next_file = readdir(c_dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, "..") ||
       !is_file(dir + "/" + next_file->d_name))
      continue;
    filename = dir + "/" + next_file->d_name;
    files.push_back(filename);
  } 

  // Close array directory  
  if (closedir(c_dir)) {
    POSIX_ERROR("Cannot close directory", dir);
  }

  // Return
  return files;
}

int PosixFS::create_file(const std::string& filename, int flags, mode_t mode) {
  reset_errno();
  
  int fd = open(filename.c_str(), flags, mode);
  if(fd == -1 || close(fd)) {
    POSIX_ERROR("Failed to create file", filename);
    return TILEDB_FS_ERR;
  }

  return TILEDB_FS_OK;
}

int PosixFS::delete_file(const std::string& filename) {
  reset_errno();

  if(remove(filename.c_str())) {
    POSIX_ERROR("Cannot remove file", filename);
    return TILEDB_FS_ERR;
  }

  return TILEDB_FS_OK;
}

size_t PosixFS::file_size(const std::string& filename) {
  reset_errno();
  
  if (!is_file(filename)) {
    POSIX_ERROR("Cannot get file size for paths that are not files", filename);
    return TILEDB_FS_ERR;
  }
  
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    POSIX_ERROR("Cannot get file size; File opening error", filename);
    return TILEDB_FS_ERR;
  }

  struct stat st;
  memset(&st, 0, sizeof(struct stat));
  fstat(fd, &st);
  off_t file_size = st.st_size;
  
  if (close(fd)) {
    POSIX_ERROR("Cannot get file size; File closing error", filename);
  }

  return file_size;
}

int PosixFS::read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length) {
  reset_errno();

  if (length == 0) {
    return TILEDB_FS_OK;
  }
  
  // Open file
  int fd = open(filename.c_str(), O_RDONLY);
  if (fd == -1) {
    POSIX_ERROR("Cannot read from file; File opening error", filename);
    return TILEDB_FS_ERR;
  }

  // Read in batches of SSIZE_MAX
  size_t nbytes = 0;
  char *pbuf = reinterpret_cast<char *>(buffer);
  do {
    ssize_t bytes_read = pread(fd, reinterpret_cast<void *>(pbuf), (length - nbytes) > SSIZE_MAX?SSIZE_MAX : length-nbytes, offset + nbytes);
    if (bytes_read < 0) {
      POSIX_ERROR("Cannot read from file; File reading error", filename);
      return TILEDB_FS_ERR;
    } else if (bytes_read == 0) {
      POSIX_ERROR("EOF reached; File reading error", filename);
      return TILEDB_FS_ERR;
    }
    nbytes += bytes_read;
    pbuf += bytes_read;
  } while (nbytes < length);
  
  // Close file
  if (close(fd)) {
    POSIX_ERROR("Cannot read from file; File closing error", filename);
    return TILEDB_FS_ERR;
  }

  // Success
  return TILEDB_FS_OK;
}

int PosixFS::write_to_file(const std::string& filename, const void *buffer, size_t buffer_size) {
  reset_errno();
  
  if (buffer_size == 0) {
    return TILEDB_FS_OK;
  }

  // Open file
  int fd = open(filename.c_str(), O_WRONLY | O_APPEND | O_CREAT, S_IRWXU);
  if(fd == -1) {
    POSIX_ERROR("Cannot write to file; File opening error", filename);
    return TILEDB_FS_ERR;
  }

  // Write in batches of SSIZE_MAX
  size_t nbytes = 0;
  char *pbuf = reinterpret_cast<char *>(const_cast<void *>(buffer));
  do {
    ssize_t bytes_written = write(fd, reinterpret_cast<void *>(pbuf), (buffer_size - nbytes) > SSIZE_MAX ? SSIZE_MAX : buffer_size - nbytes);
    if(bytes_written < 0) {
      POSIX_ERROR("Cannot write to file; File writing error", filename);
      return TILEDB_FS_ERR;
    }
    nbytes += bytes_written;
    pbuf += bytes_written;
  } while (nbytes < buffer_size);

  // Close file
  if(close(fd)) {
    POSIX_ERROR("Cannot write to file; File closing error", filename);
    return TILEDB_FS_ERR;
  }

  // Success 
  return TILEDB_FS_OK;
}

int PosixFS::move_path(const std::string& old_path, const std::string& new_path) {
  reset_errno();
  
  if(rename(old_path.c_str(), new_path.c_str())) {
    POSIX_ERROR("Cannot rename path", old_path);
    return TILEDB_FS_ERR;
  }
  
  return TILEDB_FS_OK;
}
    
int PosixFS::sync_path(const std::string& filename) {
  reset_errno();
  
  // Open file
  int fd;
  if(is_dir(filename)) {      // DIRECTORY 
    fd = open(filename.c_str(), O_RDONLY, S_IRWXU);
  } else if(is_file(filename)) { // FILE
    fd = open(filename.c_str(), O_WRONLY | O_APPEND, S_IRWXU);
  } else {
    return TILEDB_FS_OK;     // If file does not exist, exit
  }

  // Handle error
  if(fd == -1) {
    POSIX_ERROR("Cannot sync file; File opening error", filename);
    return TILEDB_FS_ERR;
  }

  // Sync
  if(fsync(fd)) {
    if (locking_support()) {
    POSIX_ERROR("Cannot sync file; File syncing error", filename);
    return TILEDB_FS_ERR;
    }
  }

  // Close file
  if(close(fd)) {
    POSIX_ERROR("Cannot sync file; File closing error", filename);
    return TILEDB_FS_ERR;
  }

  // Success 
  return TILEDB_FS_OK;
}

bool PosixFS::locking_support() {
  return !disable_file_locking();
}
