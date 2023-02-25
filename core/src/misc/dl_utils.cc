/**
 * @file   mem_utils.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2022 Omics Data Automation, Inc.
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
 * This file implements utilities to help profile memory
 */

#include <dl_utils.h>
#include <vector>

#ifdef __APPLE__
  std::vector<std::string> dl_paths_ = {"/usr/local/Cellar/lib/", 
      "/usr/local/lib/", "/usr/lib/", ""};
#elif __linux__
  std::vector<std::string> dl_paths_ = {"/usr/lib64/", "/usr/lib/", 
      "/usr/lib/x86_64-linux-gnu", ""};
#else
#  error Platform not supported
#endif
std::string dl_error_;
unsigned long ossl_ver = 0;


void *get_dlopen_handle(const std::string& name, const std::string& version) {
  void *handle = NULL;
  std::string prefix("lib");
#ifdef __APPLE__
  std::string suffix(".dylib");
#elif __linux__
  std::string suffix(".so");
#else
#  error Platform not supported
#endif
    
  clear_dlerror();
  for (std::string dl_path : dl_paths_) {
    std::string path = dl_path + prefix + name;
    if (version.empty()) {
      handle = dlopen( (path + suffix).c_str(), RTLD_GLOBAL|RTLD_NOW);
    } else {
#ifdef __APPLE__
      handle = dlopen((path+"."+version+suffix).c_str(), RTLD_GLOBAL|RTLD_NOW);
#else
      handle = dlopen((path+suffix+"."+version).c_str(), RTLD_GLOBAL|RTLD_NOW);
#endif
    }   
    if (handle) {
      clear_dlerror();
      return handle;
    } else {
      set_dlerror();
    }   
  }   

  return handle;
}

void *get_dlopen_handle(const std::string& name) {
  return get_dlopen_handle(name, "");
}

