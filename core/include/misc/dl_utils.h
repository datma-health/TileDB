/**
 * @file   dl_utils.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 * @copyright Copyright (c) 2021 Omics Data Automation Inc.
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
 * This file specifies the memory utility functions
 */

#pragma once

#include <string>
#include <system_error>
#include <dlfcn.h>

extern void* dl_handle;
extern std::string dl_error_;
extern unsigned long ossl_ver;

inline void clear_dlerror() {
  dl_error_ = std::string("");
  dlerror();
}

inline void set_dlerror() {
  char *errmsg = dlerror();
  if(errmsg) {
    dl_error_.empty() ? (dl_error_ = errmsg) : 
        (dl_error_ += std::string("\n") + errmsg);
  }
}

void ossl_shim_init(void);
void *get_dlopen_handle(const std::string& name, const std::string& version);
void *get_dlopen_handle(const std::string& name); 

#define BIND_SYMBOL(H, X, Y, Z)  \
  do {                           \
    clear_dlerror();             \
    X = Z dlsym(H, Y);           \
    if (!X) {                    \
      set_dlerror();             \
      throw std::system_error(ECANCELED, std::generic_category(), dl_error_); \
    }                            \
  } while (false)

