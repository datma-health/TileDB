/**
 * @file   tdb_openssl_shim.cc
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

#include <array>
#include <cstring>
#include <stdexcept>
#include <iostream>

#include "dl_utils.h"
#define OPENSSL_SHIM_EXTERN
#include "tdb_openssl_shim.h"

void* dl_handle = nullptr;

void ossl_shim_init(void) {
  // TODO: This should be called only once using std::call_once and using a std::once_flag as this could
  // potentially be called simultaneously in a multi threaded environment. Not sufficient to check for
  // dl_handle!! See codec_zstd.h for a pattern

  if (OpenSSL_version_num()) {
    std::cerr << "Found OpenSSL version ver=" << OpenSSL_version_num() << std::endl;
  }
 
  /*
  if(dl_handle == nullptr) {
    dl_handle = get_dlopen_handle("crypto");
    if(!dl_handle) {
      throw std::runtime_error("libcrypto is not present in the system");
    }
    // BIND_SYMBOL(dl_handle, OpenSSL_version_num, "OpenSSL_version_num", (unsigned long(*)(void)) );
    auto ossl_ver = OpenSSL_version_num();
    if(ossl_ver < 0x30000000L) {
      // see hmac.h
      BIND_SYMBOL(dl_handle, HMAC_CTX_new, "HMAC_CTX_new", 
                  (HMAC_CTX *(*)(void)) );
      BIND_SYMBOL(dl_handle, HMAC_CTX_reset, "HMAC_CTX_reset", 
          (int(*)(void*)) );
      BIND_SYMBOL(dl_handle, HMAC_Init_ex, "HMAC_Init_ex", 
          (int(*)(void *, const void *, int , const void *, void *)) );
      BIND_SYMBOL(dl_handle, HMAC_Update, "HMAC_Update", 
          (int(*)(void *, const unsigned char *, size_t)) );
      BIND_SYMBOL(dl_handle, HMAC_Final, "HMAC_Final", 
          (int(*)(void *, unsigned char *, unsigned int *)) );
      BIND_SYMBOL(dl_handle, HMAC_CTX_free, "HMAC_CTX_free", 
          (void(*)(void *)) );
      
      BIND_SYMBOL(dl_handle, MD5_Init, "MD5_Init", (int(*)(void *)) );
      BIND_SYMBOL(dl_handle, MD5_Update, "MD5_Update", 
          (int(*)(void *, const void *, size_t)) );
      BIND_SYMBOL(dl_handle, MD5_Final, "MD5_Final", 
          (int(*)(unsigned char *, void *)) );
      
      BIND_SYMBOL(dl_handle, SHA256_Init, "SHA256_Init", 
          (int (*)(void*)));
      BIND_SYMBOL(dl_handle, SHA256_Final, "SHA256_Final", 
          (int (*)(unsigned char *, void*)));
      BIND_SYMBOL(dl_handle, SHA256_Update, "SHA256_Update",  
          (int (*)(void*, const void *, size_t)));
    } else {
      // see evp.h
      BIND_SYMBOL(dl_handle, EVP_MAC_fetch, "EVP_MAC_fetch", 
          (void*(*)(void *, const char *, const char *)) );
      BIND_SYMBOL(dl_handle, EVP_MAC_CTX_new, "EVP_MAC_CTX_new", 
          (void*(*)(void *)));
      BIND_SYMBOL(dl_handle, EVP_MAC_CTX_free, "EVP_MAC_CTX_free",
                  (void(*)(void *)));

      // see params.h and types.h
      BIND_SYMBOL(dl_handle, OSSL_PARAM_construct_utf8_string, 
          "OSSL_PARAM_construct_utf8_string", 
          (OSSL_PARAM(*)(const char *, char *, size_t)) );
      BIND_SYMBOL(dl_handle, OSSL_PARAM_construct_end, 
          "OSSL_PARAM_construct_end", (OSSL_PARAM(*)(void)) );
      
      BIND_SYMBOL(dl_handle, EVP_MAC_init, "EVP_MAC_init", 
          (int(*)(void *, const unsigned char *, size_t, 
          const OSSL_PARAM [])) );
      
      BIND_SYMBOL(dl_handle, EVP_MAC_update, "EVP_MAC_update", 
          (int(*)(void *, const unsigned char *, size_t)) );
      BIND_SYMBOL(dl_handle, EVP_MAC_final, "EVP_MAC_final", 
          (int(*)(void *, unsigned char *, size_t *, size_t)) );
      BIND_SYMBOL(dl_handle, EVP_MD_get_size, "EVP_MD_get_size", 
          (int (*)(const void *)));
      
      BIND_SYMBOL(dl_handle, EVP_DigestInit_ex, "EVP_DigestInit_ex", 
          (int (*)(void *, const void *, void *)) );
      BIND_SYMBOL(dl_handle, EVP_DigestUpdate, "EVP_DigestUpdate", 
          (int (*)(void *, const void *, size_t)) );
            BIND_SYMBOL(dl_handle, EVP_DigestFinal_ex, "EVP_DigestFinal_ex", 
          (int (*)(void *, unsigned char *, unsigned int *)) );
            
      BIND_SYMBOL(dl_handle, EVP_MD_CTX_free, "EVP_MD_CTX_free", 
          (void (*)(void *)) );
      BIND_SYMBOL(dl_handle, EVP_md5, "EVP_md5", (const void*(*)(void)));
      BIND_SYMBOL(dl_handle, EVP_MD_CTX_new, "EVP_MD_CTX_new", 
          (void* (*)(void)) );
      
      BIND_SYMBOL(dl_handle, EVP_sha256, "EVP_sha256", 
          (const void* (*)(void)));
    }
    }*/
}

