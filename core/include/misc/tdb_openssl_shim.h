/**
 * @file   tdb_openssl_shim.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 Omics Data Automation, Inc.
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
 * This file provides a shim to both OpenSSL 1.x and 3.x versions. Clients can
 * choose to use the 3.x api if available using "OpenSSL_version_num() < 0x30000000L" for 1.x 
 * and "OpenSSL_version_num() >= 0x30000000L" for 3.x.
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

void ossl_shim_init();

// General
unsigned long OpenSSL_version_num(void);

// OpenSSL 1.x prototypes - see hmac.h
typedef struct hmac_ctx_st HMAC_CTX;
typedef struct evp_md_st   EVP_MD;
typedef struct engine_st   ENGINE;
typedef struct evp_md_ctx_st EVP_MD_CTX;
typedef struct SHA256state_st SHA256_CTX;
HMAC_CTX* __attribute__((weak)) HMAC_CTX_new(void);
int __attribute__((weak)) HMAC_CTX_reset(HMAC_CTX*);
int __attribute__((weak)) HMAC_Init_ex(HMAC_CTX *ctx, const void *key, 
    int key_len, const EVP_MD *md, ENGINE *impl);
int __attribute__((weak)) HMAC_Update(HMAC_CTX *ctx, const unsigned char *data, size_t len);
int __attribute__((weak)) HMAC_Final(HMAC_CTX *ctx, unsigned char *md, unsigned int *len);
void __attribute__((weak)) HMAC_CTX_free(HMAC_CTX *ctx);

// See md5.h

/*
#define MD5_LONG_OSSL1_SHIM unsigned int
#define MD5_CBLOCK_OSSL1_SHIM 64
#define MD5_LBLOCK_OSSL1_SHIM (MD5_CBLOCK_OSSL1_SHIM / 4)
typedef struct MD5state_st {
  MD5_LONG_OSSL1_SHIM A, B, C, D;
  MD5_LONG_OSSL1_SHIM Nl, Nh;
  MD5_LONG_OSSL1_SHIM data[MD5_LBLOCK_OSSL1_SHIM];
  unsigned int num;
} MD5_CTX;
*/

#define MD5_LONG unsigned int
#define MD5_CBLOCK 64
#define MD5_LBLOCK (MD5_CBLOCK / 4)
typedef struct MD5state_st {
  MD5_LONG A, B, C, D;
  MD5_LONG Nl, Nh;
  MD5_LONG data[MD5_LBLOCK];
  unsigned int num;
} MD5_CTX;

#define MD5_DIGEST_LENGTH 16
#define SHA256_DIGEST_LENGTH 32
int __attribute__((weak)) MD5_Init(MD5_CTX* c);
int __attribute__((weak)) MD5_Update(MD5_CTX* c, const void* data, size_t);
int __attribute__((weak)) MD5_Final(unsigned char*, MD5_CTX* c);
unsigned char __attribute__((weak)) *MD5(const unsigned char *d, size_t n, unsigned char *md);

// See sha.h
/*
#define SHA_LONG_OSSL1_SHIM unsigned int
#define SHA_LBLOCK_OSSL1_SHIM 16

typedef struct SHA256state_st {
  SHA_LONG_OSSL1_SHIM h[8];
  SHA_LONG_OSSL1_SHIM Nl, Nh;
  SHA_LONG_OSSL1_SHIM data[SHA_LBLOCK_OSSL1_SHIM];
  unsigned int num, md_len;
} SHA256_CTX;
*/

/*
#define SHA_LONG unsigned int
#define SHA_LBLOCK 16

__attribute__((weak))
typedef struct SHA256state_st {
  SHA_LONG h[8];
  SHA_LONG Nl, Nh;
  SHA_LONG data[SHA_LBLOCK];
  unsigned int num, md_len;
} SHA256_CTX;
*/


int __attribute__((weak)) SHA256_Init(SHA256_CTX *c);
int __attribute__((weak)) SHA256_Update(SHA256_CTX *c, const void*, size_t);
int __attribute__((weak)) SHA256_Final(unsigned char *md, SHA256_CTX *c);


// #define OPENSSL_malloc(num) CRYPTO_malloc(num, __FILE__, __LINE__)
void __attribute__((weak)) *CRYPTO_malloc(size_t num, const char* file, int line);


// OpenSSL 3 prototypes - see evp.h
typedef struct evp_mac_st EVP_MAC;
typedef struct evp_mac_ctx_st EVP_MAC_CTX;
typedef struct ossl_lib_ctx_st OSSL_LIB_CTX;

EVP_MAC* __attribute__((weak))
EVP_MAC_fetch(OSSL_LIB_CTX*, const char*, const char*);
EVP_MAC_CTX* __attribute__((weak)) EVP_MAC_CTX_new(EVP_MAC*);
void __attribute__((weak)) EVP_MAC_CTX_free(EVP_MAC_CTX*);

#if 0
// See params.h and types.h
struct ossl_param_st {
  const char* key;        /* the name of the parameter */
  unsigned int data_type; /* declare what kind of content is in buffer */
  void* data;             /* value being passed in or out */
  size_t data_size;       /* data size */
  size_t return_size;     /* returned content size */
};
#endif

typedef struct ossl_param_st OSSL_PARAM;
OSSL_PARAM __attribute__((weak))
OSSL_PARAM_construct_utf8_string(const char*, char*, size_t);
OSSL_PARAM __attribute__((weak)) OSSL_PARAM_construct_end(void);

int __attribute__((weak))
EVP_MAC_init(EVP_MAC_CTX*, const unsigned char*, size_t, const OSSL_PARAM[]);

int __attribute__((weak)) EVP_MAC_update(EVP_MAC_CTX*, const unsigned char*, size_t);
int __attribute__((weak)) EVP_MAC_final(EVP_MAC_CTX*, unsigned char*, size_t*, size_t);
int __attribute__((weak)) EVP_MD_get_size(const EVP_MD*);

int __attribute__((weak)) EVP_DigestInit_ex(EVP_MD_CTX*, const EVP_MD*, ENGINE*);
int __attribute__((weak)) EVP_DigestUpdate(EVP_MD_CTX*, const void*, size_t);
int __attribute__((weak)) EVP_DigestFinal_ex(EVP_MD_CTX*, unsigned char*, unsigned int*);

void __attribute__((weak)) EVP_MD_CTX_free(EVP_MD_CTX*);
const EVP_MD* __attribute__((weak)) EVP_md5(void);
EVP_MD_CTX* __attribute__((weak)) EVP_MD_CTX_new(void);
  
const EVP_MD* __attribute__((weak)) EVP_sha256(void);

#ifdef __cplusplus
}
#endif
