/**
 * @file   tiledb_openssl_shim.h
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
#define SHA_LONG unsigned int
#define SHA_LBLOCK 16

typedef struct SHA256state_st {
  SHA_LONG h[8];
  SHA_LONG Nl, Nh;
  SHA_LONG data[SHA_LBLOCK];
  unsigned int num, md_len;
} SHA256_CTX;



int __attribute__((weak)) SHA256_Init(SHA256_CTX *c);
int __attribute__((weak)) SHA256_Update(SHA256_CTX *c, const void*, size_t);
int __attribute__((weak)) SHA256_Final(unsigned char *md, SHA256_CTX *c);



// OpenSSL 3 prototypes - see evp.h

#define OSSL_ALG_PARAM_DIGEST       "digest"    /* utf8_string */
#define OSSL_MAC_PARAM_DIGEST           OSSL_ALG_PARAM_DIGEST     /* utf8 string */
#define OPENSSL_INIT_LOAD_CRYPTO_STRINGS    0x00000002L
#define OPENSSL_INIT_ADD_ALL_CIPHERS        0x00000004L
#define OPENSSL_INIT_ADD_ALL_DIGESTS        0x00000008L

#define EVP_MD_CTX_FLAG_NON_FIPS_ALLOW  0x0008
#define EVP_CTRL_AEAD_GET_TAG           0x10
#define EVP_CTRL_AEAD_SET_TAG           0x11
#define EVP_CTRL_GCM_GET_TAG    EVP_CTRL_AEAD_GET_TAG
#define EVP_CTRL_GCM_SET_TAG    EVP_CTRL_AEAD_SET_TAG
#define EVP_MD_CTX_create()     EVP_MD_CTX_new()
#define EVP_MD_CTX_destroy(ctx) EVP_MD_CTX_free((ctx))
//#define EVP_MD_size             EVP_MD_get_size
#define EVP_CIPHER_CTX_init(c)      EVP_CIPHER_CTX_reset(c)
#define EVP_CIPHER_CTX_cleanup(c)   EVP_CIPHER_CTX_reset(c)
#define OPENSSL_add_all_algorithms_noconf() \
    OPENSSL_init_crypto(OPENSSL_INIT_ADD_ALL_CIPHERS \
    | OPENSSL_INIT_ADD_ALL_DIGESTS, NULL)

typedef struct evp_mac_st EVP_MAC;
typedef struct evp_mac_ctx_st EVP_MAC_CTX;
typedef struct ossl_lib_ctx_st OSSL_LIB_CTX;
typedef struct evp_cipher_st EVP_CIPHER;
typedef struct evp_cipher_ctx_st EVP_CIPHER_CTX;
typedef struct ossl_param_st OSSL_PARAM;
typedef struct ossl_init_settings_st OPENSSL_INIT_SETTINGS;

EVP_MAC* __attribute__((weak)) EVP_MAC_fetch(OSSL_LIB_CTX*, const char*, const char*);
EVP_MAC_CTX* __attribute__((weak)) EVP_MAC_CTX_new(EVP_MAC*);
void __attribute__((weak)) EVP_MAC_CTX_free(EVP_MAC_CTX*);
void __attribute__((weak)) EVP_MAC_free(EVP_MAC *mac);

// See params.h and types.h
struct ossl_param_st {
  const char* key;        /* the name of the parameter */
  unsigned int data_type; /* declare what kind of content is in buffer */
  void* data;             /* value being passed in or out */
  size_t data_size;       /* data size */
  size_t return_size;     /* returned content size */
};

OSSL_PARAM __attribute__((weak)) OSSL_PARAM_construct_utf8_string(const char*, char*, size_t);
OSSL_PARAM __attribute__((weak)) OSSL_PARAM_construct_end(void);

int __attribute__((weak)) EVP_MAC_init(EVP_MAC_CTX*, const unsigned char*, size_t, const OSSL_PARAM[]);

int __attribute__((weak)) EVP_MAC_update(EVP_MAC_CTX*, const unsigned char*, size_t);
int __attribute__((weak)) EVP_MAC_final(EVP_MAC_CTX*, unsigned char*, size_t*, size_t);
int __attribute__((weak)) EVP_MD_get_size(const EVP_MD*);
int __attribute__((weak)) EVP_MD_size(const EVP_MD*);

int __attribute__((weak)) EVP_DigestInit_ex(EVP_MD_CTX*, const EVP_MD*, ENGINE*);
int __attribute__((weak)) EVP_DigestUpdate(EVP_MD_CTX*, const void*, size_t);
int __attribute__((weak)) EVP_DigestFinal_ex(EVP_MD_CTX*, unsigned char*, unsigned int*);

void __attribute__((weak)) EVP_MD_CTX_free(EVP_MD_CTX*);
const EVP_MD* __attribute__((weak)) EVP_md5(void);
EVP_MD_CTX* __attribute__((weak)) EVP_MD_CTX_new(void);
  
const EVP_MD* __attribute__((weak)) EVP_sha256(void);
__attribute__((weak)) void EVP_MD_CTX_set_flags(EVP_MD_CTX *ctx, int flags);
__attribute__((weak)) int EVP_DigestFinal(EVP_MD_CTX *ctx, unsigned char *md, 
    unsigned int *s); 
__attribute__((weak)) const EVP_MD *EVP_sha1(void);
__attribute__((weak)) int EVP_CIPHER_CTX_copy(EVP_CIPHER_CTX *out, 
    const EVP_CIPHER_CTX *in);
__attribute__((weak)) int EVP_CIPHER_CTX_set_padding(EVP_CIPHER_CTX *c, int pad);
__attribute__((weak)) int EVP_CIPHER_CTX_ctrl(EVP_CIPHER_CTX *ctx, int type, 
    int arg, void *ptr);
__attribute__((weak)) EVP_CIPHER_CTX *EVP_CIPHER_CTX_new(void);
__attribute__((weak)) void EVP_CIPHER_CTX_free(EVP_CIPHER_CTX *c);
__attribute__((weak)) int EVP_CIPHER_CTX_reset(EVP_CIPHER_CTX *c);

__attribute__((weak)) int EVP_EncryptInit_ex(EVP_CIPHER_CTX *ctx, 
    const EVP_CIPHER *cipher, ENGINE *impl, const unsigned char *key, 
    const unsigned char *iv);
__attribute__((weak)) int EVP_EncryptUpdate(EVP_CIPHER_CTX *ctx, 
    unsigned char *out, int *outl, const unsigned char *in, int inl);
__attribute__((weak)) int EVP_EncryptFinal_ex(EVP_CIPHER_CTX *ctx, 
    unsigned char *out, int *outl);
__attribute__((weak)) int EVP_DecryptInit_ex(EVP_CIPHER_CTX *ctx, 
    const EVP_CIPHER *cipher, ENGINE *impl, const unsigned char *key, 
    const unsigned char *iv);
__attribute__((weak)) int EVP_DecryptUpdate(EVP_CIPHER_CTX *ctx, 
    unsigned char *out, int *outl, const unsigned char *in, int inl);
__attribute__((weak)) int EVP_DecryptFinal_ex(EVP_CIPHER_CTX *ctx, 
    unsigned char *outm, int *outl);

__attribute__((weak)) const EVP_CIPHER *EVP_aes_256_cbc(void);
__attribute__((weak)) const EVP_CIPHER *EVP_aes_256_ctr(void);
__attribute__((weak)) const EVP_CIPHER *EVP_aes_256_gcm(void);
__attribute__((weak)) const EVP_CIPHER *EVP_aes_256_ecb(void);

__attribute__((weak)) int OPENSSL_init_crypto(uint64_t opts, const OPENSSL_INIT_SETTINGS *settings);

__attribute__((weak)) unsigned long ERR_get_error(void);
__attribute__((weak)) void ERR_error_string_n(unsigned long e, char *buf, size_t len);
__attribute__((weak)) int RAND_poll(void);
__attribute__((weak)) int RAND_bytes(unsigned char *buf, int num);

#ifdef __cplusplus
}
#endif
