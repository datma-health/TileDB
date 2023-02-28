#pragma once

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#include <bcrypt.h>
#else
#include <openssl/hmac.h>
#endif


//OpenSSL1 functions redirected to call dynamically using the below functions

#define MD5_LONG_OSSL1_SHIM unsigned int
#define MD5_CBLOCK_OSSL1_SHIM      64
#define MD5_LBLOCK_OSSL1_SHIM      (MD5_CBLOCK_OSSL1_SHIM/4)

typedef struct MD5state_ossl1_shim_st {
  MD5_LONG_OSSL1_SHIM A, B, C, D;
  MD5_LONG_OSSL1_SHIM Nl, Nh; 
  MD5_LONG_OSSL1_SHIM data[MD5_LBLOCK_OSSL1_SHIM];
  unsigned int num;
} MD5_CTX_OSSL1_SHIM;

#define MD5_CTX MD5_CTX_OSSL1_SHIM;

#define SHA_LONG_OSSL1_SHIM unsigned int
#define SHA_LBLOCK_OSSL1_SHIM      16
typedef struct SHA256state_ossl1_shim_st {
    SHA_LONG_OSSL1_SHIM h[8];
    SHA_LONG_OSSL1_SHIM Nl, Nh; 
    SHA_LONG_OSSL1_SHIM data[SHA_LBLOCK_OSSL1_SHIM];
    unsigned int num, md_len;
} SHA256_CTX_OSSL1_SHIM;

#define SHA256_CTX SHA256_CTX_OSSL1_SHIM;


#define HMAC_CTX_new   HMAC_CTX_new_ossl1_shim
#define HMAC_CTX_reset HMAC_CTX_reset_ossl1_shim
#define HMAC_Init_ex   HMAC_Init_ex_ossl1_shim
#define HMAC_Update    HMAC_Update_ossl1_shim
#define HMAC_Final     HMAC_Final_ossl1_shim
#define HMAC_CTX_free  HMAC_CTX_free_ossl1_shim
#define MD5_Init       MD5_Init_ossl1_shim
#define MD5_Update     MD5_Update_ossl1_shim
#define MD5_Final      MD5_Final_ossl1_shim
#define SHA256_Init    SHA256_Init_ossl1_shim
#define SHA256_Final   SHA256_Final_ossl1_shim
#define SHA256_Update  SHA256_Update_ossl1_shim


void *HMAC_CTX_new_ossl1_shim(void); 
int HMAC_CTX_reset_ossl1_shim(void *ctx);
int HMAC_Init_ex_ossl1_shim(void *ctx, const void *key, int key_len, 
    const void *md, void *impl);
int HMAC_Update_ossl1_shim(void *ctx, const unsigned char *data, size_t len);
int HMAC_Final_ossl1_shim(void *ctx, unsigned char *md, unsigned int *len);
void HMAC_CTX_free_ossl1_shim(void *ctx);

int MD5_Init_ossl1_shim(void *c);
int MD5_Update_ossl1_shim(void *c, const void *data, size_t len);
int MD5_Final_ossl1_shim(unsigned char *md, void *c);

int SHA256_Init_ossl1_shim(void *c);
int SHA256_Final_ossl1_shim(unsigned char *md, void *c);
int SHA256_Update_ossl1_shim(void *c, const void *data, size_t len);

//OpenSSL3 functions redirected to call dynamically using the below functions
struct ossl_param_ossl3_shim_st {
  const char *key;             /* the name of the parameter */
  unsigned int data_type;      /* declare what kind of content is in buffer */
  void *data;                  /* value being passed in or out */
  size_t data_size;            /* data size */
  size_t return_size;          /* returned content size */
};

typedef struct ossl_param_ossl3_shim_st OSSL_PARAM_OSSL3_SHIM;
#define OSSL_PARAM      OSSL_PARAM_OSSL3_SHIM

#define OSSL_MAC_PARAM_DIGEST              OSSL_ALG_PARAM_DIGEST_OSSL3_SHIM   /* utf8 string */
#define OSSL_MAC_PARAM_DIGEST_OSSL3_SHIM   OSSL_ALG_PARAM_DIGEST_OSSL3_SHIM   /* utf8 string */
#define OSSL_ALG_PARAM_DIGEST_OSSL3_SHIM   "digest"    /* utf8_string */

#define EVP_MAC_fetch       EVP_MAC_fetch_ossl3_shim
#define EVP_MAC_CTX_new     EVP_MAC_CTX_new_ossl3_shim
#define EVP_MAC_CTX_free    EVP_MAC_CTX_free_ossl3_shim
#define EVP_MAC_init        EVP_MAC_init_ossl3_shim
#define EVP_MAC_update      EVP_MAC_update_ossl3_shim
#define EVP_MAC_final       EVP_MAC_final_ossl3_shim
#define EVP_MD_get_size     EVP_MD_get_size_ossl3_shim
#define EVP_DigestInit_ex   EVP_DigestInit_ex_ossl3_shim
#define EVP_DigestUpdate    EVP_DigestUpdate_ossl3_shim
#define EVP_DigestFinal_ex  EVP_DigestFinal_ex_ossl3_shim
#define EVP_MD_CTX_free     EVP_MD_CTX_free_ossl3_shim
/* #define EVP_md5             EVP_md5_ossl3_shim
   #define EVP_MD_CTX_new      EVP_MD_CTX_new_ossl3_shim 
   #define EVP_sha256          EVP_sha256_ossl3_shim */
#define OSSL_PARAM_construct_utf8_string OSSL_PARAM_construct_utf8_string_ossl3_shim
#define OSSL_PARAM_construct_end OSSL_PARAM_construct_end_ossl3_shim

void* EVP_MAC_fetch_ossl3_shim(void *libctx, const char *algorithm, 
    const char *properties);
void* EVP_MAC_CTX_new_ossl3_shim(void *mac);
void EVP_MAC_CTX_free_ossl3_shim(void *ctx);
int EVP_MAC_init_ossl3_shim(void *ctx, const unsigned char *key, 
    size_t keylen, const OSSL_PARAM params[]);
int EVP_MAC_update_ossl3_shim(void *ctx, const unsigned char *data, 
    size_t datalen);
int EVP_MAC_final_ossl3_shim(void *ctx, unsigned char *out, size_t *outl, 
    size_t outsize);
OSSL_PARAM_OSSL3_SHIM OSSL_PARAM_construct_utf8_string_ossl3_shim(
    const char *key, char *buf, size_t bsize);
OSSL_PARAM_OSSL3_SHIM OSSL_PARAM_construct_end_ossl3_shim(void);
int EVP_MD_get_size_ossl3_shim(const void *md);
int EVP_DigestInit_ex_ossl3_shim(void *ctx, const void *type, void *impl);
int EVP_DigestUpdate_ossl3_shim(void *ctx, const void *d, size_t cnt);
int EVP_DigestFinal_ex_ossl3_shim(void *ctx, unsigned char *md, unsigned int *s);
void EVP_MD_CTX_free_ossl3_shim(void *ctx);
const void* EVP_md5_ossl3_shim(void);
void* EVP_MD_CTX_new_ossl3_shim(void);
const void* EVP_sha256_ossl3_shim(void);

