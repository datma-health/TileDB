/**
 * @file   test_codec.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019-2020 Omics Data Automation, Inc.
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
 * Test compression/decompression tiledb classes
 */

#include "catch.h"
#include "codec.h"
#include "codec_gzip.h"
#include "tiledb.h"

#include <limits.h>

class TestCodecBasic : public Codec {
 public:
  using Codec::Codec;
  int do_compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size) {
    return TILEDB_CD_OK;
  }
  int do_decompress_tile(unsigned char* tile_compressed,  size_t tile_compressed_size, unsigned char* tile, size_t tile_size) {
    return TILEDB_CD_OK;
  }
};

TEST_CASE("Test codec static methods", "[codec_static]") {
  CHECK(Codec::get_default_level(TILEDB_GZIP) == TILEDB_COMPRESSION_LEVEL_GZIP);
  CHECK(Codec::get_default_level(INT_MAX) == -1);
}

TEST_CASE("Test codec basic", "[codec_basic]") {
  TestCodecBasic *codec_basic = new TestCodecBasic(0);
  
  void *dl_handle = codec_basic->get_dlopen_handle("non-existent-library");
  CHECK(dl_handle == NULL);
  CHECK(!codec_basic->get_dlerror().empty());

  dl_handle = codec_basic->get_dlopen_handle("non-existent-library", "5.2");
  CHECK(dl_handle == NULL);
  CHECK(!codec_basic->get_dlerror().empty());
  
  dl_handle = codec_basic->get_dlopen_handle("z");
  CHECK(dl_handle);
  CHECK(codec_basic->get_dlerror().empty());

  dl_handle = codec_basic->get_dlopen_handle("z", "1");
  CHECK(dl_handle);
  CHECK(codec_basic->get_dlerror().empty());
  
  dl_handle = codec_basic->get_dlopen_handle("z", "non-existent-version");
  CHECK(dl_handle == NULL);
  CHECK(!codec_basic->get_dlerror().empty());

  free(codec_basic);
}

TEST_CASE("Test zlib", "[codec-z]") {
  Codec* zlib = new CodecGzip(TILEDB_COMPRESSION_LEVEL_GZIP);
  unsigned char test_string[] = "HELLO";
  unsigned char* buffer;
  size_t buffer_size;

  CHECK(zlib->compress_tile(test_string, 0, (void **)(&buffer), buffer_size) == TILEDB_CD_OK);
  CHECK(zlib->compress_tile(test_string, 6, (void **)(&buffer), buffer_size) == TILEDB_CD_OK);

  unsigned char* decompressed_string =  (unsigned char*)malloc(6);
  CHECK(zlib->decompress_tile(buffer, buffer_size, decompressed_string, 6) == TILEDB_CD_OK);

  // Should get Z_BUF_ERROR
  CHECK(zlib->decompress_tile(buffer, buffer_size, decompressed_string, 4) == TILEDB_CD_ERR);
  CHECK(zlib->decompress_tile(buffer, 4, decompressed_string, 6) == TILEDB_CD_ERR);
  // Should get Z_DATA_ERROR
  CHECK(zlib->decompress_tile(test_string, 4, decompressed_string, 6) == TILEDB_CD_ERR);
   
  free(zlib);
}

#include "codec_lz4.h"

TEST_CASE("Test lz4", "[codec-lz4]") {
  Codec* lz4 = new CodecLZ4(TILEDB_COMPRESSION_LEVEL_LZ4, 1);
  unsigned char test_string[] = "HELLO";
  unsigned char* buffer;
  size_t buffer_size;

  CHECK(lz4->compress_tile(test_string, 0, (void **)(&buffer), buffer_size) == TILEDB_CD_OK);
  CHECK(lz4->compress_tile(test_string, 6, (void **)(&buffer), buffer_size) == TILEDB_CD_OK);

  unsigned char* decompressed_string =  (unsigned char*)malloc(6);
  CHECK(lz4->decompress_tile(buffer, buffer_size, decompressed_string, 6) == TILEDB_CD_OK);
  CHECK(strlen((char *)decompressed_string) == 5);
  CHECK(strcmp((char *)decompressed_string, (char *)test_string) == 0);

  free(lz4);

  // Try lz4 with bit-shuffline
  lz4 = new CodecLZ4(TILEDB_COMPRESSION_LEVEL_BSHUF_LZ4, 1);

  CHECK(lz4->compress_tile(test_string, 0, (void **)(&buffer), buffer_size) == TILEDB_CD_OK);
  CHECK(lz4->compress_tile(test_string, 6, (void **)(&buffer), buffer_size) == TILEDB_CD_OK);

  memset(decompressed_string, 0, 6);
  CHECK(lz4->decompress_tile(buffer, buffer_size, decompressed_string, 6) == TILEDB_CD_OK);
  CHECK(strlen((char *)decompressed_string) == 5);
  CHECK(strcmp((char *)decompressed_string, (char *)test_string) == 0);

  free(lz4);

  free(decompressed_string);
}

