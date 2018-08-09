/**
 * @file   codec.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 Omics Data Automation, Inc.
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
 * This file implements the base class for compression and decompression.
 */

#include "codec.h"
#include "codec_gzip.h"
#ifdef ENABLE_ZSTD
#  include "codec_zstd.h"
#endif
#ifdef ENABLE_LZ4
#  include "codec_lz4.h"
#endif
#ifdef ENABLE_BLOSC
#  include "codec_blosc.h"
#endif
#include "codec_rle.h"
#include "tiledb.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef TILEDB_VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_CD_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif


/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_cd_errmsg = "";

/* ****************************** */
/*        FACTORY METHODS         */
/* ****************************** */

#ifdef ENABLE_BLOSC
static std::string get_blosc_compressor(const int compression_type) {
  switch (compression_type) {
    case TILEDB_BLOSC:
      return "blosclz";
    case TILEDB_BLOSC_LZ4:
      return "lz4";
    case TILEDB_BLOSC_LZ4HC:
      return "lz4hc";
    case TILEDB_BLOSC_SNAPPY:
      return "snappy";
    case TILEDB_BLOSC_ZLIB:
      return "zlib";
    case TILEDB_BLOSC_ZSTD:
      return "zstd";
    default:
      return "";
  };
}
#endif

Codec* Codec::create(const ArraySchema* array_schema, const int attribute_id) {
  int compression_type = array_schema->compression(attribute_id);
  int compression_level = array_schema->compression_level(attribute_id);
  
  switch (compression_type) {
    case TILEDB_NO_COMPRESSION:
      return NULL;
    case TILEDB_GZIP:
      return new CodecGzip(compression_level);
#ifdef ENABLE_ZSTD
    case TILEDB_ZSTD:
      return new CodecZStandard(compression_level);
#endif
#ifdef ENABLE_LZ4
    case TILEDB_LZ4:
      return new CodecLZ4(compression_level);
#endif
#ifdef ENABLE_BLOSC
    case TILEDB_BLOSC:
    case TILEDB_BLOSC_LZ4:
    case TILEDB_BLOSC_LZ4HC:
    case TILEDB_BLOSC_SNAPPY:
    case TILEDB_BLOSC_ZLIB:
    case TILEDB_BLOSC_ZSTD: {
      size_t type_size = array_schema->type_size(attribute_id);
      return new CodecBlosc(compression_level, get_blosc_compressor(compression_type), type_size);
    }
#endif
    case TILEDB_RLE: {
      int attribute_num = array_schema->attribute_num();
      int dim_num = array_schema->dim_num();
      int cell_order = array_schema->cell_order();
      bool is_coords = (attribute_id == attribute_num);
      size_t value_size = 
      (array_schema->var_size(attribute_id) || is_coords) ? 
          array_schema->type_size(attribute_id) :
          array_schema->cell_size(attribute_id);
      return new CodecRLE(attribute_num, dim_num, cell_order, is_coords, value_size);
    }
    default:
      // TODO throw exception
      std::cerr << "Unsupported compression type:" << compression_type << "\n";
      return NULL;
  }
}

int Codec::print_errmsg(const std::string& msg) {
  if (msg.length() > 0) {
    PRINT_ERROR(msg);
    tiledb_cd_errmsg = TILEDB_CD_ERRMSG + msg;
  }
  return TILEDB_CD_ERR;
}
