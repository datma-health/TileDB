/**
 * @file   codec.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2020 Omics Data Automation, Inc.
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
#include "codec_lz4.h"
#ifdef ENABLE_BLOSC
#  include "codec_blosc.h"
#endif
#include "codec_rle.h"
#include "codec_filter.h"
#include "codec_filter_bit_shuffle.h"
#include "codec_filter_delta_encode.h"
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

int get_filter_type(const ArraySchema* array_schema, const int attribute_id,
                         const bool is_offsets_compression, filter_type_t filter_type) {
  if (is_offsets_compression) {
    return array_schema->offsets_compression(attribute_id) & filter_type;
  } else {
    return array_schema->compression(attribute_id) & filter_type;
  }
}

int get_filter_level(const ArraySchema* array_schema, const int attribute_id,
                         const bool is_offsets_compression) {
  if (is_offsets_compression) {
    return array_schema->offsets_compression_level(attribute_id);
  } else {
    return array_schema->compression_level(attribute_id);
  }
}

static std::map<int, Codec::create_fn_t> registered_codecs_;
void Codec::register_codec(int compression_type, Codec::create_fn_t create_fn) {
  registered_codecs_.insert({compression_type, create_fn});
}

bool Codec::is_registered_codec(int compression_type) {
  return registered_codecs_.find(compression_type) != registered_codecs_.end();
}

Codec::create_fn_t Codec::get_registered_codec(int compression_type) {
  auto registered_codec = registered_codecs_.find(compression_type);
  if (registered_codec != registered_codecs_.end()) {
    return registered_codec->second;
  }
  return nullptr;
}

Codec* Codec::create(const ArraySchema* array_schema, const int attribute_id, const bool is_offsets_compression) {
  int compression_type = get_filter_type(array_schema, attribute_id, is_offsets_compression, COMPRESS);
  if (compression_type == TILEDB_NO_COMPRESSION) {
    return NULL;
  }

  // Check if there is an external creator registered first
  auto codec_creator = registered_codecs_.find(compression_type);
  if (codec_creator != registered_codecs_.end()) {
    return codec_creator->second(array_schema, attribute_id, is_offsets_compression);
  }
  
  int compression_level = get_filter_level(array_schema, attribute_id, is_offsets_compression);
  Codec* codec = NULL;

  switch (compression_type) {
    case TILEDB_GZIP:
      codec = new CodecGzip(compression_level);
      break;
#ifdef ENABLE_ZSTD
    case TILEDB_ZSTD:
      codec = new CodecZStandard(compression_level);
      break;
#endif
    case TILEDB_LZ4:
      codec = new CodecLZ4(compression_level);
      break;
#ifdef ENABLE_BLOSC
    case TILEDB_BLOSC:
    case TILEDB_BLOSC_LZ4:
    case TILEDB_BLOSC_LZ4HC:
    case TILEDB_BLOSC_SNAPPY:
    case TILEDB_BLOSC_ZLIB:
    case TILEDB_BLOSC_ZSTD: {
      size_t type_size;
      if (is_offsets_compression) {
        type_size = sizeof(size_t);
      } else {
        type_size = array_schema->type_size(attribute_id);
      }
      codec = new CodecBlosc(compression_level, get_blosc_compressor(compression_type), type_size);
      break;
    }
#endif
    case TILEDB_RLE: {
      int attribute_num = array_schema->attribute_num();
      int dim_num = array_schema->dim_num();
      int cell_order = array_schema->cell_order();
      bool is_coords = (attribute_id == attribute_num);
      // TODO: visit offsets compression for RLE
      size_t value_size = 
          (array_schema->var_size(attribute_id) || is_coords) ? 
          array_schema->type_size(attribute_id) :
          array_schema->cell_size(attribute_id);
      codec = new CodecRLE(attribute_num, dim_num, cell_order, is_coords, value_size);
      break;
    }
    default:
      // TODO throw exception
      std::cerr << "Unsupported compression type:" << compression_type << "\n";
      return NULL;
  }

  int pre_compress_type = get_filter_type(array_schema, attribute_id, is_offsets_compression, PRE_COMPRESS);
  switch (pre_compress_type) {
    case 0:
      break;
    case TILEDB_DELTA_ENCODE:
      CodecFilter* filter;
      if (array_schema->attribute(attribute_id) == TILEDB_COORDS) {
        filter = new CodecDeltaEncode(array_schema->type(attribute_id), array_schema->dim_num());
      } else if (is_offsets_compression) {
        filter = new CodecDeltaEncode(TILEDB_UINT64, 1);
      } else {
        filter = new CodecDeltaEncode(array_schema->type(attribute_id), array_schema->cell_val_num(attribute_id));
      }
      codec->set_pre_compression(filter);
      break;
    case TILEDB_BIT_SHUFFLE:
      codec->set_pre_compression(new CodecBitShuffle(array_schema->type(attribute_id)));
      break;
  default:
      std::cerr << "Unsupported pre-compression filter: " << pre_compress_type << "\n";
  }

  int post_compress_type = get_filter_type(array_schema, attribute_id, is_offsets_compression, POST_COMPRESS);
  switch (post_compress_type) {
    case 0:
      break;
    default:
      std::cerr << "Unsupported post-compression filter: " << post_compress_type << "\n";
  }

  return codec;
}

int Codec::get_default_level(int compression_type) {
  switch(compression_type) {
  case TILEDB_GZIP:
    return TILEDB_COMPRESSION_LEVEL_GZIP;
  case TILEDB_ZSTD:
    return TILEDB_COMPRESSION_LEVEL_ZSTD;
  case TILEDB_BLOSC:
    return TILEDB_COMPRESSION_LEVEL_BLOSC;
  default:
    return -1;
  }
}

int Codec::print_errmsg(const std::string& msg) {
  if (msg.length() > 0) {
    PRINT_ERROR(msg);
    tiledb_cd_errmsg = TILEDB_CD_ERRMSG + msg;
  }
  return TILEDB_CD_ERR;
}

int Codec::compress_tile(unsigned char* tile, size_t tile_size, void** tile_compressed, size_t& tile_compressed_size) {
  unsigned char* tile_precompressed = tile;
  if (pre_compression_filter_) {
    if (pre_compression_filter_->code(tile, tile_size) != 0) {
      return print_errmsg("Could not apply filter " + pre_compression_filter_->name() + " before compressing");
    }
    if (!pre_compression_filter_->in_place()) {
      if (pre_compression_filter_->buffer() == NULL) {
        return print_errmsg("Error from precompression filter " + pre_compression_filter_->name());
      }
      tile_precompressed = pre_compression_filter_->buffer();
    }
  }

  if (do_compress_tile(tile_precompressed, tile_size, tile_compressed, tile_compressed_size)) {
    return print_errmsg("Could not compress with " + name());
  }
  
  return TILEDB_CD_OK;
}

int Codec::decompress_tile(unsigned char* tile_compressed, size_t tile_compressed_size, unsigned char* tile, size_t tile_size) {
  unsigned char* buffer = tile;
  if (pre_compression_filter_ && !pre_compression_filter_->in_place()) {
    if (pre_compression_filter_->allocate_buffer(tile_size) != TILEDB_CDF_OK) {
      return print_errmsg("OOM while trying to allocate memory for decompress using " + pre_compression_filter_->name());
    }
    buffer = pre_compression_filter_->buffer();
  }
  if (do_decompress_tile(tile_compressed, tile_compressed_size, buffer, tile_size)) {
    return print_errmsg("Could not compress with " + name());
  }
  if (pre_compression_filter_) {
    if (pre_compression_filter_->decode(tile, tile_size) != 0) {
      return print_errmsg("Could not apply filter " + pre_compression_filter_->name() + " after decompressing");
    }
  }
  return TILEDB_CD_OK;
}
