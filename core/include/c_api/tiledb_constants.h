/**
 * @file   tiledb_constants.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file contains global constants. 
 */

#ifndef __TILEDB_CONSTANTS_H__
#define __TILEDB_CONSTANTS_H__

#include <float.h>
#include <limits.h>
#include <zlib.h>
#include <stdint.h>

/** Version. */
#define TILEDB_VERSION                          "0.5.2"

/**@{*/
/** Return code. */  
#define TILEDB_ERR                                  -1
#define TILEDB_OK                                    0
/**@}*/

/**@{*/
/** Array mode. */
#define TILEDB_ARRAY_READ                           0
#define TILEDB_ARRAY_READ_SORTED_COL                1
#define TILEDB_ARRAY_READ_SORTED_ROW                2
#define TILEDB_ARRAY_WRITE                          3
#define TILEDB_ARRAY_WRITE_SORTED_COL               4
#define TILEDB_ARRAY_WRITE_SORTED_ROW               5
#define TILEDB_ARRAY_WRITE_UNSORTED                 6
#define TILEDB_ARRAY_CONSOLIDATE                    7
/**@}*/

/**@{*/
/** Metadata mode. */
#define TILEDB_METADATA_READ                         0
#define TILEDB_METADATA_WRITE                        1
/**@}*/

/**@{*/
/** I/O method. */
#define TILEDB_IO_MMAP                              0
#define TILEDB_IO_READ                              1
#define TILEDB_IO_MPI                               2
#define TILEDB_IO_WRITE                             0
/**@}*/

/**@{*/
/** Asynchronous I/O (AIO) code. */  
#define TILEDB_AIO_ERR                                  -1
#define TILEDB_AIO_COMPLETED                             0
#define TILEDB_AIO_INPROGRESS                            1
#define TILEDB_AIO_OVERFLOW                              2
/**@}*/

/** 
 * The TileDB home directory, where TileDB-related system metadata structures
 * are kept. If it is set to "", then the home directory is set to "~/.tiledb"
 * by default.
 */
#define TILEDB_HOME                                 ""

/**@{*/
/** TileDB object type. */
#define TILEDB_WORKSPACE                             0
#define TILEDB_GROUP                                 1
#define TILEDB_ARRAY                                 2
#define TILEDB_METADATA                              3
/**@}*/

/** The maximum length for the names of TileDB objects. */
#define TILEDB_NAME_MAX_LEN                        4096

/** Size of the buffer used during consolidation. */
#define TILEDB_CONSOLIDATION_BUFFER_SIZE      10000000 // ~10 MB

/**@{*/
/** Special empty cell value. */
#define TILEDB_EMPTY_CHAR                      CHAR_MAX
#define TILEDB_EMPTY_INT8                      INT8_MAX
#define TILEDB_EMPTY_INT16                     INT16_MAX
#define TILEDB_EMPTY_INT32                     INT32_MAX
#define TILEDB_EMPTY_INT64                     INT64_MAX
#define TILEDB_EMPTY_UINT8                     UINT8_MAX
#define TILEDB_EMPTY_UINT16                    UINT16_MAX
#define TILEDB_EMPTY_UINT32                    UINT32_MAX
#define TILEDB_EMPTY_UINT64                    UINT64_MAX
#define TILEDB_EMPTY_FLOAT32                   FLT_MAX
#define TILEDB_EMPTY_FLOAT64                   DBL_MAX
/**@}*/

/**@{*/
/** Special value indicating a variable number or size. */
#define TILEDB_VAR_NUM                         INT_MAX
#define TILEDB_VAR_SIZE                     (size_t)-1
/**@}*/

/**@{*/
/** Data type. */
#define TILEDB_INT32                                 0
#define TILEDB_INT64                                 1
#define TILEDB_FLOAT32                               2
#define TILEDB_FLOAT64                               3
#define TILEDB_CHAR                                  4
#define TILEDB_INT8                                  5
#define TILEDB_UINT8                                 6
#define TILEDB_INT16                                 7
#define TILEDB_UINT16                                8
#define TILEDB_UINT32                                9
#define TILEDB_UINT64                               10
/**@}*/

/**@{*/
/** Tile or cell order. */
#define TILEDB_ROW_MAJOR                             0
#define TILEDB_COL_MAJOR                             1
#define TILEDB_HILBERT                               2
/**@}*/

/**@{*/
/** Compression type. Range from 0-15 */
#define TILEDB_NO_COMPRESSION                        0
#define TILEDB_GZIP                                  1
#define TILEDB_ZSTD                                  2
#define TILEDB_LZ4                                   3
#define TILEDB_BLOSC                                 4
#define TILEDB_BLOSC_LZ4                             5
#define TILEDB_BLOSC_LZ4HC                           6
#define TILEDB_BLOSC_SNAPPY                          7
#define TILEDB_BLOSC_ZLIB                            8
#define TILEDB_BLOSC_ZSTD                            9
#define TILEDB_RLE                                  10
/**@}*/

/**@{*/
/** Pre compression filter defines. Can only range from 1 to 3 for now */
#define TILEDB_DELTA_ENCODE                         (1<<4)
#define TILEDB_BIT_SHUFFLE                          (2<<4)
/**@}*/

/**@{*/
/** Post compression filter defines. Can only range from 1 to 3 for now */
#define TILEDB_POST_FILTER_EXAMPLE_UNUSED           (1<<6)
/**@}*/

/**@{*/
/** Special attribute name. */
#define TILEDB_COORDS                       "__coords"
#define TILEDB_KEY                             "__key"
/**@}*/

/**@{*/
/** Special TileDB file name suffix. */
#define TILEDB_FILE_SUFFIX                      ".tdb"
#define TILEDB_GZIP_SUFFIX                       ".gz"
/**@}*/

/** Chunk size in GZIP decompression. */
#define TILEDB_GZIP_CHUNK_SIZE                  131072 // 128KB

/**@{*/
/** Special TileDB file name. */
#define TILEDB_ARRAY_SCHEMA_FILENAME         "__array_schema.tdb"
#define TILEDB_METADATA_SCHEMA_FILENAME   "__metadata_schema.tdb"
#define TILEDB_BOOK_KEEPING_FILENAME             "__book_keeping"
#define TILEDB_FRAGMENT_FILENAME          "__tiledb_fragment.tdb"
#define TILEDB_GROUP_FILENAME                "__tiledb_group.tdb"
#define TILEDB_WORKSPACE_FILENAME        "__tiledb_workspace.tdb"
/**@}*/

/**@{*/
/** Size of buffer used for sorting. */
#define TILEDB_SORTED_BUFFER_SIZE             10000000  // ~10MB
#define TILEDB_SORTED_BUFFER_VAR_SIZE         10000000  // ~10MB
/**@}*/

/**@{*/
/** Compression levels. */
#ifndef TILEDB_COMPRESSION_LEVEL_GZIP
#  define TILEDB_COMPRESSION_LEVEL_GZIP      Z_DEFAULT_COMPRESSION
#endif
#ifndef TILEDB_COMPRESSION_LEVEL_ZSTD
#  define TILEDB_COMPRESSION_LEVEL_ZSTD                          1
#endif
#ifndef TILEDB_COMPRESSION_LEVEL_BLOSC
#  define TILEDB_COMPRESSION_LEVEL_BLOSC                         5
#endif
#ifndef TILEDB_COMPRESSION_LEVEL_LZ4
#define TILEDB_COMPRESSION_LEVEL_LZ4                             1
#endif
/**@}*/

#endif
