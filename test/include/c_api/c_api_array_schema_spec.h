/**
 * @file   c_api_array_schema_spec.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 * @copyright Copyright (c) 2020 Omics Data Automation, Inc.
 * @copyright Copyright (c) 2023 dātma, inc™
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
 * Declarations for testing the C API array schema spec.
 */

#ifndef __C_API_ARRAY_SCHEMA_SPEC_H__
#define __C_API_ARRAY_SCHEMA_SPEC_H__

#include "tiledb.h"
#include "storage_posixfs.h"

#include "catch.h"

/** Test fixture for the array schema. */
class ArraySchemaTestFixture : TempDir {

 public:
  /* ********************************* */
  /*             CONSTANTS             */
  /* ********************************* */

  /** Workspace folder name. */
  const std::string WORKSPACE = get_temp_dir() + "/schema_workspace/";
  /** 
   * Array name.
   * Format: (<domain_size_1>x<domain_size_2>_<tile_extent_1>x<tile_extent_2>). 
   */
  const std::string ARRAYNAME = "dense_test_100x100_10x10";




  /* ********************************* */
  /*    CONSTRUCTOR/DESTRUCTOR         */
  /* ********************************* */
  ArraySchemaTestFixture();
  ~ArraySchemaTestFixture();


  /* ********************************* */
  /*           PUBLIC METHODS          */
  /* ********************************* */

  /** 
   * Creates a dense array. 
   *
   * @return TILEDB_OK on success and TILEDB_ERR on error.
   */
  int create_dense_array(std::string array_name, int attribute_datatype, int compression_type);

  void check_dense_array(std::string array_name);


  /* ********************************* */
  /*         PUBLIC ATTRIBUTES         */
  /* ********************************* */

  /** Array name. */
  std::string array_name_;
  /** Array schema object under test. */
  TileDB_ArraySchema array_schema_;
  /** True if the array schema is set. */
  bool array_schema_set_;
  /** TileDB context. */
  TileDB_CTX* tiledb_ctx_;
  PosixFS fs_;
};

#endif
