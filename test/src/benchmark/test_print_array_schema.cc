/**
 * @file   test_array_print_schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2020 Omics Data Automation, Inc.
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
 * Given a File or Cloud URL to a TileDB array, print out its array schema
 */

#include "array_schema.h"
#include "tiledb.h"
#include "tiledb_storage.h"

#include <iostream>
#include <string>

#define CHECK_RC(...)                                      \
do {                                                       \
  int rc = __VA_ARGS__;                                    \
  if (rc) {                                                \
    printf("%s", &tiledb_errmsg[0]);                       \
    printf("[PrintArraySchema::%s] Runtime Error.\n", __FILE__);   \
    return rc;                                             \
  }                                                        \
} while (false)

int main(int argc, char *argv[]) {
  if (argc < 2) {
    std::cerr << "No arguments specified\n";
    std::cerr << "Usage: test_print_array_schema /path/to/TileDBArray\n";
    return 0;
  }
  
  std::string array(argv[1]);
  std::string array_schema_filename(array+'/'+TILEDB_ARRAY_SCHEMA_FILENAME);
    
  TileDB_CTX* tiledb_ctx;
  TileDB_Config tiledb_config;
  memset(&tiledb_config, 0, sizeof(TileDB_Config));
  tiledb_config.home_ = array.c_str();
  CHECK_RC(tiledb_ctx_init(&tiledb_ctx, &tiledb_config));

  if (!is_array(tiledb_ctx, array)) {
    std::cerr << "Array " << array << " does not seem to be a TileDB Array. Exiting\n";
    return 1;
  }

  size_t buffer_size = file_size(tiledb_ctx, array_schema_filename);
  void *buffer = malloc(buffer_size);
  
  // Load array schema into buffer
  CHECK_RC(read_file(tiledb_ctx, array_schema_filename, 0, buffer, buffer_size));

  // Initialize array schema from buffer
  ArraySchema* array_schema = new ArraySchema(NULL);
  CHECK_RC(array_schema->deserialize(buffer, buffer_size));

  // Print the schema
  array_schema->print();

  delete array_schema;
  free(buffer);

  std::cerr << "\n\n";

  // Finalize context
  CHECK_RC(tiledb_ctx_finalize(tiledb_ctx));

  return 0;
}

