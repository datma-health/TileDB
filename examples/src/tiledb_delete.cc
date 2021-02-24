/**
 * @file   tiledb_delete.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2019 Omics Data Automation, Inc.
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
 * Recursive delete a TileDB directory
 */

#include "tiledb.h"
#include "tiledb_utils.h"
#include <iostream>
#include <string.h>

int main(int argc, char *argv[]) {
  if (argc > 2 && strcmp(argv[2], "-f") == 0) {
    std::cerr << "Forcibly deleting everything under " << argv[1] << std::endl;
    if (TileDBUtils::is_dir(argv[1])) {
      TileDBUtils::delete_dir(argv[1]);
    }
    return 0;
  }
    
  // Initialize context with home dir if specified in command line, else
  // initialize with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  if (argc > 1) {
    TileDB_Config tiledb_config;
    tiledb_config.home_ = argv[1];
    tiledb_ctx_init(&tiledb_ctx, &tiledb_config);

    // Delete a workspace/group/array/fragment
    tiledb_delete(tiledb_ctx, argv[1]);
    
    // Finalize context
    tiledb_ctx_finalize(tiledb_ctx);
  } else {
    std::cerr << "Usage: tiledb_delete <either_posix_dir_or_cloud_url> [-f]" << std::endl;
  }
  
  return 0;
}
