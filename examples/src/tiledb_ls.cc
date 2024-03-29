/**
 * @file   tiledb_list.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 * @copyright Copyright (c) 2018 Omics Data Automation, Inc.
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
 * It shows how to explore the contents of a TileDB directory.
 */

#include "examples.h"

int main(int argc, char** argv) {
  char *home;
  TileDB_CTX* tiledb_ctx;
  if (argc > 1) {
    TileDB_Config tiledb_config;
    home = strdup(argv[1]);
    tiledb_config.home_ = home;
    CHECK_RC(tiledb_ctx_init(&tiledb_ctx, &tiledb_config));
  } else {
    home = (char *)malloc(TILEDB_NAME_MAX_LEN+1);
    home = getcwd(home, TILEDB_NAME_MAX_LEN);
    CHECK_RC(tiledb_ctx_init(&tiledb_ctx, NULL));
  }

  // Retrieve number of directories
  int dir_num;
  CHECK_RC(tiledb_ls_c(tiledb_ctx, home, &dir_num));

  // Exit if there are not TileDB objects in the input directory_
  if(dir_num == 0)
    return 0;

  // Initialize variables
  char** dirs = new char*[dir_num];
  int* dir_types = new int[dir_num];
  for(int i=0; i<dir_num; ++i)
    dirs[i] = (char*) malloc(TILEDB_NAME_MAX_LEN);

  // List TileDB objects
  CHECK_RC(tiledb_ls(
      tiledb_ctx,                                    // Context
      home,                                          // Parent directory
      dirs,                                          // Directories
      dir_types,                                     // Directory types
      &dir_num));                                    // Directory number

  // Print TileDB objects
  for(int i=0; i<dir_num; ++i) {
    printf("%s ", dirs[i]);
    if(dir_types[i] == TILEDB_ARRAY)
      printf("ARRAY\n");
    else if(dir_types[i] == TILEDB_METADATA)
      printf("METADATA\n");
    else if(dir_types[i] == TILEDB_GROUP)
      printf("GROUP\n");
    else if(dir_types[i] == TILEDB_WORKSPACE)
      printf("WORKSPACE\n");
  }
 
  // Clean up
  for(int i=0; i<dir_num; ++i)
    free(dirs[i]);
  delete[] dirs;
  delete[] dir_types;
  free(home);

  // Finalize context
  CHECK_RC(tiledb_ctx_finalize(tiledb_ctx));

  return 0;
}
