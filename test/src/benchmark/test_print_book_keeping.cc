/**
 * @file   test_array_print_book_keeping.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2022 Omics Data Automation, Inc.
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
 * Given a File or Cloud URL to a TileDB fragment, print out the book-keeping contents
 */

#include "array_schema.h"
#include "book_keeping.h"
#include "storage_manager_config.h"
#include "tiledb.h"
#include "tiledb_storage.h"
#include "tiledb_utils.h"

#include <iostream>
#include <string>
#include <string.h>

#define CHECK_RC(...)                                      \
do {                                                       \
  int rc = __VA_ARGS__;                                    \
  if (rc) {                                                \
    printf("%s", &tiledb_errmsg[0]);                       \
    printf("[PrintBookKeeping::%s] Runtime Error.\n", __FILE__);   \
    return rc;                                             \
  }                                                        \
} while (false)

ArraySchema* array_schema;
std::shared_ptr<CompressedStorageBuffer> bk_buffer;
int attribute_num = -1;

int print_non_empty_domain() {
  // Get domain size
  size_t domain_size;
  CHECK_RC(bk_buffer->read_buffer(&domain_size, sizeof(size_t)));
  std::cerr << "Domain size=" << domain_size << std::endl;

  // Get non-empty domain, assuming domain to be int64_t for now
  int64_t domain[domain_size/sizeof(size_t)];
  if(domain_size != 0) {
    CHECK_RC(bk_buffer->read_buffer(&domain[0], domain_size));
    std::cerr << "Domain coords =";
    for (auto i = 0; i<domain_size/sizeof(domain_size); i++) {
      std::cerr << domain[i] << " ";
    }
    std::cerr << std::endl;

    /*
    // This is computed, not persisted in the bookkeeping file
    array_schema->expand_domain(domain);
    int64_domain = static_cast<int64_t*>(domain);
    std::cerr << "Expanded domain coords =";
    for (auto i = 0; i<domain_size/sizeof(domain_size); i++) {
      std::cerr << *int64_domain++ << " ";
    }
    std::cerr << std::endl;
    */
  }
  
  return TILEDB_OK;
}

int print_mbrs() {
  // For easy reference
  size_t mbr_size = 2*array_schema->coords_size();

  // Get number of MBRs
  int64_t mbr_num;
  CHECK_RC(bk_buffer->read_buffer(&mbr_num, sizeof(int64_t)));
  std::cerr << "Number of mbrs=" << mbr_num << std::endl;

  // Get MBRs
  std::cerr << "MBRs :" << std::endl;
  int64_t mbr[mbr_size/sizeof(size_t)];
  for(int64_t i=0; i<mbr_num; ++i) {
    CHECK_RC(bk_buffer->read_buffer(&mbr[0], mbr_size));
    for (auto j = 0; j<mbr_size/sizeof(size_t); j++) {
      std::cerr << mbr[j] << " ";
    }
    std::cerr << std::endl;
  }

  // Success
  return TILEDB_OK;
}

int print_bounding_coords() {
   // For easy reference
  size_t bounding_coords_size = 2*array_schema->coords_size();

  // Get number of bounding coordinates
  int64_t bounding_coords_num;
  CHECK_RC(bk_buffer->read_buffer(&bounding_coords_num, sizeof(int64_t)));
  std::cerr << "Number of bounding coords=" << bounding_coords_num << std::endl;

  // Get bounding coordinates
  std::cerr << "Bounding Coords :" << std::endl;
  int64_t bounding_coords[bounding_coords_size/sizeof(int64_t)];
  for(int64_t i=0; i<bounding_coords_num; ++i) {
    CHECK_RC(bk_buffer->read_buffer(&bounding_coords[0], bounding_coords_size));
    for (auto j = 0; j<bounding_coords_size/sizeof(int64_t); j++) {
      std::cerr << bounding_coords[j] << " ";
    }
    std::cerr << std::endl;
  }

  // Success
  return TILEDB_OK;
}

int print_tile_offsets() {
  // For all attributes, get the tile offsets
  int64_t tile_offsets_num;
  for(int i=0; i<attribute_num+1; ++i) {
    // Get number of tile offsets
    tile_offsets_num = 0;
    CHECK_RC(bk_buffer->read_buffer(&tile_offsets_num, sizeof(int64_t)));
    std::cerr << "Tile offsets for attribute=" << array_schema->attribute(i) << " tile_offsets_num="
              << tile_offsets_num << std::endl;
    if(tile_offsets_num == 0) {
      continue;
    }

    // Get tile offsets
    off_t tile_offsets[tile_offsets_num];
    CHECK_RC(bk_buffer->read_buffer(&tile_offsets[0], tile_offsets_num * sizeof(off_t)));
    for (auto j = 0; j<tile_offsets_num; j++) {
      std::cerr << tile_offsets[j] << " ";
    }
    std::cerr << std::endl;
  }

  // Success
  return TILEDB_OK;
}

int print_tile_var_offsets() {
  // For all attributes, get the variable tile offsets
  int64_t tile_var_offsets_num;
  for(int i=0; i<attribute_num; ++i) {
    // Get number of tile var offsets
    CHECK_RC(bk_buffer->read_buffer(&tile_var_offsets_num, sizeof(int64_t)));
    std::cerr << "Tile var offsets for attribute=" << array_schema->attribute(i) << " tile_var_offsets_num="
          << tile_var_offsets_num << std::endl;
    if(tile_var_offsets_num == 0)
      continue;

    // Get variable tile offsets
    off_t tile_var_offsets[tile_var_offsets_num];
    CHECK_RC(bk_buffer->read_buffer(&tile_var_offsets[0], tile_var_offsets_num * sizeof(off_t)));
    for (auto j = 0; j<tile_var_offsets_num; j++) {
      std::cerr << tile_var_offsets[j] << " ";
    }
    std::cerr << std::endl;
  }

  // Success
  return TILEDB_OK;
}

int print_tile_var_sizes() {
  int64_t tile_var_sizes_num;

  // For all attributes, get the variable tile sizes
  for(int i=0; i<attribute_num; ++i) {
    // Get number of tile sizes
    CHECK_RC(bk_buffer->read_buffer(&tile_var_sizes_num, sizeof(int64_t)));
    std::cerr << "Tile var sizes for attribute=" << array_schema->attribute(i) << " tile_var_sizes_num="
          << tile_var_sizes_num << std::endl;
    if(tile_var_sizes_num == 0)
      continue;

    // Get variable tile sizes
    size_t tile_var_sizes[tile_var_sizes_num];
    CHECK_RC(bk_buffer->read_buffer(&tile_var_sizes[0], tile_var_sizes_num * sizeof(size_t)));
    for (auto j = 0; j<tile_var_sizes_num; j++) {
      std::cerr << tile_var_sizes[j] << " ";
    }
    std::cerr << std::endl;
  }

  // Success
  return TILEDB_OK;
}

int print_last_tile_cell_num() {
  // Get last tile cell number
  int64_t last_tile_cell_num;
  CHECK_RC(bk_buffer->read_buffer(&last_tile_cell_num, sizeof(int64_t)));
  std::cerr << "Last Tile cell num=" << last_tile_cell_num << std::endl;
  // Success
  return TILEDB_OK;
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    std::cerr << "No arguments specified\n";
    std::cerr << "Usage: test_print_book_keeping /path/to/TileDBArray/<fragment>\n";
    return 0;
  }
  
  std::string array_fragment(argv[1]); // from argv[1]

  // Get Array Schema
  std::string array_name = parent_dir(array_fragment);
  std::string array_schema_filename(array_name+'/'+TILEDB_ARRAY_SCHEMA_FILENAME);
  void *buffer;
  size_t buffer_size;
  CHECK_RC(TileDBUtils::read_entire_file(array_name+'/'+TILEDB_ARRAY_SCHEMA_FILENAME, &buffer, &buffer_size));
  array_schema = new ArraySchema(NULL);
  CHECK_RC(array_schema->deserialize(buffer, buffer_size));
  free(buffer);

  // Get Storage Filesystem
  StorageManagerConfig config;
  config.init(argv[1]);
  StorageFS* fs = config.get_filesystem();

  // Load Bookkeeping
  std::string book_keeping_filename = array_fragment+'/'+TILEDB_BOOK_KEEPING_FILENAME+TILEDB_FILE_SUFFIX+TILEDB_GZIP_SUFFIX;
  std::shared_ptr<BookKeeping> book_keeping =
      std::make_shared<BookKeeping>(array_schema,
                                    array_schema->dense(),
                                    array_fragment,
                                    TILEDB_ARRAY_READ);

  bk_buffer = std::make_shared<CompressedStorageBuffer>(fs, book_keeping_filename, 1024, true, TILEDB_GZIP, TILEDB_COMPRESSION_LEVEL_GZIP);

  // For easy reference
  attribute_num = array_schema->attribute_num();

  int rc = print_non_empty_domain();
  rc |= print_mbrs();
  rc |= print_bounding_coords();
  rc |= print_tile_offsets();
  rc |= print_tile_var_offsets();
  rc |= print_tile_var_sizes();
  rc |= print_last_tile_cell_num();

  if (!rc) {
    std::cerr << "Something went wrong!!" << std::endl;
  }
  
  delete array_schema;
  return rc;
}
  
