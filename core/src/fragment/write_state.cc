/**
 * @file   write_state.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 * @copyright Copyright (c) 2018-2022 Omics Data Automation, Inc.
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
 * This file implements the WriteState class.
 */

#include "comparators.h"
#include "storage_buffer.h"
#include "tiledb_constants.h"
#include "utils.h"
#include "write_state.h"
#include <cassert>
#include <cmath>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>


/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef TILEDB_VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_WS_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif

#if defined HAVE_OPENMP && defined USE_PARALLEL_SORT
  #include <parallel/algorithm>
  #define SORT(first, last, comp) __gnu_parallel::sort((first), (last), (comp))
#else
  #include <algorithm>
  #define SORT(first, last, comp) std::sort((first), (last), (comp))
#endif




/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_ws_errmsg = "";




/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

WriteState::WriteState(
    const Fragment* fragment, 
    BookKeeping* book_keeping)
    : book_keeping_(book_keeping),
      fragment_(fragment) {
  // For easy reference
  array_ = fragment_->array();
  array_schema_ = array_->array_schema();
  attribute_num_ = array_schema_->attribute_num();
  size_t coords_size = array_schema_->coords_size();

  // Initialize the number of cells written in the current tile
  tile_cell_num_.resize(attribute_num_+1);
  for(int i=0; i<attribute_num_+1; ++i)
    tile_cell_num_[i] = 0;

  // Initialize current tiles
  tiles_.resize(attribute_num_+1);
  for(int i=0; i<attribute_num_+1; ++i)
    tiles_[i] = NULL;

  // Initialize current variable tiles
  tiles_var_.resize(attribute_num_);
  for(int i=0; i<attribute_num_; ++i)
    tiles_var_[i] = NULL;

  // Initialize current tile offsets
  tile_offsets_.resize(attribute_num_+1);
  for(int i=0; i<attribute_num_+1; ++i)
    tile_offsets_[i] = 0;

  // Initialize current variable tile offsets
  tiles_var_offsets_.resize(attribute_num_);
  for(int i=0; i<attribute_num_; ++i)
    tiles_var_offsets_[i] = 0;

  // Initialize current variable tile sizes
  tiles_var_sizes_.resize(attribute_num_);
  for(int i=0; i<attribute_num_; ++i)
    tiles_var_sizes_[i] = 0;

  // Initialize the current size of the variable attribute file
  buffer_var_offsets_.resize(attribute_num_);
  for(int i=0; i<attribute_num_; ++i)
    buffer_var_offsets_[i] = 0;

  // Initialize current MBR
  mbr_ = malloc(2*coords_size);

  // Initialize current bounding coordinates
  bounding_coords_ = malloc(2*coords_size);

  fs_ = array_->config()->get_filesystem();
  
  init_file_buffers();

  // Intialize compression for tiles per attribute
  codec_.resize(attribute_num_+1);
  for(int i=0; i<attribute_num_+1; ++i) {
    codec_[i] = Codec::create(array_schema_, i);
  }
  offsets_codec_.resize(attribute_num_);
  for(int i=0; i<attribute_num_; ++i) {
    if (array_schema_->var_size(i)) {
      offsets_codec_[i] = Codec::create(array_schema_, i, true);
    } else {
      offsets_codec_[i] = NULL;
    }
  }
}

WriteState::~WriteState() {
  // Delete codec instances
  for(auto i=0u; i<codec_.size(); ++i) {
    if (codec_[i]) {
      delete codec_[i];
    }
  }
  for(auto i=0u; i<offsets_codec_.size(); ++i) {
    if (offsets_codec_[i]) {
      delete offsets_codec_[i];
    }
  }
    
  // Free current tiles
  int64_t tile_num = tiles_.size();
  for(int64_t i=0; i<tile_num; ++i) 
    if(tiles_[i] != NULL)
      free(tiles_[i]);

  // Free current tiles
  int64_t tile_var_num = tiles_var_.size();
  for(int64_t i=0; i<tile_var_num; ++i) 
    if(tiles_var_[i] != NULL)
      free(tiles_var_[i]);

  // Free current MBR
  if(mbr_ != NULL)
    free(mbr_);

  // Free current bounding coordinates
  if(bounding_coords_ != NULL)
    free(bounding_coords_);
}




/* ****************************** */
/*           MUTATORS             */
/* ****************************** */

int WriteState::finalize() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Write last tile (applicable only to the sparse case)
  if(tile_cell_num_[attribute_num] != 0) { 
    if(write_last_tile() != TILEDB_WS_OK)
      return TILEDB_WS_ERR;
    tile_cell_num_[attribute_num] = 0;
  }

  if (write_file_buffers() != TILEDB_WS_OK) {
    return TILEDB_WS_ERR;
  }

  // Sync all attributes 
  if(sync() != TILEDB_WS_OK) 
    return TILEDB_WS_ERR;

  // Success
  return TILEDB_WS_OK;
}

int WriteState::sync() {
  // No sync needed for storage classes derived from StorageCloudFS
  if (dynamic_cast<StorageCloudFS *>(fs_)) {
    return TILEDB_FS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
  int write_method = fragment_->array()->config()->write_method();
#ifdef HAVE_MPI
  MPI_Comm* mpi_comm = fragment_->array()->config()->mpi_comm();
#endif
  std::string filename;
  int rc;

  // Sync all attributes
  for(int i=0; i<(int)attribute_ids.size(); ++i) {
    // For all attributes
    filename = 
        fragment_->fragment_name() + "/" + 
        array_schema->attribute(attribute_ids[i]) + TILEDB_FILE_SUFFIX;
    if(write_method == TILEDB_IO_WRITE) {
      rc = ::sync_path(fs_, filename);
      // Handle error
      if(rc != TILEDB_UT_OK) {
        tiledb_ws_errmsg = tiledb_ut_errmsg;
        return TILEDB_WS_ERR;
      }
    } else if(write_method == TILEDB_IO_MPI) { 
#ifdef HAVE_MPI
      rc = mpi_io_sync(mpi_comm, filename.c_str());
      // Handle error
      if(rc != TILEDB_UT_OK) {
        tiledb_ws_errmsg = tiledb_ut_errmsg;
        return TILEDB_WS_ERR;
      }
#else
    // Error: MPI not supported
    std::string errmsg = "Cannot sync; MPI not supported";
    PRINT_ERROR(errmsg);
    tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
    return TILEDB_WS_ERR;
#endif
    } else {
      assert(0);
    }

    // Only for variable-size attributes (they have an extra file)
    if(array_schema->var_size(attribute_ids[i])) {
      filename = 
          fragment_->fragment_name() + "/" + 
          array_schema->attribute(attribute_ids[i]) + "_var" + 
          TILEDB_FILE_SUFFIX;
      if(write_method == TILEDB_IO_WRITE) {
        rc = ::sync_path(fs_, filename);
        // Handle error
        if(rc != TILEDB_UT_OK) {
          tiledb_ws_errmsg = tiledb_ut_errmsg;
          return TILEDB_WS_ERR;
        }
      } else if(write_method == TILEDB_IO_MPI) {
#ifdef HAVE_MPI
        rc = mpi_io_sync(mpi_comm, filename.c_str());
        // Handle error
        if(rc != TILEDB_UT_OK) {
          tiledb_ws_errmsg = tiledb_ut_errmsg;
          return TILEDB_WS_ERR;
        }
#else
        // Error: MPI not supported
        std::string errmsg = "Cannot sync; MPI not supported";
        PRINT_ERROR(errmsg);
        tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
        return TILEDB_WS_ERR;
#endif
      } else {
        assert(0);
      }
    }

    // Handle error
    if(rc != TILEDB_UT_OK) {
      tiledb_ws_errmsg = tiledb_ut_errmsg;
      return TILEDB_WS_ERR;
    }
  }

  // Sync fragment directory
  filename = fragment_->fragment_name();
  if(write_method == TILEDB_IO_WRITE) {
    rc = ::sync_path(fs_, filename);
  } else if(write_method == TILEDB_IO_MPI) {
#ifdef HAVE_MPI
    rc = mpi_io_sync(mpi_comm, filename.c_str());
#else
    // Error: MPI not supported
    std::string errmsg = "Cannot sync; MPI not supported";
    PRINT_ERROR(errmsg);
    tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
    return TILEDB_WS_ERR;
#endif
  } else {
    assert(0);
  }

  // Handle error
  if(rc != TILEDB_UT_OK) {
    tiledb_ws_errmsg = tiledb_ut_errmsg;
    return TILEDB_WS_ERR;
  }

  // Success
  return TILEDB_WS_OK;
}

int WriteState::sync_attribute(const std::string& attribute) {
  // No sync needed for storage classes derived from StorageCloudFS
  if (dynamic_cast<StorageCloudFS *>(fs_)) {
    return TILEDB_FS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int write_method = fragment_->array()->config()->write_method();
#ifdef HAVE_MPI
  MPI_Comm* mpi_comm = fragment_->array()->config()->mpi_comm();
#endif
  int attribute_id = array_schema->attribute_id(attribute); 
  std::string filename;
  int rc = TILEDB_OK;

  // Sync attribute
  filename = fragment_->fragment_name() + "/" + attribute + TILEDB_FILE_SUFFIX;
  if(write_method == TILEDB_IO_WRITE) {
    rc = ::sync_path(fs_, filename);
  } else if(write_method == TILEDB_IO_MPI) {
#ifdef HAVE_MPI
    rc = mpi_io_sync(mpi_comm, filename.c_str());
#else
    // Error: MPI not supported
    std::string errmsg = "Cannot sync attribute; MPI not supported";
    PRINT_ERROR(errmsg);
    tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
    return TILEDB_WS_ERR;
#endif
  } else {
    assert(0);
  }

  // Handle error
  if(rc != TILEDB_UT_OK) {
    tiledb_ws_errmsg = tiledb_ut_errmsg;
    return TILEDB_WS_ERR;
  }

  // Only for variable-size attributes (they have an extra file)
  if(array_schema->var_size(attribute_id)) {
    filename = 
        fragment_->fragment_name() + "/" + 
        attribute + "_var" + TILEDB_FILE_SUFFIX;
    if(write_method == TILEDB_IO_WRITE) {
      rc = ::sync_path(fs_, filename);
    } else if(write_method == TILEDB_IO_MPI) {
#ifdef HAVE_MPI
      rc = mpi_io_sync(mpi_comm, filename.c_str());
#else
    // Error: MPI not supported
    std::string errmsg = "Cannot sync attribute; MPI not supported";
    PRINT_ERROR(errmsg);
    tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
    return TILEDB_WS_ERR;
#endif
    } else {
      assert(0);
    }
  }

  // Handle error
  if(rc != TILEDB_UT_OK) {
    tiledb_ws_errmsg = tiledb_ut_errmsg;
    return TILEDB_WS_ERR;
  }

  // Sync fragment directory
  filename = fragment_->fragment_name();
  if(write_method == TILEDB_IO_WRITE) {
    rc = ::sync_path(fs_, filename);
  } else if(write_method == TILEDB_IO_MPI) {
#ifdef HAVE_MPI
    rc = mpi_io_sync(mpi_comm, filename.c_str());
#else
    // Error: MPI not supported
    std::string errmsg = "Cannot sync attribute; MPI not supported";
    PRINT_ERROR(errmsg);
    tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
    return TILEDB_WS_ERR;
#endif
  } else {
    assert(0);
  }

  // Handle error
  if(rc != TILEDB_UT_OK) {
    tiledb_ws_errmsg = tiledb_ut_errmsg;
    return TILEDB_WS_ERR;
  }

  // Success
  return TILEDB_WS_OK;
}

void WriteState::init_file_buffers() {
  file_buffer_.resize(attribute_num_+1);
  file_var_buffer_.resize(attribute_num_+1);

  for(int i=0; i<attribute_num_+1; ++i) {
    file_buffer_[i] = NULL;
    file_var_buffer_[i] = NULL;
  }
}

std::string WriteState::construct_filename(int attribute_id, bool is_var) {
  std::string filename;
  if (attribute_id == attribute_num_) {
    filename = fragment_->fragment_name() + "/" + TILEDB_COORDS + TILEDB_FILE_SUFFIX;
  } else {
    filename = fragment_->fragment_name() + "/" + array_schema_->attribute(attribute_id) + (is_var?"_var":"") +TILEDB_FILE_SUFFIX;
  }
  return filename;
}

int WriteState::write_file_buffers() {
  int rc = TILEDB_WS_OK;
  for(int i=0; i<attribute_num_+1; ++i) {
    if (file_buffer_[i] != NULL) {
      rc = file_buffer_[i]->finalize() || rc;
      delete file_buffer_[i];
      file_buffer_[i] = NULL;
    } else {
      rc = close_file(fs_, construct_filename(i, false)) || rc;
    }

    if (file_var_buffer_[i] != NULL) {
      rc = file_var_buffer_[i]->finalize() || rc;
      delete file_var_buffer_[i];
      file_var_buffer_[i] = NULL;
    } else {
      rc = close_file(fs_, construct_filename(i, true)) || rc;
    }

    if (rc) {
      std::string errmsg = "Could not finalize files from storage buffers for attribute " + fragment_->fragment_name() + construct_filename(i, false);
      PRINT_ERROR(errmsg);
      tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
      return TILEDB_WS_ERR;
    }
    
    // For variable length attributes, ensure an empty file exists even if there
    // are no valid values for querying.
    if(!rc && array_schema_->var_size(i) && is_file(fs_, construct_filename(i, false))) {
      std::string filename = construct_filename(i, true);
      if (!is_file(fs_, filename)) {
        rc = create_file(fs_, filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU) == TILEDB_UT_ERR;
        if (rc) {
          std::string errmsg = "Cannot create file " + filename;
          PRINT_ERROR(errmsg);
          tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
          rc = TILEDB_WS_ERR;
        }
      }
    }
  }

  return rc;
}

int WriteState::write_segment(int attribute_id, bool is_var, const void *segment, size_t length) {
  // Construct the attribute file name
  std::string filename = construct_filename(attribute_id, is_var);

  // Use buffers when desired, otherwise rely on HDFS or the Posix Filesystem to handle buffering
  if (fs_->get_upload_buffer_size() > 0) {
    StorageBuffer *file_buffer;
    if (is_var) {
      assert((attribute_id < attribute_num_) && "Coords attribute cannot be variable");
      if (file_var_buffer_[attribute_id] == NULL) {
        file_var_buffer_[attribute_id]= new StorageBuffer(fs_, filename, fs_->get_upload_buffer_size());
      }
      file_buffer = file_var_buffer_[attribute_id];
    } else {
      if (file_buffer_[attribute_id] == NULL) {
        file_buffer_[attribute_id] = new StorageBuffer(fs_, filename, fs_->get_upload_buffer_size());
      }
      file_buffer = file_buffer_[attribute_id];
    }
  
    // Buffered writing to help with distributed filesystem and cloud performance
    if (file_buffer != NULL) {
      if (file_buffer->append_buffer(segment, length) == TILEDB_BF_ERR) {
        std::string errmsg = "Cannot write attribute file " + filename + " to memory buffer. Will try write directly to file";
        PRINT_ERROR(errmsg);
        tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
      } else {
        return TILEDB_WS_OK;
      }
    }
  }

  // Write_segment directly
  int rc = TILEDB_WS_OK;
  int write_method = array_->config()->write_method();
  if(write_method == TILEDB_IO_WRITE) {
    rc = write_to_file(fs_, filename.c_str(), segment, length);
  } else if(write_method == TILEDB_IO_MPI) {
#ifdef HAVE_MPI
      rc = mpi_io_write_to_file(array_->config()->mpi_comm(), filename.c_str(), segment, length);
#else
    // Error: MPI not supported
    std::string errmsg = "Cannot write segment to file; MPI not supported";
    PRINT_ERROR(errmsg);
    tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
    return TILEDB_WS_ERR;
#endif
  }

  if (rc != TILEDB_UT_OK) {
    std::string errmsg = "Cannot write segment to file";
    PRINT_ERROR(errmsg);
    tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg + '\n' + tiledb_ut_errmsg;
    return TILEDB_WS_ERR;
  }

  return TILEDB_WS_OK;
}

int WriteState::write(const void** buffers, const size_t* buffer_sizes) {
  // Create fragment directory if it does not exist
  std::string fragment_name = fragment_->fragment_name();
  if(!is_dir(fs_, fragment_name)) {
    if(create_dir(fs_, fragment_name) != TILEDB_UT_OK) {
      tiledb_ws_errmsg = tiledb_ut_errmsg;
      return TILEDB_WS_ERR;
    }
  }

  // Dispatch the proper write command
  if(fragment_->mode() == TILEDB_ARRAY_WRITE ||
     fragment_->mode() == TILEDB_ARRAY_WRITE_SORTED_COL ||
     fragment_->mode() == TILEDB_ARRAY_WRITE_SORTED_ROW) {       // SORTED
    if(fragment_->dense())           // DENSE FRAGMENT
      return write_dense(buffers, buffer_sizes);          
    else                             // SPARSE FRAGMENT
      return write_sparse(buffers, buffer_sizes);
  } else if (fragment_->mode() == TILEDB_ARRAY_WRITE_UNSORTED) { // UNSORTED
    return write_sparse_unsorted(buffers, buffer_sizes);
  } else {
    std::string errmsg = "Cannot write to fragment; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
    return TILEDB_WS_ERR;
  } 
}




/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

int WriteState::compress_tile(
    int attribute_id,
    unsigned char* tile, 
    size_t tile_size,
    void** tile_compressed,
    size_t& tile_compressed_size,
    bool compress_offsets) {
  Codec* codec;
  if (compress_offsets) {
    codec = offsets_codec_[attribute_id];
    if (codec == NULL) {
      tile_compressed = reinterpret_cast<void **>(&tile);
      tile_compressed_size = tile_size;
      return TILEDB_WS_OK;
    }
  } else {
   codec = codec_[attribute_id];
  }
  if(codec->compress_tile(tile, tile_size, tile_compressed, tile_compressed_size) != TILEDB_CD_OK) {
    std::string errmsg = "Cannot compress tile for " + construct_filename(attribute_id, compress_offsets);
    PRINT_ERROR(errmsg);
    tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
    return TILEDB_WS_ERR;
  }
  return TILEDB_WS_OK;
}

int WriteState::compress_and_write_tile(int attribute_id) {
  // For easy reference
  unsigned char* tile = static_cast<unsigned char*>(tiles_[attribute_id]);
  size_t tile_size = tile_offsets_[attribute_id];

  // Trivial case - No in-memory tile
  if(tile_size == 0)
    return TILEDB_WS_OK;

  // Compress tile
  void *tile_compressed;
  size_t tile_compressed_size;
  if(compress_tile(
         attribute_id, 
         tile, 
         tile_size,
	 &tile_compressed,
         tile_compressed_size,
         array_schema_->var_size(attribute_id)) != TILEDB_WS_OK) 
    return TILEDB_WS_ERR;

  // Write segment to file
  int rc = write_segment(attribute_id, false, tile_compressed, tile_compressed_size);

  // Error
  if(rc != TILEDB_WS_OK) {
    return TILEDB_WS_ERR;
  }

  // Append offset to book-keeping
  book_keeping_->append_tile_offset(attribute_id, tile_compressed_size);

  // Success
  return TILEDB_WS_OK;
}

int WriteState::compress_and_write_tile_var(int attribute_id) {
  // For easy reference
  unsigned char* tile = static_cast<unsigned char*>(tiles_var_[attribute_id]);
  size_t tile_size = tiles_var_offsets_[attribute_id];

  // Trivial case - No in-memory tile
  if(tile_size == 0) {
    // Append offset to book-keeping
    book_keeping_->append_tile_var_offset(attribute_id, 0u);
    book_keeping_->append_tile_var_size(attribute_id, 0u);
    return TILEDB_WS_OK;
  }

  // Compress tile
  void *tile_compressed;
  size_t tile_compressed_size;
  if(compress_tile(
         attribute_id, 
         tile, 
         tile_size,
	 &tile_compressed,
         tile_compressed_size) != TILEDB_WS_OK) 
    return TILEDB_WS_ERR;

  // Write segment to file
  int rc = write_segment(attribute_id, true, tile_compressed, tile_compressed_size);

  // Error
  if(rc != TILEDB_WS_OK) {
    return TILEDB_WS_ERR;
  }

  // Append offset to book-keeping
  book_keeping_->append_tile_var_offset(attribute_id, tile_compressed_size);
  book_keeping_->append_tile_var_size(attribute_id, tile_size);

  // Success
  return TILEDB_WS_OK;
}

template<class T>
void WriteState::expand_mbr(const T* coords) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  T* mbr = static_cast<T*>(mbr_);

  // Initialize MBR 
  if(tile_cell_num_[attribute_num] == 0) {
    for(int i=0; i<dim_num; ++i) {
      mbr[2*i] = coords[i];
      mbr[2*i+1] = coords[i];
    }
  } else { // Expand MBR 
    ::expand_mbr(mbr, coords, dim_num);
  }
}

void WriteState::shift_var_offsets(
    int attribute_id,
    size_t buffer_var_size,
    const void* buffer,
    size_t buffer_size,
    void* shifted_buffer) {
  // For easy reference
  int64_t buffer_cell_num = buffer_size / sizeof(size_t);
  const size_t* buffer_s = 
      static_cast<const size_t*>(buffer);
  size_t* shifted_buffer_s = 
      static_cast<size_t*>(shifted_buffer);
  size_t& buffer_var_offset = buffer_var_offsets_[attribute_id];

  // Shift offsets
  for(int64_t i=0; i<buffer_cell_num; ++i) 
    shifted_buffer_s[i] = buffer_var_offset + buffer_s[i];

  // Update the last offset
  buffer_var_offset += buffer_var_size; 
}

void WriteState::sort_cell_pos(
    const void* buffer,
    size_t buffer_size,
    std::vector<int64_t>& cell_pos) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32)
    sort_cell_pos<int>(buffer, buffer_size, cell_pos);
  else if(coords_type == TILEDB_INT64)
    sort_cell_pos<int64_t>(buffer, buffer_size, cell_pos);
  else if(coords_type == TILEDB_FLOAT32)
    sort_cell_pos<float>(buffer, buffer_size, cell_pos);
  else if(coords_type == TILEDB_FLOAT64)
    sort_cell_pos<double>(buffer, buffer_size, cell_pos);
}

template<class T>
void WriteState::sort_cell_pos(
    const void* buffer,
    size_t buffer_size,
    std::vector<int64_t>& cell_pos) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  int64_t buffer_cell_num = buffer_size / coords_size;
  int cell_order = array_schema->cell_order();
  const T* buffer_T = static_cast<const T*>(buffer);

  // Populate cell_pos
  cell_pos.resize(buffer_cell_num);
  for(int i=0; i<buffer_cell_num; ++i)
    cell_pos[i] = i;

  // Invoke the proper sort function, based on the cell order
  if(array_schema->tile_extents() == NULL)  {    // NO TILE GRID
    if(cell_order == TILEDB_ROW_MAJOR) {
      // Sort cell positions
      SORT(
          cell_pos.begin(), 
          cell_pos.end(), 
          SmallerRow<T>(buffer_T, dim_num)); 
    } else if(cell_order == TILEDB_COL_MAJOR) {
      // Sort cell positions
      SORT(
          cell_pos.begin(), 
          cell_pos.end(), 
          SmallerCol<T>(buffer_T, dim_num)); 
    } else if(cell_order == TILEDB_HILBERT) {
      // Get hilbert ids
      std::vector<int64_t> ids;
      ids.resize(buffer_cell_num);
      for(int i=0; i<buffer_cell_num; ++i) 
        ids[i] = array_schema->hilbert_id<T>(&buffer_T[i * dim_num]); 
 
      // Sort cell positions
      SORT(
          cell_pos.begin(), 
          cell_pos.end(), 
          SmallerIdRow<T>(buffer_T, dim_num, ids)); 
    } else {
      assert(0); // The code should never reach here
    }
  } else {                                       // TILE GRID
    // Get tile ids
    std::vector<int64_t> ids;
    ids.resize(buffer_cell_num);
    for(int i=0; i<buffer_cell_num; ++i) 
      ids[i] = array_schema->tile_id<T>(&buffer_T[i * dim_num]); 
 
    // Sort cell positions
    if(cell_order == TILEDB_ROW_MAJOR) {
      SORT(
          cell_pos.begin(), 
          cell_pos.end(), 
          SmallerIdRow<T>(buffer_T, dim_num, ids)); 
    } else if(cell_order == TILEDB_COL_MAJOR) {
       SORT(
          cell_pos.begin(), 
          cell_pos.end(), 
          SmallerIdCol<T>(buffer_T, dim_num, ids));
    } else {
      assert(0); // The code should never reach here
    }
  }
}

void WriteState::update_book_keeping(
    const void* buffer,
    size_t buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32)
    update_book_keeping<int>(buffer, buffer_size);
  else if(coords_type == TILEDB_INT64)
    update_book_keeping<int64_t>(buffer, buffer_size);
  else if(coords_type == TILEDB_FLOAT32)
    update_book_keeping<float>(buffer, buffer_size);
  else if(coords_type == TILEDB_FLOAT64)
    update_book_keeping<double>(buffer, buffer_size);
}

template<class T>
void WriteState::update_book_keeping(
    const void* buffer,
    size_t buffer_size) {
  // Trivial case
  if(buffer_size == 0)
    return;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  int64_t capacity = array_schema->capacity();
  size_t coords_size = array_schema->coords_size();
  int64_t buffer_cell_num = buffer_size / coords_size;
  const T* buffer_T = static_cast<const T*>(buffer); 
  int64_t& tile_cell_num = tile_cell_num_[attribute_num];

  // Update bounding coordinates and MBRs
  for(int64_t i = 0; i<buffer_cell_num; ++i) {
    // Set first bounding coordinates
    if(tile_cell_num == 0) 
      memcpy(bounding_coords_, &buffer_T[i*dim_num], coords_size);

    // Set second bounding coordinates
    memcpy(
        static_cast<char*>(bounding_coords_) + coords_size,
        &buffer_T[i*dim_num], 
        coords_size);

    // Expand MBR
    expand_mbr(&buffer_T[i*dim_num]);

    // Advance a cell
    ++tile_cell_num;

    // Send MBR and bounding coordinates to book-keeping
    if(tile_cell_num == capacity) {
      book_keeping_->append_mbr(mbr_);
      book_keeping_->append_bounding_coords(bounding_coords_);
      tile_cell_num = 0; 
    }
  }
}

int WriteState::write_last_tile() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Send last MBR, bounding coordinates and tile cell number to book-keeping
  book_keeping_->append_mbr(mbr_);
  book_keeping_->append_bounding_coords(bounding_coords_);
  book_keeping_->set_last_tile_cell_num(tile_cell_num_[attribute_num]);

  // Flush the last tile for each compressed attribute (it is still in main
  // memory
  for(int i=0; i<attribute_num+1; ++i) {
    if(array_schema->compression(i) != TILEDB_NO_COMPRESSION) {
      if(compress_and_write_tile(i) != TILEDB_WS_OK)
        return TILEDB_WS_ERR;
      if(array_schema->var_size(i)) {
        if(compress_and_write_tile_var(i) != TILEDB_WS_OK)
          return TILEDB_WS_ERR;
      }
    }
  } 

  // Success
  return TILEDB_WS_OK;
}

int WriteState::write_dense(
    const void** buffers,
    const size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 

  // Write each attribute individually
  int buffer_i = 0;
  for(int i=0; i<attribute_id_num; ++i) {
    if(!array_schema->var_size(attribute_ids[i])) { // FIXED CELLS
      if(write_dense_attr(
             attribute_ids[i], 
             buffers[buffer_i], 
             buffer_sizes[buffer_i]) != TILEDB_WS_OK)
        return TILEDB_WS_ERR;
      ++buffer_i;
    } else {                                        // VARIABLE-SIZED CELLS
      if(write_dense_attr_var(
             attribute_ids[i], 
             buffers[buffer_i],       // offsets 
             buffer_sizes[buffer_i],
             buffers[buffer_i+1],     // actual cell values
             buffer_sizes[buffer_i+1]) != TILEDB_WS_OK)
        return TILEDB_WS_ERR;
      buffer_i += 2;
    }
  }

  // Success
  return TILEDB_WS_OK;
}

int WriteState::write_dense_attr(
    int attribute_id,
    const void* buffer,
    size_t buffer_size) {
  // Trivial case
  if(buffer_size == 0)
    return TILEDB_WS_OK;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int compression = array_schema->compression(attribute_id);

  // No compression
  if(compression == TILEDB_NO_COMPRESSION)
    return write_dense_attr_cmp_none(attribute_id, buffer, buffer_size);
  else // All compressions 
    return write_dense_attr_cmp(attribute_id, buffer, buffer_size);
}

int WriteState::write_dense_attr_cmp_none(
    int attribute_id,
    const void* buffer,
    size_t buffer_size) {
  // Write buffer to file
  return write_segment(attribute_id, false, buffer, buffer_size);
}

int WriteState::write_dense_attr_cmp(
    int attribute_id,
    const void* buffer,
    size_t buffer_size) {
  // For easy reference
  size_t tile_size = fragment_->tile_size(attribute_id); 

  // Initialize local tile buffer if needed
  if(tiles_[attribute_id] == NULL)
    tiles_[attribute_id] = malloc(tile_size);

  // For easy reference
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  size_t& tile_offset = tile_offsets_[attribute_id];
  const char* buffer_c = static_cast<const char*>(buffer);
  size_t buffer_offset = 0;

  // Bytes to fill the potentially partially buffered tile
  size_t bytes_to_fill = tile_size - tile_offset;

  // The buffer has enough cells to fill at least one tile
  if(bytes_to_fill <= buffer_size) {
    // Fill up current tile
    memcpy(
        tile + tile_offset, 
        buffer_c + buffer_offset,
        bytes_to_fill); 
    buffer_offset += bytes_to_fill;
    tile_offset += bytes_to_fill;

    // Compress current tile and write it to disk
    if(compress_and_write_tile(attribute_id) != TILEDB_WS_OK)
      return TILEDB_WS_ERR;

    // Update local tile buffer offset
    tile_offset = 0;
  }
      
  // Continue to fill and compress entire tiles
  while(buffer_offset + tile_size <= buffer_size) {
    // Prepare tile
    memcpy(tile, buffer_c + buffer_offset, tile_size); 
    buffer_offset += tile_size;
    tile_offset += tile_size;

    // Compress and write current tile
    if(compress_and_write_tile(attribute_id) != TILEDB_WS_OK)
      return TILEDB_WS_ERR;

    // Update local tile buffer offset
    tile_offset = 0;
  }

  // Partially fill the (new) current tile
  bytes_to_fill = buffer_size - buffer_offset;
  if(bytes_to_fill != 0) {
    memcpy(tile + tile_offset, buffer_c + buffer_offset, bytes_to_fill);
    buffer_offset += bytes_to_fill;
    assert(buffer_offset == buffer_size);
    tile_offset += bytes_to_fill;
  }

  // Success
  return TILEDB_WS_OK;
}

int WriteState::write_dense_attr_var(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size) {
  // Trivial case
  if(buffer_size == 0)
    return TILEDB_WS_OK;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int compression = array_schema->compression(attribute_id);

  // No compression
  if(compression == TILEDB_NO_COMPRESSION)
    return write_dense_attr_var_cmp_none(
               attribute_id, 
               buffer,  
               buffer_size,
               buffer_var,  
               buffer_var_size);
  else // All compressions 
    return write_dense_attr_var_cmp(
               attribute_id, 
               buffer,  
               buffer_size,
               buffer_var,  
               buffer_var_size);

  // Sanity check
  assert(0);

  // Error
  return TILEDB_WS_ERR;
}

int WriteState::write_dense_attr_var_cmp_none(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size) {
  int rc = write_segment(attribute_id, true, buffer_var, buffer_var_size);

  // Error
  if(rc != TILEDB_WS_OK) {
    return TILEDB_WS_ERR;
  }

  // Recalculate offsets
  void* shifted_buffer = malloc(buffer_size);
  shift_var_offsets(
      attribute_id,
      buffer_var_size,
      buffer, 
      buffer_size,
      shifted_buffer);

  rc = write_segment(attribute_id, false, shifted_buffer, buffer_size);

  // Clean up
  free(shifted_buffer);

  // Error
  if(rc != TILEDB_WS_OK) {
    return TILEDB_WS_ERR;
  }

  // Success
  return TILEDB_WS_OK;
}

int WriteState::write_dense_attr_var_cmp(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size) {
  // For easy reference
  size_t cell_size = TILEDB_CELL_VAR_OFFSET_SIZE; 
  int64_t cell_num_per_tile = fragment_->cell_num_per_tile();
  size_t tile_size = cell_num_per_tile * cell_size; 

  // Initialize local tile buffer if needed
  if(tiles_[attribute_id] == NULL)
    tiles_[attribute_id] = malloc(tile_size);

  // Initialize local variable tile buffer if needed
  if(tiles_var_[attribute_id] == NULL) {
    tiles_var_[attribute_id] = malloc(tile_size);
    tiles_var_sizes_[attribute_id] = tile_size;
  }

  // Recalculate offsets
  void* shifted_buffer = malloc(buffer_size);
  shift_var_offsets(
      attribute_id,
      buffer_var_size,
      buffer, 
      buffer_size,
      shifted_buffer);

  // For easy reference
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  size_t& tile_offset = tile_offsets_[attribute_id];
  const size_t* buffer_s = static_cast<const size_t*>(buffer);
  const char* shifted_buffer_c = static_cast<const char*>(shifted_buffer);
  size_t buffer_offset = 0;
  char* tile_var = static_cast<char*>(tiles_var_[attribute_id]);
  size_t& tile_var_offset = tiles_var_offsets_[attribute_id];
  const char* buffer_var_c = static_cast<const char*>(buffer_var);
  size_t buffer_var_offset = 0;

  // Update total number of cells
  int64_t buffer_cell_num = buffer_size / cell_size;

  // Bytes to fill the potentially partially buffered tile
  size_t bytes_to_fill = tile_size - tile_offset;
  int64_t cell_num_to_fill = bytes_to_fill / cell_size;
  int64_t end_cell_pos = cell_num_to_fill;
  size_t bytes_to_fill_var = 
      (end_cell_pos == buffer_cell_num) ? buffer_var_size : 
                                          buffer_s[end_cell_pos]; 

  // The buffer has enough cells to fill at least one tile
  if(bytes_to_fill <= buffer_size) {
    // Fill up current tile
    memcpy(
        tile + tile_offset, 
        shifted_buffer_c + buffer_offset,
        bytes_to_fill); 
    buffer_offset += bytes_to_fill;
    tile_offset += bytes_to_fill;

    // Compress current tile and write it to disk
    if(compress_and_write_tile(attribute_id) != TILEDB_WS_OK) {
      free(shifted_buffer);
      return TILEDB_WS_ERR;
    }

    // Update local tile buffer offset
    tile_offset = 0;

    // Potentially expand the variable tile buffer
    if(tile_var_offset + bytes_to_fill_var >  
          tiles_var_sizes_[attribute_id]) {
      tiles_var_sizes_[attribute_id] = tile_var_offset + bytes_to_fill_var;
      tiles_var_[attribute_id] = 
          realloc(tiles_var_[attribute_id], tiles_var_sizes_[attribute_id]);
      // Re-allocation may assign tiles_var_ to a different region of memory
      tile_var = static_cast<char*>(tiles_var_[attribute_id]);
    }

    // Fill up current variable tile
    memcpy(
        tile_var + tile_var_offset, 
        buffer_var_c + buffer_var_offset,
        bytes_to_fill_var); 
    buffer_var_offset += bytes_to_fill_var;
    tile_var_offset += bytes_to_fill_var;

    // Compress current tile and write it to disk
    if(compress_and_write_tile_var(attribute_id) != TILEDB_WS_OK) {
      free(shifted_buffer);
      return TILEDB_WS_ERR;
    }
    // Update local tile buffer offset
    tile_var_offset = 0;   
  }
      
  // Continue to fill and compress entire tiles
  while(buffer_offset + tile_size <= buffer_size) {
    // Prepare tile
    memcpy(tile, shifted_buffer_c + buffer_offset, tile_size); 
    buffer_offset += tile_size;
    tile_offset += tile_size;

    // Compress and write current tile
    if(compress_and_write_tile(attribute_id) != TILEDB_WS_OK) {
      free(shifted_buffer);
      return TILEDB_WS_ERR;
    }

    // Update local tile buffer offset
    tile_offset = 0;

    // Calculate the number of bytes to fill for the variable tile
    bytes_to_fill_var = 
        (end_cell_pos + cell_num_per_tile == buffer_cell_num) 
             ? buffer_var_size - buffer_var_offset
             : buffer_s[end_cell_pos + cell_num_per_tile] - 
               buffer_s[end_cell_pos]; 
     end_cell_pos += cell_num_per_tile;

    // Potentially expand the variable tile buffer
    if(tile_var_offset + bytes_to_fill_var >  
          tiles_var_sizes_[attribute_id]) {
      tiles_var_sizes_[attribute_id] = tile_var_offset + bytes_to_fill_var;
      tiles_var_[attribute_id] = 
          realloc(tiles_var_[attribute_id], tiles_var_sizes_[attribute_id]);
      // Re-allocation may assign tiles_var_ to a different region of memory
      tile_var = static_cast<char*>(tiles_var_[attribute_id]);
    }

    // Fill up current variable tile
    memcpy(
        tile_var + tile_var_offset, 
        buffer_var_c + buffer_var_offset,
        bytes_to_fill_var); 
    buffer_var_offset += bytes_to_fill_var;
    tile_var_offset += bytes_to_fill_var;

    // Compress current tile and write it to disk
    if(compress_and_write_tile_var(attribute_id) != TILEDB_WS_OK) {
      free(shifted_buffer);
      return TILEDB_WS_ERR;
    }

    // Update local tile buffer offset
    tile_var_offset = 0;
  }

  // Partially fill the (new) current tile
  bytes_to_fill = buffer_size - buffer_offset;
  if(bytes_to_fill != 0) {
    memcpy(tile + tile_offset, shifted_buffer_c + buffer_offset, bytes_to_fill);
    buffer_offset += bytes_to_fill;
    assert(buffer_offset == buffer_size);
    tile_offset += bytes_to_fill;

    // Calculate the number of bytes to fill for the variable tile
    bytes_to_fill_var = buffer_var_size - buffer_var_offset;

    // Potentially expand the variable tile buffer
    if(tile_var_offset + bytes_to_fill_var >  
       tiles_var_sizes_[attribute_id]) {
      tiles_var_sizes_[attribute_id] = tile_var_offset + bytes_to_fill_var;
      tiles_var_[attribute_id] = 
          realloc(tiles_var_[attribute_id], tiles_var_sizes_[attribute_id]);
      // Re-allocation may assign tiles_var_ to a different region of memory
      tile_var = static_cast<char*>(tiles_var_[attribute_id]);
    }

    // Fill up current variable tile
    memcpy(
        tile_var + tile_var_offset, 
        buffer_var_c + buffer_var_offset,
        bytes_to_fill_var); 
    buffer_var_offset += bytes_to_fill_var;
    assert(buffer_var_offset == buffer_var_size);
    tile_var_offset += bytes_to_fill_var;
  }

  // Clean up
  free(shifted_buffer);

  // Success
  return TILEDB_WS_OK;
}

int WriteState::write_sparse(
    const void** buffers,
    const size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 

  // Write each attribute individually
  int buffer_i = 0;
  for(int i=0; i<attribute_id_num; ++i) {
    if(!array_schema->var_size(attribute_ids[i])) { // FIXED CELLS
      if(write_sparse_attr(
             attribute_ids[i], 
             buffers[buffer_i], 
             buffer_sizes[buffer_i]) != TILEDB_WS_OK)
        return TILEDB_WS_ERR;
      ++buffer_i;
    } else {                                        // VARIABLE-SIZED CELLS
      if(write_sparse_attr_var(
             attribute_ids[i], 
             buffers[buffer_i],       // offsets 
             buffer_sizes[buffer_i],
             buffers[buffer_i+1],     // actual cell values
             buffer_sizes[buffer_i+1]) != TILEDB_WS_OK)
        return TILEDB_WS_ERR;
      buffer_i += 2;
    }
  }

  // Success
  return TILEDB_WS_OK;
}

int WriteState::write_sparse_attr(
    int attribute_id,
    const void* buffer,
    size_t buffer_size) {
  // Trivial case
  if(buffer_size == 0)
    return TILEDB_WS_OK;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int compression = array_schema->compression(attribute_id);

  // No compression
  if(compression == TILEDB_NO_COMPRESSION)
    return write_sparse_attr_cmp_none(attribute_id, buffer, buffer_size);
  else // All compressions
    return write_sparse_attr_cmp(attribute_id, buffer, buffer_size);
}

int WriteState::write_sparse_attr_cmp_none(
    int attribute_id,
    const void* buffer,
    size_t buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Update book-keeping
  if(attribute_id == attribute_num) 
    update_book_keeping(buffer, buffer_size);

  // Write buffer to file 
  int rc = write_segment(attribute_id, false, buffer, buffer_size);

  // Error
  if(rc != TILEDB_UT_OK) {
    return TILEDB_WS_ERR;
  }

  // Success
  return TILEDB_WS_OK;
}

int WriteState::write_sparse_attr_cmp(
    int attribute_id,
    const void* buffer,
    size_t buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  size_t tile_size = fragment_->tile_size(attribute_id); 

  // Update book-keeping
  if(attribute_id == attribute_num) 
    update_book_keeping(buffer, buffer_size);

  // Initialize local tile buffer if needed
  if(tiles_[attribute_id] == NULL)
    tiles_[attribute_id] = malloc(tile_size);

  // For easy reference
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  size_t& tile_offset = tile_offsets_[attribute_id];
  const char* buffer_c = static_cast<const char*>(buffer);
  size_t buffer_offset = 0;

  // Bytes to fill the potentially partially buffered tile
  size_t bytes_to_fill = tile_size - tile_offset;

  // The buffer has enough cells to fill at least one tile
  if(bytes_to_fill <= buffer_size) {
    // Fill up current tile, and append offset to book-keeping
    memcpy(
        tile + tile_offset, 
        buffer_c + buffer_offset,
        bytes_to_fill); 
    buffer_offset += bytes_to_fill;
    tile_offset += bytes_to_fill;

    // Compress current tile and write it to disk
    if(compress_and_write_tile(attribute_id) != TILEDB_WS_OK)
      return TILEDB_WS_ERR;

    // Update local tile buffer offset
    tile_offset = 0;
  }
      
  // Continue to fill and compress entire tiles
  while(buffer_offset + tile_size <= buffer_size) {
    // Prepare tile
    memcpy(tile, buffer_c + buffer_offset, tile_size); 
    buffer_offset += tile_size;
    tile_offset += tile_size;

    // Compress current tile, append to segment.
    if(compress_and_write_tile(attribute_id) != TILEDB_WS_OK)
      return TILEDB_WS_ERR;

    // Update local tile buffer offset
    tile_offset = 0;
  }

  // Partially fill the (new) current tile
  bytes_to_fill = buffer_size - buffer_offset;
  if(bytes_to_fill != 0) {
    memcpy(tile + tile_offset, buffer_c + buffer_offset, bytes_to_fill);
    buffer_offset += bytes_to_fill;
    assert(buffer_offset == buffer_size);
    tile_offset += bytes_to_fill;
  }
 
  // Success
  return TILEDB_WS_OK;
}

int WriteState::write_sparse_attr_var(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size) {
  // Trivial case
  if(buffer_size == 0)
    return TILEDB_WS_OK;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int compression = array_schema->compression(attribute_id);

  // No compression
  if(compression == TILEDB_NO_COMPRESSION)
    return write_sparse_attr_var_cmp_none(
               attribute_id, 
               buffer,  
               buffer_size,
               buffer_var,  
               buffer_var_size);
  else //  All compressions
    return write_sparse_attr_var_cmp(
               attribute_id, 
               buffer,  
               buffer_size,
               buffer_var,  
               buffer_var_size);
}

int WriteState::write_sparse_attr_var_cmp_none(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();

  // Update book-keeping
  assert(attribute_id != array_schema->attribute_num());

  // Write buffer with variable-sized cells to disk
  int rc = write_segment(attribute_id, true, buffer_var, buffer_var_size);

  // Error
  if(rc != TILEDB_WS_OK) {
    return TILEDB_WS_ERR;
  }

  // Recalculate offsets
  void* shifted_buffer = malloc(buffer_size);
  shift_var_offsets(
      attribute_id,
      buffer_var_size,
      buffer, 
      buffer_size,
      shifted_buffer);

  rc = write_segment(attribute_id, false, shifted_buffer, buffer_size);

  // Clean up
  free(shifted_buffer);

  // Return
  if(rc != TILEDB_WS_OK) {
    return TILEDB_WS_ERR;
  }

  // Success
  return TILEDB_WS_OK;
}

int WriteState::write_sparse_attr_var_cmp(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = TILEDB_CELL_VAR_OFFSET_SIZE;
  int64_t cell_num_per_tile = array_schema->capacity();
  size_t tile_size = fragment_->tile_size(attribute_id); 

  // Sanity check (coordinates are always fixed-sized)
  assert(attribute_id != array_schema->attribute_num());

  // Initialize local tile buffer if needed
  if(tiles_[attribute_id] == NULL)
    tiles_[attribute_id] = malloc(tile_size);

  // Initialize local variable tile buffer if needed
  if(tiles_var_[attribute_id] == NULL) {
    tiles_var_[attribute_id] = malloc(tile_size);
    tiles_var_sizes_[attribute_id] = tile_size;
  }

  // Recalculate offsets
  void* shifted_buffer = malloc(buffer_size);
  shift_var_offsets(
      attribute_id,
      buffer_var_size,
      buffer, 
      buffer_size,
      shifted_buffer);

  // For easy reference
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  size_t& tile_offset = tile_offsets_[attribute_id];
  const size_t* buffer_s = static_cast<const size_t*>(buffer);
  const char* shifted_buffer_c = static_cast<const char*>(shifted_buffer);
  size_t buffer_offset = 0;
  char* tile_var = static_cast<char*>(tiles_var_[attribute_id]);
  size_t& tile_var_offset = tiles_var_offsets_[attribute_id];
  const char* buffer_var_c = static_cast<const char*>(buffer_var);
  size_t buffer_var_offset = 0;

  // Update total number of cells
  int64_t buffer_cell_num = buffer_size / cell_size;

  // Bytes to fill the potentially partially buffered tile
  size_t bytes_to_fill = tile_size - tile_offset;
  int64_t cell_num_to_fill = bytes_to_fill / cell_size;
  int64_t end_cell_pos = cell_num_to_fill;
  size_t bytes_to_fill_var = 
      (end_cell_pos == buffer_cell_num) ? buffer_var_size : 
                                          buffer_s[end_cell_pos]; 

  // The buffer has enough cells to fill at least one tile
  if(bytes_to_fill <= buffer_size) {
    // Fill up current tile
    memcpy(
        tile + tile_offset, 
        shifted_buffer_c + buffer_offset,
        bytes_to_fill); 
    buffer_offset += bytes_to_fill;
    tile_offset += bytes_to_fill;

    // Compress current tile and write it to disk
    if(compress_and_write_tile(attribute_id) != TILEDB_WS_OK) {
      free(shifted_buffer);
      return TILEDB_WS_ERR;
    }

    // Update local tile buffer offset
    tile_offset = 0;

    // Potentially expand the variable tile buffer
    while(tile_var_offset + bytes_to_fill_var >  
          tiles_var_sizes_[attribute_id])
      expand_buffer(tiles_var_[attribute_id], tiles_var_sizes_[attribute_id]);
    // Re-allocation may assign tiles_var_ to a different region of memory
    tile_var = static_cast<char*>(tiles_var_[attribute_id]);

    // Fill up current variable tile
    memcpy(
        tile_var + tile_var_offset, 
        buffer_var_c + buffer_var_offset,
        bytes_to_fill_var); 
    buffer_var_offset += bytes_to_fill_var;
    tile_var_offset += bytes_to_fill_var;

    // Compress current tile and write it to disk
    if(compress_and_write_tile_var(attribute_id) != TILEDB_WS_OK) {
      free(shifted_buffer);
      return TILEDB_WS_ERR;
    }

    // Update local tile buffer offset
    tile_var_offset = 0;   
  }
      
  // Continue to fill and compress entire tiles
  while(buffer_offset + tile_size <= buffer_size) {
    // Prepare tile
    memcpy(tile, shifted_buffer_c + buffer_offset, tile_size); 
    buffer_offset += tile_size;
    tile_offset += tile_size;

    // Compress and write current tile
    if(compress_and_write_tile(attribute_id) != TILEDB_WS_OK) {
      free(shifted_buffer);
      return TILEDB_WS_ERR;
    }

    // Update local tile buffer offset
    tile_offset = 0;

    // Calculate the number of bytes to fill for the variable tile
    bytes_to_fill_var = 
        (end_cell_pos + cell_num_per_tile == buffer_cell_num) 
             ? buffer_var_size - buffer_var_offset
             : buffer_s[end_cell_pos + cell_num_per_tile] - 
               buffer_s[end_cell_pos]; 
     end_cell_pos += cell_num_per_tile;

    // Potentially expand the variable tile buffer
    if(tile_var_offset + bytes_to_fill_var >  
          tiles_var_sizes_[attribute_id]) {
      tiles_var_sizes_[attribute_id] = tile_var_offset + bytes_to_fill_var;
      tiles_var_[attribute_id] = 
          realloc(tiles_var_[attribute_id], tiles_var_sizes_[attribute_id]);
      // Re-allocation may assign tiles_var_ to a different region of memory
      tile_var = static_cast<char*>(tiles_var_[attribute_id]);
    }

    // Fill up current variable tile
    memcpy(
        tile_var + tile_var_offset, 
        buffer_var_c + buffer_var_offset,
        bytes_to_fill_var); 
    buffer_var_offset += bytes_to_fill_var;
    tile_var_offset += bytes_to_fill_var;

    // Compress current tile and write it to disk
    if(compress_and_write_tile_var(attribute_id) != TILEDB_WS_OK) {
      free(shifted_buffer);
      return TILEDB_WS_ERR;
    }

    // Update local tile buffer offset
    tile_var_offset = 0;
  }

  // Partially fill the (new) current tile
  bytes_to_fill = buffer_size - buffer_offset;
  if(bytes_to_fill != 0) {
    memcpy(tile + tile_offset, shifted_buffer_c + buffer_offset, bytes_to_fill);
    buffer_offset += bytes_to_fill;
    assert(buffer_offset == buffer_size);
    tile_offset += bytes_to_fill;

    // Calculate the number of bytes to fill for the variable tile
    bytes_to_fill_var = buffer_var_size - buffer_var_offset;

    // Potentially expand the variable tile buffer
    if(tile_var_offset + bytes_to_fill_var >  
          tiles_var_sizes_[attribute_id]) {
      tiles_var_sizes_[attribute_id] = tile_var_offset + bytes_to_fill_var;
      tiles_var_[attribute_id] = 
          realloc(tiles_var_[attribute_id], tiles_var_sizes_[attribute_id]);
      // Re-allocation may assign tiles_var_ to a different region of memory
      tile_var = static_cast<char*>(tiles_var_[attribute_id]);
    }

    // Fill up current variable tile
    memcpy(
        tile_var + tile_var_offset, 
        buffer_var_c + buffer_var_offset,
        bytes_to_fill_var); 
    buffer_var_offset += bytes_to_fill_var;
    assert(buffer_var_offset == buffer_var_size);
    tile_var_offset += bytes_to_fill_var;
  }

  // Clean up
  free(shifted_buffer);

  // Success
  return TILEDB_WS_OK;
}

int WriteState::write_sparse_unsorted(
    const void** buffers,
    const size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 

  // Find the coordinates buffer
  int coords_buffer_i = -1;
  int buffer_i = 0;
  for(int i=0; i<attribute_id_num; ++i) { 
    if(attribute_ids[i] == attribute_num) {
      coords_buffer_i = buffer_i;
      break;
    }
    if(!array_schema->var_size(attribute_ids[i])) // FIXED CELLS
      ++buffer_i;
    else                                          // VARIABLE-SIZED CELLS
      buffer_i +=2;
  }

  // Coordinates are missing
  if(coords_buffer_i == -1) {
    std::string errmsg = "Cannot write sparse unsorted; Coordinates missing";
    PRINT_ERROR(errmsg);
    tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
    return TILEDB_WS_ERR;
  }

  // Sort cell positions
  std::vector<int64_t> cell_pos;
  sort_cell_pos(
      buffers[coords_buffer_i], 
      buffer_sizes[coords_buffer_i], 
      cell_pos);

  // Write each attribute individually
  buffer_i=0; 
  for(int i=0; i<attribute_id_num; ++i) {
    if(!array_schema->var_size(attribute_ids[i])) { // FIXED CELLS
      if(write_sparse_unsorted_attr(
             attribute_ids[i], 
             buffers[buffer_i], 
             buffer_sizes[buffer_i],
             cell_pos) != TILEDB_WS_OK)
        return TILEDB_WS_ERR;
      ++buffer_i;
    } else {                                        // VARIABLE-SIZED CELLS
      if(write_sparse_unsorted_attr_var(
             attribute_ids[i], 
             buffers[buffer_i],       // offsets 
             buffer_sizes[buffer_i],
             buffers[buffer_i+1],     // actual values
             buffer_sizes[buffer_i+1],
             cell_pos) != TILEDB_WS_OK)
        return TILEDB_WS_ERR;
      buffer_i += 2;
    }
  }

  // Success
  return TILEDB_WS_OK;
}

int WriteState::write_sparse_unsorted_attr(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int compression = array_schema->compression(attribute_id);

  // No compression
  if(compression == TILEDB_NO_COMPRESSION)
    return write_sparse_unsorted_attr_cmp_none(
               attribute_id, 
               buffer, 
               buffer_size,
               cell_pos);
  else // All compressions
    return write_sparse_unsorted_attr_cmp(
               attribute_id,
               buffer, 
               buffer_size,
               cell_pos);
}

int WriteState::write_sparse_unsorted_attr_cmp_none(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = array_schema->cell_size(attribute_id); 
  const char* buffer_c = static_cast<const char*>(buffer); 

  // Check number of cells in buffer
  int64_t buffer_cell_num = buffer_size / cell_size;
  if(buffer_cell_num != int64_t(cell_pos.size())) {
    std::string errmsg = 
        std::string("Cannot write sparse unsorted; Invalid number of "
        "cells in attribute '") + 
        array_schema->attribute(attribute_id) + "'";
    PRINT_ERROR(errmsg);
    tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
    return TILEDB_WS_ERR;
  }

  // Allocate a local buffer to hold the sorted cells
  char* sorted_buffer = new char[TILEDB_SORTED_BUFFER_SIZE]; 
  size_t sorted_buffer_size = 0;

  // Sort and write attribute values in batches
  for(int64_t i=0; i<buffer_cell_num; ++i) {
    // Write batch
    if(sorted_buffer_size + cell_size > TILEDB_SORTED_BUFFER_SIZE) {
      if(write_sparse_attr_cmp_none(
             attribute_id,
             sorted_buffer, 
             sorted_buffer_size) != TILEDB_WS_OK) {
        delete [] sorted_buffer;
        return TILEDB_WS_ERR;
      } else {
        sorted_buffer_size = 0;
      }
    }

    // Keep on copying the cells in the sorted order in the sorted buffer
    memcpy(
        sorted_buffer + sorted_buffer_size, 
        buffer_c + cell_pos[i] * cell_size, 
        cell_size);
    sorted_buffer_size += cell_size;
  }

  // Write final batch
  if(sorted_buffer_size != 0) {
    if(write_sparse_attr_cmp_none(
           attribute_id, 
           sorted_buffer, 
           sorted_buffer_size) != TILEDB_WS_OK) {
      delete [] sorted_buffer;
      return TILEDB_WS_ERR;
    }
  }

  // Clean up
  delete [] sorted_buffer;

  // Success
  return TILEDB_WS_OK;
}

int WriteState::write_sparse_unsorted_attr_cmp(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = array_schema->cell_size(attribute_id); 
  const char* buffer_c = static_cast<const char*>(buffer); 

  // Check number of cells in buffer
  int64_t buffer_cell_num = buffer_size / cell_size;
  if(buffer_cell_num != int64_t(cell_pos.size())) {
    std::string errmsg = 
        std::string("Cannot write sparse unsorted; Invalid number of "
        "cells in attribute '") + 
        array_schema->attribute(attribute_id) + "'";
    PRINT_ERROR(errmsg);
    tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
    return TILEDB_WS_ERR;
  }

  // Allocate a local buffer to hold the sorted cells
  char* sorted_buffer = new char[TILEDB_SORTED_BUFFER_SIZE]; 
  size_t sorted_buffer_size = 0;

  // Sort and write attribute values in batches
  for(int64_t i=0; i<buffer_cell_num; ++i) {
    // Write batch
    if(sorted_buffer_size + cell_size > TILEDB_SORTED_BUFFER_SIZE) {
      if(write_sparse_attr_cmp(
             attribute_id,
             sorted_buffer, 
             sorted_buffer_size) != TILEDB_WS_OK) {
        delete [] sorted_buffer;
        return TILEDB_WS_ERR;
      } else {
        sorted_buffer_size = 0;
      }
    }

    // Keep on copying the cells in the sorted order in the sorted buffer
    memcpy(
        sorted_buffer + sorted_buffer_size, 
        buffer_c + cell_pos[i] * cell_size, 
        cell_size);
    sorted_buffer_size += cell_size;
  }

  // Write final batch
  if(sorted_buffer_size != 0) {
    if(write_sparse_attr_cmp(
           attribute_id, 
           sorted_buffer, 
           sorted_buffer_size) != TILEDB_WS_OK) {
      delete [] sorted_buffer;
      return TILEDB_WS_ERR;
    }
  }

  // Clean up
  delete [] sorted_buffer;

  // Success
  return TILEDB_WS_OK;
} 

int WriteState::write_sparse_unsorted_attr_var(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int compression = array_schema->compression(attribute_id);

  // No compression
  if(compression == TILEDB_NO_COMPRESSION)
    return write_sparse_unsorted_attr_var_cmp_none(
               attribute_id, 
               buffer,
               buffer_size,
               buffer_var, 
               buffer_var_size,
               cell_pos);
  else // All compressions
    return write_sparse_unsorted_attr_var_cmp(
               attribute_id, 
               buffer,
               buffer_size,
               buffer_var, 
               buffer_var_size,
               cell_pos);
}

int WriteState::write_sparse_unsorted_attr_var_cmp_none(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = TILEDB_CELL_VAR_OFFSET_SIZE; 
  size_t cell_var_size;
  const size_t* buffer_s = static_cast<const size_t*>(buffer);
  const char* buffer_var_c = static_cast<const char*>(buffer_var); 

  // Check number of cells in buffer
  int64_t buffer_cell_num = buffer_size / cell_size;
  if(buffer_cell_num != int64_t(cell_pos.size())) {
    std::string errmsg = 
        std::string("Cannot write sparse unsorted variable; "
        "Invalid number of cells in attribute '") + 
        array_schema->attribute(attribute_id) + "'";
    PRINT_ERROR(errmsg);
    tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
    return TILEDB_WS_ERR;
  }

  // Allocate a local buffer to hold the sorted cells
  char* sorted_buffer = new char[TILEDB_SORTED_BUFFER_SIZE]; 
  size_t sorted_buffer_size = 0;
  char* sorted_buffer_var = new char[TILEDB_SORTED_BUFFER_VAR_SIZE]; 
  size_t sorted_buffer_var_size = 0;

  // Sort and write attribute values in batches
  for(int64_t i=0; i<buffer_cell_num; ++i) {
    // Calculate variable cell size
    cell_var_size = (cell_pos[i] == buffer_cell_num - 1) 
                        ? buffer_var_size - buffer_s[cell_pos[i]] 
                        : buffer_s[cell_pos[i]+1] - buffer_s[cell_pos[i]]; 

    // Write batch
    if(sorted_buffer_size + cell_size > TILEDB_SORTED_BUFFER_SIZE ||
       sorted_buffer_var_size + cell_var_size > TILEDB_SORTED_BUFFER_VAR_SIZE) {
      if(write_sparse_attr_var_cmp_none(
             attribute_id,
             sorted_buffer, 
             sorted_buffer_size,
             sorted_buffer_var,
             sorted_buffer_var_size) != TILEDB_WS_OK) {
        delete [] sorted_buffer;
        delete [] sorted_buffer_var;
        return TILEDB_WS_ERR;
      }

      sorted_buffer_size = 0;
      sorted_buffer_var_size = 0;
    } 

    // Keep on copying the cells in sorted order in the sorted buffer
    memcpy(
        sorted_buffer + sorted_buffer_size, 
        &sorted_buffer_var_size, 
        cell_size);
    sorted_buffer_size += cell_size;

    // Keep on copying the variable cells in sorted order in the sorted buffer
    memcpy(
        sorted_buffer_var + sorted_buffer_var_size, 
        buffer_var_c + buffer_s[cell_pos[i]], 
        cell_var_size);
    sorted_buffer_var_size += cell_var_size;
  }

  // Write final batch
  if(sorted_buffer_size != 0) {
    if(write_sparse_attr_var_cmp_none(
           attribute_id, 
           sorted_buffer, 
           sorted_buffer_size,
           sorted_buffer_var,
           sorted_buffer_var_size) != TILEDB_WS_OK) {
      delete [] sorted_buffer;
      delete [] sorted_buffer_var;
      return TILEDB_WS_ERR;
    }
  }

  // Clean up
  delete [] sorted_buffer;
  delete [] sorted_buffer_var;

  // Success
  return TILEDB_WS_OK;
}

int WriteState::write_sparse_unsorted_attr_var_cmp(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size,
    const std::vector<int64_t>& cell_pos) {

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = TILEDB_CELL_VAR_OFFSET_SIZE; 
  size_t cell_var_size;
  const size_t* buffer_s = static_cast<const size_t*>(buffer);
  const char* buffer_var_c = static_cast<const char*>(buffer_var); 

  // Check number of cells in buffer
  int64_t buffer_cell_num = buffer_size / cell_size;
  if(buffer_cell_num != int64_t(cell_pos.size())) {
    std::string errmsg = 
        std::string("Cannot write sparse unsorted variable; "
        "Invalid number of cells in attribute '") + 
        array_schema->attribute(attribute_id) + "'";
    PRINT_ERROR(errmsg);
    tiledb_ws_errmsg = TILEDB_WS_ERRMSG + errmsg;
    return TILEDB_WS_ERR;
  }

  // Allocate a local buffer to hold the sorted cells
  char* sorted_buffer = new char[TILEDB_SORTED_BUFFER_SIZE]; 
  size_t sorted_buffer_size = 0;
  char* sorted_buffer_var = new char[TILEDB_SORTED_BUFFER_VAR_SIZE]; 
  size_t sorted_buffer_var_size = 0;

  // Sort and write attribute values in batches
  for(int64_t i=0; i<buffer_cell_num; ++i) {
    // Calculate variable cell size
    cell_var_size = (cell_pos[i] == buffer_cell_num - 1) 
                        ? buffer_var_size - buffer_s[cell_pos[i]] 
                        : buffer_s[cell_pos[i]+1] - buffer_s[cell_pos[i]]; 

    // Write batch
    if(sorted_buffer_size + cell_size > TILEDB_SORTED_BUFFER_SIZE ||
       sorted_buffer_var_size + cell_var_size > TILEDB_SORTED_BUFFER_VAR_SIZE) {
      if(write_sparse_attr_var_cmp(
             attribute_id,
             sorted_buffer, 
             sorted_buffer_size,
             sorted_buffer_var,
             sorted_buffer_var_size) != TILEDB_WS_OK) {
        delete [] sorted_buffer;
        delete [] sorted_buffer_var;
        return TILEDB_WS_ERR;
      }

      sorted_buffer_size = 0;
      sorted_buffer_var_size = 0;
    } 

    // Keep on copying the cells in sorted order in the sorted buffer
    memcpy(
        sorted_buffer + sorted_buffer_size, 
        &sorted_buffer_var_size, 
        cell_size);
    sorted_buffer_size += cell_size;

    // Keep on copying the variable cells in sorted order in the sorted buffer
    memcpy(
        sorted_buffer_var + sorted_buffer_var_size, 
        buffer_var_c + buffer_s[cell_pos[i]], 
        cell_var_size);
    sorted_buffer_var_size += cell_var_size;
  }

  // Write final batch
  if(sorted_buffer_size != 0) {
    if(write_sparse_attr_var_cmp(
           attribute_id, 
           sorted_buffer, 
           sorted_buffer_size,
           sorted_buffer_var,
           sorted_buffer_var_size) != TILEDB_WS_OK) {
      delete [] sorted_buffer;
      delete [] sorted_buffer_var;
      return TILEDB_WS_ERR;
    }
  }

  // Clean up
  delete [] sorted_buffer;
  delete [] sorted_buffer_var;

  // Success
  return TILEDB_WS_OK;
}
