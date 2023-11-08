/**
 * @file   array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 * @copyright Copyright (c) 2018-2019, 2022-2023 Omics Data Automation, Inc.
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
 * This file implements the Array class.
 */

#include "array.h"
#include "mem_utils.h"
#include "utils.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <sys/time.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <uuid/uuid.h>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef TILEDB_VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_AR_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif




/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_ar_errmsg = "";


/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Array::Array() {
  array_read_state_ = NULL;
  array_sorted_read_state_ = NULL;
  array_sorted_write_state_ = NULL;
  array_schema_ = NULL;
  subarray_ = NULL;
  expression_ = NULL;
  aio_thread_created_ = false;
  array_clone_ = NULL;
}

Array::~Array() {
  // Applicable to both arrays and array clones
  std::vector<Fragment*>::iterator it = fragments_.begin();
  for(; it != fragments_.end(); ++it)
    if(*it != NULL)
       delete *it;
  if(expression_ != NULL)
    delete expression_;
  if(array_read_state_ != NULL)
    delete array_read_state_;
  if(array_sorted_read_state_ != NULL)
    delete array_sorted_read_state_;
  if(array_sorted_write_state_ != NULL)
    delete array_sorted_write_state_;

  // Applicable only to non-clones
  if(array_clone_ != NULL) {
    delete array_clone_;
    if(array_schema_ != NULL)
      delete array_schema_;
  }
  if(subarray_ != NULL)
    free(subarray_);
  subarray_ = NULL;
}




/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

void Array::aio_handle_requests() {
  // Holds the next AIO request
  AIO_Request* aio_next_request;

  // Initiate infinite loop
  for(;;) {
    // Lock AIO mutext
    if(pthread_mutex_lock(&aio_mtx_)) {
      std::string errmsg = "Cannot lock AIO mutex";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return;
    } 

    // If the thread is canceled, unblock and exit
    if(aio_thread_canceled_) {
      if(pthread_mutex_unlock(&aio_mtx_)) 
        PRINT_ERROR("Cannot unlock AIO mutex while canceling AIO thread");
      else 
        aio_thread_created_ = false;
      return;
    }

    // Wait for AIO requests
    while(aio_queue_.size() == 0) {
      // Wait to be signaled
      if(pthread_cond_wait(&aio_cond_, &aio_mtx_)) {
        PRINT_ERROR("Cannot wait on AIO mutex condition");
        return;
      }

      // If the thread is canceled, unblock and exit
      if(aio_thread_canceled_) {
        if(pthread_mutex_unlock(&aio_mtx_)) { 
          std::string errmsg = 
              "Cannot unlock AIO mutex while canceling AIO thread";
          PRINT_ERROR(errmsg);
          tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
        } else { 
          aio_thread_created_ = false;
        }
        return;
      }
    }
    
    // Pop the next AIO request 
    aio_next_request = aio_queue_.front(); 
    aio_queue_.pop();

    // Unlock AIO mutext
    if(pthread_mutex_unlock(&aio_mtx_)) {
      std::string errmsg = "Cannot unlock AIO mutex";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return;
    }

    // Handle the next AIO request
    aio_handle_next_request(aio_next_request);

    // Set last handled AIO request
    aio_last_handled_request_ = aio_next_request->id_;
  }
}

int Array::aio_read(AIO_Request* aio_request) {
  // Sanity checks
  if(!read_mode()) {
    std::string errmsg = "Cannot (async) read from array; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Create the AIO thread if not already done
  if(!aio_thread_created_)
    if(aio_thread_create() != TILEDB_AR_OK) 
      return TILEDB_ERR;

  // Push the AIO request in the queue
  if(aio_push_request(aio_request) != TILEDB_AR_OK)
    return TILEDB_AR_ERR; 

  // Success
  return TILEDB_AR_OK;
}

int Array::aio_write(AIO_Request* aio_request) {
  // Sanity checks
  if(!write_mode()) {
    std::string errmsg = "Cannot (async) write to array; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Create the AIO thread if not already done
  if(!aio_thread_created_)
    if(aio_thread_create() != TILEDB_AR_OK) 
      return TILEDB_ERR;

  // Push the AIO request in the queue
  if(aio_push_request(aio_request) != TILEDB_AR_OK)
    return TILEDB_AR_ERR; 

  // Success
  return TILEDB_AR_OK;
}

Array* Array::array_clone() const {
  return array_clone_;
}

const ArraySchema* Array::array_schema() const {
  return array_schema_;
}

const std::vector<int>& Array::attribute_ids() const {
  return attribute_ids_;
}

const StorageManagerConfig* Array::config() const {
  return config_;
}

int Array::fragment_num() const {
  return fragments_.size();
}

std::vector<Fragment*> Array::fragments() const {
  return fragments_;
}

int Array::mode() const {
  return mode_;
}

bool Array::overflow() const {
  // Not applicable to writes
  if(!read_mode()) 
    return false;

  // Check overflow
  if(array_sorted_read_state_ != NULL)
     return array_sorted_read_state_->overflow();
  else
    return array_read_state_->overflow();
}

bool Array::overflow(int attribute_id) const {
  assert(read_mode() || consolidate_mode());

  // Trivial case
  if(fragments_.size() == 0)
    return false;

  // Check overflow
  if(array_sorted_read_state_ != NULL)
    return array_sorted_read_state_->overflow(attribute_id);
  else
    return array_read_state_->overflow(attribute_id);
}

int Array::read(void** buffers, size_t* buffer_sizes, size_t* skip_counts) {
  // Sanity checks
  if(!read_mode() && !consolidate_mode()) {
    std::string errmsg = "Cannot read from array; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Check if there are no fragments 
  int buffer_i = 0;
  int attribute_id_num = attribute_ids_.size();
  if(fragments_.size() == 0) {             
    for(int i=0; i<attribute_id_num; ++i) {
      // Update all sizes to 0
      buffer_sizes[buffer_i] = 0; 
      if(!array_schema_->var_size(attribute_ids_[i])) 
        ++buffer_i;
      else 
        buffer_i += 2;
    }
    return TILEDB_AR_OK;
  }

  // Handle sorted modes
  if(mode_ == TILEDB_ARRAY_READ_SORTED_COL ||
     mode_ == TILEDB_ARRAY_READ_SORTED_ROW) {
      if(skip_counts) {
        tiledb_ar_errmsg = "skip counts only handled for TILDB_ARRAY_READ mode, unsupported for TILEDB_ARRAY_READ_SORTED* modes";
        return TILEDB_AR_ERR;
      }
      if(array_sorted_read_state_->read(buffers, buffer_sizes) == 
         TILEDB_ASRS_OK) {
        return TILEDB_AR_OK;
      } else {
        tiledb_ar_errmsg = tiledb_asrs_errmsg;
        return TILEDB_AR_ERR;
      }
  } else { // mode_ == TILDB_ARRAY_READ 
    return read_default(buffers, buffer_sizes, skip_counts);
  }
}

int Array::evaluate_cell(void** buffer, size_t* buffer_sizes, int64_t* positions) {
  if (expression_) {
    int rc = expression_->evaluate_cell(buffer, buffer_sizes, positions);
    if (rc == TILEDB_EXPR_ERR) {
      tiledb_ar_errmsg = tiledb_expr_errmsg;
      return TILEDB_AR_ERR;
    }
    return rc;
  }
  return true;
}

int Array::read_default(void** buffers, size_t* buffer_sizes, size_t* skip_counts) {
  if(array_read_state_->read(buffers, buffer_sizes, skip_counts) != TILEDB_ARS_OK) {
    tiledb_ar_errmsg = tiledb_ars_errmsg;
    return TILEDB_AR_ERR;
  }

  // Success
  return TILEDB_AR_OK;
}

bool Array::read_mode() const {
  return array_read_mode(mode_);
}

const void* Array::subarray() const {
  return subarray_;
}

bool Array::write_mode() const {
  return array_write_mode(mode_);
}

bool Array::consolidate_mode() const {
  return array_consolidate_mode(mode_);
}

/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

Fragment* get_fragment_for_consolidation(StorageFS *fs, std::string fragment_name, const Array *array) {
  Fragment* fragment = new Fragment(array);
  bool dense = !fs->is_file(fs->append_paths(fragment_name, std::string(TILEDB_COORDS) + TILEDB_FILE_SUFFIX));
  BookKeeping *book_keeping = new BookKeeping(array->array_schema(), dense, fragment_name, TILEDB_ARRAY_READ);
  if (book_keeping->load(fs) != TILEDB_BK_OK) {
    tiledb_ar_errmsg = tiledb_bk_errmsg;
    return NULL;
  }
  if(fragment->init(fragment_name, book_keeping, TILEDB_ARRAY_READ) != TILEDB_FG_OK) {
    tiledb_ar_errmsg = tiledb_fg_errmsg;
    return NULL;
  }
  return fragment;
}

int Array::consolidate(
    Fragment*& new_fragment,
    std::vector<std::string>& old_fragment_names,
    size_t buffer_size,
    int batch_size) {
  // Trivial case
  if(fragment_names_.size() <= 1)
    return TILEDB_AR_OK;

#ifdef DO_MEMORY_PROFILING
  std::cerr << "Using buffer_size=" << buffer_size << " for consolidation" << std::endl;
  std::cerr << "Number of fragments to consolidate=" << fragment_names_.size() << std::endl;
  print_memory_stats("beginning consolidation");
#endif

  // Consolidate on a per-batch and per-attribute basis
  if (batch_size <= 0 || (size_t)batch_size > fragment_names_.size()) {
    batch_size = fragment_names_.size();
  }
  auto remaining = (fragment_names_.size()%batch_size);
  int num_batches = (fragment_names_.size()/batch_size)+(remaining?1:0);

  // Create the buffers
  int attribute_num = array_schema_->attribute_num();
  int var_attribute_num = array_schema_->var_attribute_num();
  int buffer_num = attribute_num + 1 + var_attribute_num;
  void **buffers = (void**) malloc(buffer_num * sizeof(void*));
  size_t *buffer_sizes = (size_t*) malloc(buffer_num * sizeof(size_t));

  void *buffer = malloc(buffer_size);
  void *buffer_var = malloc(buffer_size);

  StorageFS* fs = config_->get_filesystem();

  // Consolidating per batch
  std::string last_batch_fragment_name;
  for (auto batch=0; batch<num_batches; batch++) {
#ifdef DO_MEMORY_PROFILING
    print_memory_stats("Start: batch " + std::to_string(batch+1) + "/" + std::to_string(num_batches));
#endif

    // Get new fragment name
    std::string new_fragment_name = this->new_fragment_name();
    if(new_fragment_name == "") {
      std::string errmsg = "Cannot produce new fragment name";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return TILEDB_AR_ERR;
    }

    new_fragment = new Fragment(this);
    if(new_fragment->init(new_fragment_name, TILEDB_ARRAY_WRITE, subarray_) != TILEDB_FG_OK) {
      tiledb_ar_errmsg = tiledb_fg_errmsg;
      return TILEDB_AR_ERR;
    }

    int actual_batch_size;
    if (batch == num_batches-1) {
      actual_batch_size = remaining?remaining:batch_size;
    } else {
      actual_batch_size = batch_size;
    }
    auto start = batch*actual_batch_size;
    for (auto fragment_i = start; fragment_i<start+actual_batch_size; fragment_i++) {
      fragments_.push_back(get_fragment_for_consolidation(fs, fragment_names_[fragment_i], this));
    }
    if (!last_batch_fragment_name.empty()) {
      fragments_.push_back(get_fragment_for_consolidation(fs, last_batch_fragment_name, this));
    }

    array_read_state_ = new ArrayReadState(this);

    // Consolidating per batch per attribute
    for (auto i=0u,j=0u; i<(size_t)array_schema_->attribute_num()+1; ++i,++j) {
      buffers[j] = buffer;
      buffer_sizes[j] = buffer_size;
      if (array_schema_->var_size(i)) {
        buffers[j+1] = buffer_var;
        buffer_sizes[j+1] = buffer_size;
        j++;
      }
      if(consolidate(new_fragment, i, buffers, buffer_sizes, buffer_size) != TILEDB_AR_OK) {
        delete_dir(fs, new_fragment->fragment_name());
        delete new_fragment;
        return TILEDB_AR_ERR;
      }
#ifdef DO_MEMORY_PROFILING
      print_memory_stats("End: consolidating attribute " + array_schema_->attribute(i));
#endif
      trim_memory();
    }

    // Cleanup after batch consolidation
    delete array_read_state_;
    array_read_state_ = NULL;
    for (auto fragment_i = 0u; fragment_i<fragments_.size(); fragment_i++) {
      fragments_[fragment_i]->finalize();
      delete fragments_[fragment_i]->book_keeping();
      delete fragments_[fragment_i];
    }
    fragments_.clear();

    if (batch < (num_batches-1)) {
      new_fragment->finalize();
      last_batch_fragment_name = new_fragment->fragment_name();
      old_fragment_names.push_back(last_batch_fragment_name);
      new_fragment = NULL;
    }

#ifdef DO_MEMORY_PROFILING
    print_memory_stats("End: batch " + std::to_string(batch+1) + "/" + std::to_string(num_batches));
#endif
    trim_memory();
  }

  // Clean up
  free(buffer_var);
  free(buffer);
  free(buffer_sizes);
  free(buffers);

#ifdef DO_MEMORY_PROFILING
  print_memory_stats("after final consolidation");
#endif

  old_fragment_names.insert(std::end(old_fragment_names), std::begin(fragment_names_), std::end(fragment_names_));

  // Success
  return TILEDB_AR_OK;
}
    
int Array::consolidate(
    Fragment* new_fragment,
    int attribute_id,
    void **buffers,
    size_t *buffer_sizes,
    size_t buffer_size) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  // Do nothing if the array is dense for the coordinates attribute
  if(array_schema_->dense() && attribute_id == attribute_num)
    return TILEDB_AR_OK;

  // Cache the buffer indices associated with the attribute
  int buffer_index = -1;
  int buffer_var_index = -1;
  int buffer_i = 0;
  for(int i=0; i<attribute_num+1; ++i) {
    if(i == attribute_id) {
      assert(buffers[buffer_i]);
      buffer_index = buffer_i;
      ++buffer_i;
      if(array_schema_->var_size(i)) {
        assert(buffers[buffer_i]);
        buffer_var_index = buffer_i;
        ++buffer_i;
      }
    } else {
      buffers[buffer_i] = NULL;
      buffer_sizes[buffer_i] = 0;
      ++buffer_i;
      if(array_schema_->var_size(i)) {
        buffers[buffer_i] = NULL;
        buffer_sizes[buffer_i] = 0;
        ++buffer_i;
      }
    }
  }

#ifdef DO_MEMORY_PROFILING
  print_memory_stats("after alloc for attribute="+array_schema_->attribute(attribute_id));
#endif

  // Read and write attribute until there is no overflow
  int rc_write = TILEDB_FG_OK; 
  int rc_read = TILEDB_FG_OK; 
  do {
    // Set or reset buffer sizes as they are modified by the reads
    buffer_sizes[buffer_index] = buffer_size;
    if (buffer_var_index != -1) {
      buffer_sizes[buffer_var_index] = buffer_size;
    }
    
    // Read
    rc_read = read(buffers, buffer_sizes);
    if(rc_read != TILEDB_FG_OK)
      break;

    // Write
    rc_write = new_fragment->write(
                   (const void**) buffers, 
                   (const size_t*) buffer_sizes);
    if(rc_write != TILEDB_FG_OK)
      break;
  } while(overflow(attribute_id));

  // Error
  if(rc_write != TILEDB_FG_OK || rc_read != TILEDB_FG_OK) {
    tiledb_ar_errmsg = tiledb_fg_errmsg;
    return TILEDB_AR_ERR;
  }

  // Success
  return TILEDB_AR_OK;
}

int Array::finalize() {
  // Initializations
  int rc = TILEDB_FG_OK;
  int fragment_num =  fragments_.size();
  bool fg_error = false;
  for(int i=0; i<fragment_num; ++i) {
    rc = fragments_[i]->finalize();
    if(rc != TILEDB_FG_OK)  
      fg_error = true;
    delete fragments_[i];
  }
  fragments_.clear();

  // Clean the array read state
  if(array_read_state_ != NULL) {
    delete array_read_state_;
    array_read_state_ = NULL;
  }

  // Clean the array sorted read state
  if(array_sorted_read_state_ != NULL) {
    delete array_sorted_read_state_;
    array_sorted_read_state_ = NULL;
  }

  // Clean the array sorted write state
  if(array_sorted_write_state_ != NULL) {
    delete array_sorted_write_state_;
    array_sorted_write_state_ = NULL;
  }

  if (consolidate_mode()) {
    return fg_error?TILEDB_AR_ERR:TILEDB_AR_OK;
  }

  // Clean the AIO-related members
  int rc_aio_thread = aio_thread_destroy(); 
  int rc_aio_cond = TILEDB_AR_OK, rc_aio_mtx = TILEDB_AR_OK;
  if(pthread_cond_destroy(&aio_cond_))
    rc_aio_cond = TILEDB_AR_ERR;
  if(pthread_mutex_destroy(&aio_mtx_))
    rc_aio_mtx = TILEDB_AR_ERR;
  while(aio_queue_.size() != 0) {
    free(aio_queue_.front());
    aio_queue_.pop();
  }

  // Finalize the clone
  int rc_clone = TILEDB_AR_OK; 
  if(array_clone_ != NULL) 
    rc_clone = array_clone_->finalize();

  // Errors
  if(rc != TILEDB_FG_OK) {
    tiledb_ar_errmsg = tiledb_fg_errmsg;
    return TILEDB_AR_ERR;
  }
  if(rc_aio_thread != TILEDB_AR_OK) 
    return TILEDB_AR_ERR;
  if(rc_aio_cond != TILEDB_AR_OK) {
    std::string errmsg = "Cannot destroy AIO mutex condition";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }
  if(rc_aio_mtx != TILEDB_AR_OK) {
    std::string errmsg = "Cannot destroy AIO mutex";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }
  if(rc_clone != TILEDB_AR_OK)
    return TILEDB_AR_ERR; 
  if(fg_error) 
    return TILEDB_AR_ERR; 

  // Success
  return TILEDB_AR_OK; 
}

int Array::init(
    const ArraySchema* array_schema,
    const std::string array_path_used,
    const std::vector<std::string>& fragment_names,
    const std::vector<BookKeeping*>& book_keeping,
    int mode,
    const char** attributes,
    int attribute_num,
    const void* subarray,
    const StorageManagerConfig* config,
    Array* array_clone) {
  // Set mode
  mode_ = mode;

  //Set path used to access array - might be different from the one in the schema
  array_path_used_ = array_path_used;

  // Set array clone
  array_clone_ = array_clone;

  // Sanity check on mode
  if(!read_mode() && !write_mode() && !consolidate_mode()) {
    std::string errmsg = "Cannot initialize array; Invalid array mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Set config
  config_ = config;

  // Set array schema
  array_schema_ = array_schema;

  if (consolidate_mode()) {
    fragment_names_ = fragment_names;
  }

  // Set subarray
  size_t subarray_size = 2*array_schema_->coords_size();
  subarray_ = malloc(subarray_size);
  if(subarray == NULL) 
    memcpy(subarray_, array_schema->domain(), subarray_size);
  else 
    memcpy(subarray_, subarray, subarray_size);

  // Get attributes
  std::vector<std::string> attributes_vec;
  if(attributes == NULL) { // Default: all attributes
    attributes_vec = array_schema_->attributes();
    if(array_schema_->dense() && mode != TILEDB_ARRAY_WRITE_UNSORTED)
      // Remove coordinates attribute for dense arrays, 
      // unless in TILEDB_WRITE_UNSORTED mode
      attributes_vec.pop_back(); 
  } else {                 // Custom attributes
    // Get attributes
    bool coords_found = false;
    bool sparse = !array_schema_->dense();
    for(int i=0; i<attribute_num; ++i) {
      // Check attribute name length
      if(attributes[i] == NULL || strlen(attributes[i]) > TILEDB_NAME_MAX_LEN) {
        std::string errmsg = "Invalid attribute name length";
        PRINT_ERROR(errmsg);
        tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
        return TILEDB_AR_ERR;
      }
      attributes_vec.push_back(attributes[i]);
      if(!strcmp(attributes[i], TILEDB_COORDS))
        coords_found = true;
    }

    // Sanity check on duplicates 
    if(has_duplicates(attributes_vec)) {
      std::string errmsg = "Cannot initialize array; Duplicate attributes";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return TILEDB_AR_ERR;
    }

    // For the case of the clone sparse array, append coordinates if they do
    // not exist already
    if(sparse                   && 
       array_clone == NULL      && 
       !coords_found            && 
       !is_metadata(config_->get_filesystem(), get_array_path_used()))
      attributes_vec.push_back(TILEDB_COORDS);
  }

  // Set attribute ids
  if(array_schema_->get_attribute_ids(attributes_vec, attribute_ids_)
         != TILEDB_AS_OK) {
    tiledb_ar_errmsg = tiledb_as_errmsg;
    return TILEDB_AR_ERR;
  }

  try {
    // Initialize new fragment if needed
    if(write_mode()) { // WRITE MODE
      // Get new fragment name
      std::string new_fragment_name = this->new_fragment_name();
      if(new_fragment_name == "") {
        std::string errmsg = "Cannot produce new fragment name";
        PRINT_ERROR(errmsg);
        tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
        return TILEDB_AR_ERR;
      }

      // Create new fragment
      Fragment* fragment = new Fragment(this);
      fragments_.push_back(fragment);
      if(fragment->init(new_fragment_name, mode_, subarray) != TILEDB_FG_OK) {
        array_schema_ = NULL;
        tiledb_ar_errmsg = tiledb_fg_errmsg;
        return TILEDB_AR_ERR;
      }

      // Create ArraySortedWriteState
      if(mode_ == TILEDB_ARRAY_WRITE_SORTED_COL ||
         mode_ == TILEDB_ARRAY_WRITE_SORTED_ROW) { 
        array_sorted_write_state_ = new ArraySortedWriteState(this);
        if(array_sorted_write_state_->init() != TILEDB_ASWS_OK) {
          tiledb_ar_errmsg = tiledb_asws_errmsg;
          delete array_sorted_write_state_;
          array_sorted_write_state_ = NULL;
          return TILEDB_AR_ERR; 
        }
      } else {
        array_sorted_write_state_ = NULL;
      }
    } else if (consolidate_mode()) {
      array_read_state_ = NULL;
      array_sorted_read_state_ = NULL;
      return TILEDB_OK;
    } else {           // READ MODE
      // Open fragments
      if(open_fragments(fragment_names, book_keeping) != TILEDB_AR_OK) {
        array_schema_ = NULL;
        return TILEDB_AR_ERR;
      }
    
      // Create ArrayReadState
      array_read_state_ = new ArrayReadState(this);

      // Create ArraySortedReadState
      if(mode_ != TILEDB_ARRAY_READ) { 
        array_sorted_read_state_ = new ArraySortedReadState(this);
        if(array_sorted_read_state_->init() != TILEDB_ASRS_OK) {
          tiledb_ar_errmsg = tiledb_asrs_errmsg;
          delete array_sorted_read_state_;
          array_sorted_read_state_ = NULL;
          return TILEDB_AR_ERR; 
        }
      } else {
        array_sorted_read_state_ = NULL;
      }
    } 

    // Initialize the AIO-related members
    aio_cond_ = PTHREAD_COND_INITIALIZER; 
    if(pthread_mutex_init(&aio_mtx_, NULL)) {
      std::string errmsg = "Cannot initialize AIO mutex";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return TILEDB_AR_ERR;
    }
    if(pthread_cond_init(&aio_cond_, NULL)) {
      std::string errmsg = "Cannot initialize AIO mutex condition";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return TILEDB_AR_ERR;
    }
    aio_thread_canceled_ = false;
    aio_thread_created_ = false;
    aio_last_handled_request_ = -1;
  } catch(std::system_error& ex) {
    PRINT_ERROR(ex.what());
    tiledb_ar_errmsg = "Array initialization failed";
    PRINT_ERROR(tiledb_ar_errmsg);
    return TILEDB_AR_ERR;
  }

  // Return
  return TILEDB_AR_OK;
}

int Array::apply_filter(const char* filter_expression) {
  // Set up filter expression
  if (filter_expression != NULL && strlen(filter_expression) > 0) {
    std::vector<std::string> attributes_vec;
    for (std::vector<int>::iterator it = attribute_ids_.begin(); it != attribute_ids_.end(); it++) {
      attributes_vec.push_back(array_schema_->attribute(*it));
    }
    expression_ = new Expression(filter_expression);
    if (expression_->init(attribute_ids_, array_schema_)) {
      tiledb_ar_errmsg = tiledb_expr_errmsg;
      return TILEDB_AR_ERR;
    }
  }

  return TILEDB_AR_OK;
}

int Array::reset_attributes(
    const char** attributes,
    int attribute_num) {
  // Get attributes
  std::vector<std::string> attributes_vec;
  if(attributes == NULL) { // Default: all attributes
    attributes_vec = array_schema_->attributes();
    if(array_schema_->dense()) // Remove coordinates attribute for dense
      attributes_vec.pop_back(); 
  } else {                 //  Custom attributes
    // Copy attribute names
    for(int i=0; i<attribute_num; ++i) {
      // Check attribute name length
      if(attributes[i] == NULL || strlen(attributes[i]) > TILEDB_NAME_MAX_LEN) {
        std::string errmsg = "Invalid attribute name length";
        PRINT_ERROR(errmsg);
        tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
        return TILEDB_AR_ERR;
      }
      attributes_vec.push_back(attributes[i]);
    }

    // Sanity check on duplicates 
    if(has_duplicates(attributes_vec)) {
      std::string errmsg = "Cannot reset attributes; Duplicate attributes";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return TILEDB_AR_ERR;
    }
  }

  // Set attribute ids
  if(array_schema_->get_attribute_ids(attributes_vec, attribute_ids_) 
         != TILEDB_AS_OK) {
    tiledb_ar_errmsg = tiledb_as_errmsg;
    return TILEDB_AR_ERR;
  }

  // Reset subarray so that the read/write states are flushed
  if(reset_subarray(subarray_) != TILEDB_AR_OK) 
    return TILEDB_AR_ERR;

  // Success
  return TILEDB_AR_OK;
}

int Array::reset_subarray(const void* subarray) {
  // Sanity check
  assert(read_mode() || write_mode());

  // For easy referencd
  int fragment_num =  fragments_.size();

  // Finalize fragments if in write mode
  if(write_mode()) {  
    // Finalize and delete fragments     
    for(int i=0; i<fragment_num; ++i) { 
      fragments_[i]->finalize();
      delete fragments_[i];
    }
    fragments_.clear();
  } 

  // Set subarray
  size_t subarray_size = 2*array_schema_->coords_size();
  if(subarray_ == NULL) 
    subarray_ = malloc(subarray_size);
  if(subarray == NULL) 
    memcpy(subarray_, array_schema_->domain(), subarray_size);
  else 
    memmove(subarray_, subarray, subarray_size); //subarray_ and subarray might be the same memory area - see line 780

  // Re-set or re-initialize fragments
  if(write_mode()) {  // WRITE MODE 
    // Finalize last fragment
    if(fragments_.size() != 0) {
      assert(fragments_.size() == 1);
      if(fragments_[0]->finalize() != TILEDB_FG_OK) {
        tiledb_ar_errmsg = tiledb_fg_errmsg;
        return TILEDB_AR_ERR;
      }
      delete fragments_[0];
      fragments_.clear();
    }

    // Re-initialize ArraySortedWriteState
    if(array_sorted_write_state_ != NULL) 
      delete array_sorted_write_state_;
    if(mode_ == TILEDB_ARRAY_WRITE_SORTED_COL ||
       mode_ == TILEDB_ARRAY_WRITE_SORTED_ROW) { 
      array_sorted_write_state_ = new ArraySortedWriteState(this);
      if(array_sorted_write_state_->init() != TILEDB_ASWS_OK) {
        tiledb_ar_errmsg = tiledb_asws_errmsg;
        delete array_sorted_write_state_;
        array_sorted_write_state_ = NULL;
        return TILEDB_AR_ERR; 
      }
    } else {
      array_sorted_write_state_ = NULL;
    }

    // Get new fragment name
    std::string new_fragment_name = this->new_fragment_name();
    if(new_fragment_name == "") {
      std::string errmsg = "Cannot generate new fragment name";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return TILEDB_AR_ERR;
    }

    // Create new fragment
    Fragment* fragment = new Fragment(this);
    fragments_.push_back(fragment);
    if(fragment->init(new_fragment_name, mode_, subarray) != TILEDB_FG_OK) { 
      tiledb_ar_errmsg = tiledb_fg_errmsg;
      return TILEDB_AR_ERR;
    }
  } else {           // READ MODE
    // Re-initialize the read state of the fragments
    for(int i=0; i<fragment_num; ++i) 
      fragments_[i]->reset_read_state();

    // Re-initialize array read state
    if(array_read_state_ != NULL) {
      delete array_read_state_;
      array_read_state_ = NULL;
    }
    array_read_state_ = new ArrayReadState(this);

    // Re-initialize ArraySortedReadState
    if(array_sorted_read_state_ != NULL) 
      delete array_sorted_read_state_;
    if(mode_ != TILEDB_ARRAY_READ) { 
      array_sorted_read_state_ = new ArraySortedReadState(this);
      if(array_sorted_read_state_->init() != TILEDB_ASRS_OK) {
      tiledb_ar_errmsg = tiledb_asrs_errmsg;
        delete array_sorted_read_state_;
        array_sorted_read_state_ = NULL;
        return TILEDB_AR_ERR; 
      }
    } else {
      array_sorted_read_state_ = NULL;
    }
  }

  // Success
  return TILEDB_AR_OK;
}

int Array::reset_subarray_soft(const void* subarray) {
  // Sanity check
  assert(read_mode() || write_mode());

  // For easy referencd
  int fragment_num =  fragments_.size();

  // Finalize fragments if in write mode
  if(write_mode()) {  
    // Finalize and delete fragments     
    for(int i=0; i<fragment_num; ++i) { 
      fragments_[i]->finalize();
      delete fragments_[i];
    }
    fragments_.clear();
  } 

  // Set subarray
  size_t subarray_size = 2*array_schema_->coords_size();
  if(subarray_ == NULL) 
    subarray_ = malloc(subarray_size);
  if(subarray == NULL) 
    memcpy(subarray_, array_schema_->domain(), subarray_size);
  else 
    memcpy(subarray_, subarray, subarray_size);

  // Re-set or re-initialize fragments
  if(write_mode()) {  // WRITE MODE 
    // Do nothing
  } else {            // READ MODE
    // Re-initialize the read state of the fragments
    for(int i=0; i<fragment_num; ++i) 
      fragments_[i]->reset_read_state();

    // Re-initialize array read state
    if(array_read_state_ != NULL) {
      delete array_read_state_;
      array_read_state_ = NULL;
    }
    array_read_state_ = new ArrayReadState(this);
  }

  // Success
  return TILEDB_AR_OK;
}

int Array::sync() {
  // Sanity check
  if(!write_mode()) {
    std::string errmsg = "Cannot sync array; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Sanity check
  assert(fragments_.size() == 1);

  // Sync fragment
  if(fragments_[0]->sync() != TILEDB_FG_OK) {
    tiledb_ar_errmsg = tiledb_fg_errmsg;
    return TILEDB_AR_ERR;
  } else {
    return TILEDB_AR_OK;
  }
}

int Array::sync_attribute(const std::string& attribute) {
  // Sanity checks
  if(!write_mode()) {
    std::string errmsg = "Cannot sync attribute; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Sanity check
  assert(fragments_.size() == 1);

  // Sync fragment
  if(fragments_[0]->sync_attribute(attribute) != TILEDB_FG_OK) {
    tiledb_ar_errmsg = tiledb_fg_errmsg;
    return TILEDB_AR_ERR;
  } else {
    return TILEDB_AR_OK;
  }
}

int Array::write(const void** buffers, const size_t* buffer_sizes) {
  // Sanity checks
  if(!write_mode()) {
    std::string errmsg = "Cannot write to array; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Write based on mode
  if(mode_ == TILEDB_ARRAY_WRITE_SORTED_COL ||
     mode_ == TILEDB_ARRAY_WRITE_SORTED_ROW) { 
    if (array_sorted_write_state_->write(buffers, buffer_sizes) != TILEDB_ASWS_OK) {
      tiledb_ar_errmsg = tiledb_asws_errmsg;
      return TILEDB_AR_ERR;
    }
  } else if(mode_ == TILEDB_ARRAY_WRITE ||
            mode_ == TILEDB_ARRAY_WRITE_UNSORTED) { 
    if (write_default(buffers, buffer_sizes) != TILEDB_AR_OK) {
      return TILEDB_AR_ERR;
    }
  } else {
    assert(0);
  }

  // In all modes except TILEDB_ARRAY_WRITE, the fragment must be finalized
  if(mode_ != TILEDB_ARRAY_WRITE) {
    if(fragments_[0]->finalize() != TILEDB_FG_OK) {
      tiledb_ar_errmsg = tiledb_fg_errmsg;
      return TILEDB_AR_ERR;
    }
    delete fragments_[0];
    fragments_.clear();
  }

  // Success
  return TILEDB_AR_OK;
}

int Array::write_default(const void** buffers, const size_t* buffer_sizes) {
  // Sanity checks
  if(!write_mode()) {
    std::string errmsg = "Cannot write to array; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Create and initialize a new fragment 
  if(fragments_.size() == 0) {
    // Get new fragment name
    std::string new_fragment_name = this->new_fragment_name();
    if(new_fragment_name == "") {
      std::string errmsg = "Cannot produce new fragment name";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return TILEDB_AR_ERR;
    }
   
    // Create new fragment 
    Fragment* fragment = new Fragment(this);
    fragments_.push_back(fragment);
    if(fragment->init(new_fragment_name, mode_, subarray_) != TILEDB_FG_OK) {
      tiledb_ar_errmsg = tiledb_fg_errmsg;
      return TILEDB_AR_ERR;
    }
  }
 
  // Dispatch the write command to the new fragment
  if(fragments_[0]->write(buffers, buffer_sizes) != TILEDB_FG_OK) {
    tiledb_ar_errmsg = tiledb_fg_errmsg;
    return TILEDB_AR_ERR; 
  }

  // Success
  return TILEDB_AR_OK;
}




/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void Array::aio_handle_next_request(AIO_Request* aio_request) {
  int rc = TILEDB_AR_OK;
  if(read_mode()) {   // READ MODE
    // Invoke the read
    if(aio_request->mode_ == TILEDB_ARRAY_READ) { 
      // Reset the subarray only if this request does not continue from the last
      if(aio_last_handled_request_ != aio_request->id_)
        reset_subarray_soft(aio_request->subarray_);

      // Read 
      rc = read_default(aio_request->buffers_, aio_request->buffer_sizes_);
    } else {  
      // This may initiate a series of new AIO requests
      // Reset the subarray hard this time (updating also the subarray
      // of the ArraySortedReadState object.
      if(aio_last_handled_request_ != aio_request->id_)
        reset_subarray(aio_request->subarray_);

      // Read 
      rc = read(aio_request->buffers_, aio_request->buffer_sizes_);
    }
  } else {            // WRITE MODE
    // Invoke the write
    if(aio_request->mode_ == TILEDB_ARRAY_WRITE ||
       aio_request->mode_ == TILEDB_ARRAY_WRITE_UNSORTED) { 
      // Reset the subarray only if this request does not continue from the last
      if(aio_last_handled_request_ != aio_request->id_)
        reset_subarray_soft(aio_request->subarray_);

      // Write
      rc = write_default(
               (const void**) aio_request->buffers_, 
               (const size_t*) aio_request->buffer_sizes_);
    } else {  
      // This may initiate a series of new AIO requests
      // Reset the subarray hard this time (updating also the subarray
      // of the ArraySortedWriteState object.
      if(aio_last_handled_request_ != aio_request->id_)
        reset_subarray(aio_request->subarray_);
     
      // Write
      rc = write(
               (const void**) aio_request->buffers_, 
               (const size_t*) aio_request->buffer_sizes_);
    }
  }

  if(rc == TILEDB_AR_OK) {      // Success
    // Check for overflow (applicable only to reads)
    if(aio_request->mode_ == TILEDB_ARRAY_READ && 
       array_read_state_->overflow()) {
      *aio_request->status_= TILEDB_AIO_OVERFLOW;
      if(aio_request->overflow_ != NULL) {
        for(int i=0; i<int(attribute_ids_.size()); ++i) 
          aio_request->overflow_[i] = 
            array_read_state_->overflow(attribute_ids_[i]);
      }
    } else if((aio_request->mode_ == TILEDB_ARRAY_READ_SORTED_COL ||
               aio_request->mode_ == TILEDB_ARRAY_READ_SORTED_ROW ) && 
              array_sorted_read_state_->overflow()) {
      *aio_request->status_= TILEDB_AIO_OVERFLOW;
      if(aio_request->overflow_ != NULL) {
        for(int i=0; i<int(attribute_ids_.size()); ++i) 
          aio_request->overflow_[i] = 
              array_sorted_read_state_->overflow(attribute_ids_[i]);
      }
    } else { // Completion
      *aio_request->status_= TILEDB_AIO_COMPLETED;
    }

    // Invoke the callback
    if(aio_request->completion_handle_ != NULL) 
      (*(aio_request->completion_handle_))(aio_request->completion_data_);
  } else {                      // Error
    *aio_request->status_= TILEDB_AIO_ERR;
  }
}

void *Array::aio_handler(void* context) {
  // This will enter an indefinite loop that will handle all incoming AIO 
  // requests
  ((Array*) context)->aio_handle_requests();

  // Return
  return NULL;
}

int Array::aio_push_request(AIO_Request* aio_request) {
  // Set the request status
  *aio_request->status_ = TILEDB_AIO_INPROGRESS;

  // Lock AIO mutex
  if(pthread_mutex_lock(&aio_mtx_)) {
    std::string errmsg = "Cannot lock AIO mutex";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Push request
  aio_queue_.push(aio_request);

  // Signal AIO thread
  if(pthread_cond_signal(&aio_cond_)) { 
    std::string errmsg = "Cannot signal AIO thread";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Unlock AIO mutext
  if(pthread_mutex_unlock(&aio_mtx_)) {
    std::string errmsg = "Cannot unlock AIO mutex";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Success
  return TILEDB_AR_OK;
}

int Array::aio_thread_create() {
  // Trivial case
  if(aio_thread_created_)
    return TILEDB_AR_OK;

  // Create the thread that will be handling all AIO requests
  int rc;
  if((rc=pthread_create(&aio_thread_, NULL, Array::aio_handler, this))) {
    std::string errmsg = "Cannot create AIO thread";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  aio_thread_created_ = true;

  // Success
  return TILEDB_AR_OK;
}

int Array::aio_thread_destroy() {
  // Trivial case
  if(!aio_thread_created_)
    return TILEDB_AR_OK;

  // Lock AIO mutext
  if(pthread_mutex_lock(&aio_mtx_)) {
    std::string errmsg = "Cannot lock AIO mutex while destroying AIO thread";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Signal the cancelation so that the thread unblocks
  aio_thread_canceled_ = true;
  if(pthread_cond_signal(&aio_cond_)) { 
    std::string errmsg = "Cannot signal AIO thread while destroying AIO thread";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Unlock AIO mutext
  if(pthread_mutex_unlock(&aio_mtx_)) {
    std::string errmsg = "Cannot unlock AIO mutex while destroying AIO thread";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Wait for cancelation to take place
  while(aio_thread_created_);

  // Join with the terminated thread
  if(pthread_join(aio_thread_, NULL)) {
    std::string errmsg = "Cannot join AIO thread";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Success
  return TILEDB_AR_OK;
}

std::string Array::new_fragment_name() const {
  struct timeval tp;
  gettimeofday(&tp, NULL);
  uint64_t ms = (uint64_t) tp.tv_sec * 1000L + tp.tv_usec / 1000;
  pthread_t self = pthread_self();
  uint64_t tid = 0;
  memcpy(&tid, &self, std::min(sizeof(self), sizeof(tid)));
  char fragment_name[TILEDB_NAME_MAX_LEN];

#ifdef USE_MAC_ADDRESS_IN_FRAGMENT_NAMES
  // Get MAC address
  std::string mac = get_mac_addr();
  if(mac == "")
    return "";
#else
  //Use uuids
  uuid_t value;
  uuid_generate(value);
  char uuid_str[40];
  uuid_unparse(value, uuid_str);
  std::string mac = uuid_str;
#endif

  // Generate fragment name. Note: for cloud based filesystems or if locking_support is
  // turned off explicitly, fragment names are generated "in-place", there will be no
  // rename associated with those fragments.
  int n;
  if (config()->get_filesystem()->locking_support()) {
    n = snprintf(fragment_name, TILEDB_NAME_MAX_LEN, "%s/.__%s%" PRIu64"_%" PRIu64,
		get_array_path_used().c_str(), mac.c_str(), tid, ms);
  } else {
    n = snprintf(fragment_name, TILEDB_NAME_MAX_LEN, "%s/__%s%" PRIu64"_%" PRIu64,
		get_array_path_used().c_str(), mac.c_str(), tid, ms);
  }

  // Handle error
  if(n<0) 
    return "";

  // Return
  return fragment_name;
}

int Array::open_fragments(
    const std::vector<std::string>& fragment_names,
    const std::vector<BookKeeping*>& book_keeping) {
  // Sanity check
  assert(fragment_names.size() == book_keeping.size());

  // Create a fragment object for each fragment directory
  int fragment_num = fragment_names.size();
  for(int i=0; i<fragment_num; ++i) {
    Fragment* fragment = new Fragment(this);
    fragments_.push_back(fragment);

    if(fragment->init(fragment_names[i], book_keeping[i], mode()) != TILEDB_FG_OK) {
      tiledb_ar_errmsg = tiledb_fg_errmsg;
      return TILEDB_AR_ERR;
    }
  } 

  // Success
  return TILEDB_AR_OK;
}

void Array::free_array_schema()
{
  if(array_schema_)
    delete array_schema_;
  array_schema_ = NULL;
}

const std::string& Array::get_array_path_used() const
{
  return array_path_used_;
}
