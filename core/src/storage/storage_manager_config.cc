/**
 * @file   storage_manager_config.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 * @copyright Copyright (c) 2018-2021 Omics Data Automation, Inc.
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
 * This file implements the StorageManagerConfig class.
 */

#include "storage_azure_blob.h"
#include "storage_gcs.h"
#include "storage_s3.h"
#include "storage_manager_config.h"
#include "tiledb_constants.h"
#include "utils.h"

#include <assert.h>
#include <iostream>
#include <string.h>

#include <system_error>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef TILEDB_VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_SMC_ERRMSG << x << ".\n"
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif


/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_smc_errmsg = "";

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

StorageManagerConfig::StorageManagerConfig() {
  // Default values
  fs_ = new PosixFS();
  home_ = "";
  read_method_ = TILEDB_IO_MMAP;
  write_method_ = TILEDB_IO_WRITE;
#ifdef HAVE_MPI
  mpi_comm_ = NULL;
#endif
}

StorageManagerConfig::~StorageManagerConfig() {
  if (fs_ != NULL) {
    delete fs_;
  }
}

/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

#define CONCAT_ERRMSG(x, y, z) x+"\n"+y+"\n"+TILEDB_FS_ERRMSG+z

int StorageManagerConfig::init(
    const char* home,
#ifdef HAVE_MPI
    MPI_Comm* mpi_comm,
#endif
    int read_method,
    int write_method,
    const bool enable_shared_posixfs_optimizations) {
  // Initialize home
  if (home !=  NULL && strstr(home, "://")) {
     if (fs_ != NULL) {
        delete fs_;
        fs_ = NULL;
     }
     home_ = std::string(home, strlen(home));
     std::string errmsg;
     if (is_azure_blob_storage_path(home_)) {
       try {
         fs_ = new AzureBlob(home_);
       } catch(std::system_error& ex) {
         errmsg = CONCAT_ERRMSG("Azure Storage Blob initialization failed for home=" + home_, tiledb_fs_errmsg, ex.what());
         printf("%s\n", errmsg.c_str());
         PRINT_ERROR(errmsg);
         tiledb_smc_errmsg = TILEDB_SMC_ERRMSG + errmsg;
         return TILEDB_SMC_ERR;
       }
     } else if (is_s3_storage_path(home_)) {
       try {
          fs_ = new S3(home_);
       } catch(std::system_error& ex) {
         errmsg = CONCAT_ERRMSG("S3 Storage initialization failed for home=" + home_, tiledb_fs_errmsg, ex.what());
         PRINT_ERROR(ex.what());
         tiledb_smc_errmsg = TILEDB_SMC_ERRMSG + errmsg;
         return TILEDB_SMC_ERR;
       }
     } else if (is_gcs_path(home_)) {
       try {
          fs_ = new GCS(home_);
       } catch(std::system_error& ex) {
         errmsg =  CONCAT_ERRMSG("GCS Storage initialization failed for home=" + home_, tiledb_fs_errmsg, ex.what());
         PRINT_ERROR(ex.what());
         tiledb_smc_errmsg = TILEDB_SMC_ERRMSG + errmsg;
         return TILEDB_SMC_ERR;
       }
     } else if (is_supported_cloud_path(home_)) {
       try {
#ifdef USE_HDFS
         fs_ = new HDFS(home_);
#else
         throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "TileDB built with HDFS support disabled.");
#endif
       } catch(std::system_error& ex) {
         errmsg = CONCAT_ERRMSG("HDFS initialization failed for home=" + home_, tiledb_fs_errmsg, ex.what());
         PRINT_ERROR(ex.what());
         tiledb_smc_errmsg = TILEDB_SMC_ERRMSG + errmsg;
         return TILEDB_SMC_ERR;
       }
     } else {
       tiledb_smc_errmsg = "No TileDB support for home=" + home_;
       PRINT_ERROR(tiledb_smc_errmsg);
       return TILEDB_SMC_ERR;
     }

     read_method_ = TILEDB_IO_READ;
     write_method_ = TILEDB_IO_WRITE;
     return TILEDB_SMC_OK;
  }

  // Default Posix case
  assert(fs_ != NULL);
  dynamic_cast<PosixFS *>(fs_)->set_disable_file_locking(enable_shared_posixfs_optimizations);
  dynamic_cast<PosixFS *>(fs_)->set_keep_write_file_handles_open(enable_shared_posixfs_optimizations);

  if(home == NULL) {
    home_ = "";
  } else {
    home_ = std::string(home, strlen(home));
  }

#ifdef HAVE_MPI
  // Initialize MPI communicator
  mpi_comm_ = mpi_comm;
#endif

  // Initialize read method
  read_method_ = read_method;
  if(read_method_ != TILEDB_IO_READ &&
     read_method_ != TILEDB_IO_MMAP &&
     read_method_ != TILEDB_IO_MPI)
    read_method_ = TILEDB_IO_MMAP;  // Use default 

  // Initialize write method
  write_method_ = write_method;
  if(write_method_ != TILEDB_IO_WRITE &&
     write_method_ != TILEDB_IO_MPI)
    write_method_ = TILEDB_IO_WRITE;  // Use default 

  return TILEDB_SMC_OK;
}

/* ****************************** */
/*            ACCESSORS           */
/* ****************************** */

const std::string& StorageManagerConfig::home() const {
  return home_;
}

#ifdef HAVE_MPI
MPI_Comm* StorageManagerConfig::mpi_comm() const {
  return mpi_comm_;
}
#endif

int StorageManagerConfig::read_method() const {
  return read_method_;
}

int StorageManagerConfig::write_method() const {
  return write_method_;
}

StorageFS* StorageManagerConfig::get_filesystem() const {
  return fs_;
}
