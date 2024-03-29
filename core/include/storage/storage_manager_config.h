/**
 * @file  storage_manager_config.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines class StorageManagerConfig.  
 */  

#ifndef __CONFIG_H__
#define __CONFIG_H__

#ifdef HAVE_MPI
  #include <mpi.h>
#endif
#include <string>

#include "storage_fs.h"
#include "storage_posixfs.h"
#include "storage_hdfs.h"
#include "storage_gcs.h"

#include "tiledb_constants.h"

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_SMC_OK                                                  0
#define TILEDB_SMC_ERR                                                -1
/**@}*/

/** Default error message. */
#define TILEDB_SMC_ERRMSG std::string("[TileDB::StorageManagerConfig] Error: ")

/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Stores potential error messages. */
extern std::string tiledb_smc_errmsg;

/** 
 * This class is responsible for the TileDB storage manager configuration 
 * parameters. 
 */
class StorageManagerConfig {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  StorageManagerConfig();

  /** Destructor. */
  ~StorageManagerConfig();



 
  /* ********************************* */
  /*             MUTATORS              */
  /* ********************************* */

#ifdef HAVE_MPI
  /**
   * Initializes the configuration parameters.
   *
   * @param home The TileDB home directory.
   * @param mpi_comm The MPI communicator.
   * @param read_method The method for reading data from a file. 
   *     It can be one of the following: 
   *        - TILEDB_IO_READ
   *          TileDB will use POSIX read.
   *        - TILEDB_IO_MMAP
   *          TileDB will use mmap.
   *        - TILEDB_IO_MPI
   *          TileDB will use MPI-IO read. 
   * @param write_method The method for writing data to a file. 
   *     It can be one of the following: 
   *        - TILEDB_IO_WRITE
   *          TileDB will use POSIX write.
   *        - TILEDB_IO_MPI
   *          TileDB will use MPI-IO write.
   * @param enable_shared_posixfs_optimizations in POSIX fs if set
   * @return void. 
   */
  int init(
      const char* home,
      MPI_Comm* mpi_comm = NULL,
      int read_method = TILEDB_IO_READ,
      int write_methods = TILEDB_IO_WRITE,
      const bool enable_shared_posixfs_optimizations = false);
#else
  /**
   * Initializes the configuration parameters.
   *
   * @param home The TileDB home directory.
   * @param read_method The method for reading data from a file. 
   *     It can be one of the following: 
   *        - TILEDB_IO_READ
   *          TileDB will use POSIX read.
   *        - TILEDB_IO_MMAP
   *          TileDB will use mmap.
   *        - TILEDB_IO_MPI
   *          TileDB will use MPI-IO read. 
   * @param write_method The method for writing data to a file. 
   *     It can be one of the following: 
   *        - TILEDB_IO_WRITE
   *          TileDB will use POSIX write.
   *        - TILEDB_IO_MPI
   *          TileDB will use MPI-IO write.
   * @param enable_shared_posixfs_optimizations if set
   * @return void. 
   */
  int init(
      const char* home,
      int read_method = TILEDB_IO_READ,
      int write_method = TILEDB_IO_WRITE,
      const bool enable_shared_posixfs_optimizations = false);
#endif
 
  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** Returns the TileDB home directory. */
  const std::string& home() const; 

#ifdef HAVE_MPI
  /** Returns the MPI communicator. */
  MPI_Comm* mpi_comm() const;
#endif

  /** Returns the read method. */
  int read_method() const;

  /** Returns the write method. */
  int write_method() const;

  /** Returns the supporting filesystem */
  StorageFS* get_filesystem() const;
  
 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** TileDB home directory. */
  std::string home_;
#ifdef HAVE_MPI
  /** The MPI communicator. */
  MPI_Comm* mpi_comm_;
#endif
  /** 
   * The method for reading data from a file. 
   * It can be one of the following: 
   *    - TILEDB_IO_READ
   *      TileDB will use POSIX read.
   *    - TILEDB_IO_MMAP
   *      TileDB will use mmap.
   *    - TILEDB_IO_MPI
   *      TileDB will use MPI-IO read. 
   */
  int read_method_;
  /** 
   * The method for writing data to a file. 
   * It can be one of the following: 
   *    - TILEDB_IO_WRITE
   *      TileDB will use POSIX write.
   *    - TILEDB_IO_MPI
   *      TileDB will use MPI-IO write. 
   */
  int write_method_;

  /** The Filesystem type associated with this configuration */
  StorageFS *fs_ = NULL;
};

#endif
