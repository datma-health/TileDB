/**
 * @file   read_state.h
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
 * This file defines class ReadState. 
 */

#ifndef __READ_STATE_H__
#define __READ_STATE_H__

#include "array.h"
#include "book_keeping.h"
#include "codec.h"
#include "fragment.h"
#include "storage_buffer.h"
#include <vector>





/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_RS_OK         0
#define TILEDB_RS_ERR       -1
/**@}*/

/** Default error message. */
#define TILEDB_RS_ERRMSG std::string("[TileDB::ReadState] Error: ")




/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Stores potential error messages. */
extern std::string tiledb_rs_errmsg;




class Array;
class Fragment;

/** Stores the state necessary when reading cells from a fragment. */
class ReadState {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** A cell position pair [first, second]. */
  typedef std::pair<int64_t, int64_t> CellPosRange;

  /** A pair [fragment_id, tile_pos]. */
  typedef std::pair<int, int64_t> FragmentInfo;

  /** A pair of fragment info and fragment cell position range. */
  typedef std::pair<FragmentInfo, CellPosRange> FragmentCellPosRange;

  /** A vector of fragment cell posiiton ranges. */
  typedef std::vector<FragmentCellPosRange> FragmentCellPosRanges;

  /** A vector of vectors of fragment cell position ranges. */
  typedef std::vector<FragmentCellPosRanges> FragmentCellPosRangesVec;

  /**
   * A pair of fragment info and cell range, where the cell range is defined
   * by two bounding coordinates.
   */
  typedef std::pair<FragmentInfo, void*> FragmentCellRange;

  /** A vector of fragment cell ranges. */
  typedef std::vector<FragmentCellRange> FragmentCellRanges;

 


  /* ********************************* */
  /*    CONSTRUCTORS & DESTRUCTORS     */
  /* ********************************* */

  /** 
   * Constructor. 
   *
   * @param fragment The fragment the read state belongs to.
   * @param book_keeping The book-keeping of the fragment.
   */
  ReadState(const Fragment* fragment, BookKeeping* book_keeping);

  /** Destructor. */
  ~ReadState();


  /* ********************************* */
  /*              MUTATORS             */
  /* ********************************* */

  /**
   * Finalizes the fragment.
   *
   * @return TILEDB_WS_OK for success and TILEDB_WS_ERR for error. 
   */
  int finalize();


  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** Returns *true* if the read state corresponds to a dense fragment. */
  bool dense() const;

  /** Returns *true* if the read operation is finished for this fragment. */
  bool done() const;

  /**
   * Copies the bounding coordinates of the current search tile into the input
   * *bounding_coords*.
   */
  void get_bounding_coords(void* bounding_coords) const;

  /** 
   * Returns *true* if the MBR of the search tile overlaps with the current
   * tile under investigation. Applicable only to **sparse** fragments in
   * **dense** arrays. NOTE: if the MBR of the search tile has not not changed
   * and the function is invoked again, it will return *false*. 
   */
  bool mbr_overlaps_tile() const;

  /** Returns *true* if the read buffers overflowed for the input attribute. */
  bool overflow(int attribute_id) const;

  /** 
   * True if the fragment non-empty domain fully covers the subarray area of
   * the current overlapping tile.
   */
  bool subarray_area_covered() const;





  /* ********************************* */
  /*            MUTATORS               */
  /* ********************************* */

  /** 
   * Resets the read state. Note that it does not flush any buffered tiles, so
   * that they can be reused later if a subsequent request happens to overlap
   * with them.
   * 
   */
  void reset();

  /** 
   * Resets the overflow flag of every attribute to *false*. 
   *
   * @return void.
   */
  void reset_overflow();




  /* ********************************* */
  /*              MISC                 */
  /* ********************************* */

  /**
   * Copies the cells of the input attribute into the input buffers, as 
   * determined by the input cell position range.
   *
   * @param attribute_id The id of the targeted attribute.
   * @param tile_i The tile to copy from.
   * @param buffer The buffer to copy into - see Array::read().
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param buffer_offset The offset in *buffer* where the copy will start from.
   * @param cell_pos_range The cell position range to be copied.
   * @param remaining_skip_count The number of cells to skip before copying
   * @return TILEDB_RS_OK on success and TILEDB_RS_ERR on error.
   */
  int copy_cells(
      int attribute_id,
      int tile_i,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset,
      const CellPosRange& cell_pos_range,
      size_t& remaining_skip_count);

  /**
   * Copies the cells of the input **variable-sized** attribute into the input
   * buffers, as determined by the input cell position range.
   *
   * @param attribute_id The id of the targeted attribute.
   * @param tile_i The tile to copy from.
   * @param buffer The offsets buffer to copy into - see Array::read().
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param buffer_offset The offset in *buffer* where the copy will start from.
   * @param remaining_skip_count The number of cells to skip before copying
   * @param buffer_var The variable-sized cell buffer to copy into - see 
   *     Array::read().
   * @param buffer_var_size The size (in bytes) of *buffer_var*.
   * @param buffer_var_offset The offset in *buffer_var* where the copy will
   *      start from.
   * @param remaining_skip_count_var The number of cells to skip before copying for the var field
   * @param cell_pos_range The cell position range to be copied.
   * @return TILEDB_RS_OK on success and TILEDB_RS_ERR on error.
   */
  int copy_cells_var(
      int attribute_id,
      int tile_i,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset,
      size_t& remaining_skip_count,
      void* buffer_var,
      size_t buffer_var_size,
      size_t& buffer_var_offset,
      size_t& remaining_skip_count_var,
      const CellPosRange& cell_pos_range);

  /** 
   * Retrieves the coordinates after the input coordinates in the search tile.
   * 
   * @tparam T The coordinates type.
   * @param coords The target coordinates.
   * @param coords_after The coordinates to be retrieved.
   * @param coords_retrieved *true* if *coords_after* are indeed retrieved.
   * @return TILEDB_RS_OK on success and TILEDB_RS_ERR on error.
   */
  template<class T>
  int get_coords_after(
      const T* coords,
      T* coords_after,
      bool& coords_retrieved);

  /**
   * Given a target coordinates set, it returns the coordinates preceding and
   * succeeding it in a designated tile and inside an indicated coordinate
   * range. 
   *
   * @tparam T The coordinates type.
   * @param tile_i The targeted tile position. 
   * @param target_coords The target coordinates.
   * @param start_coords The starting coordinates of the target cell range.
   * @param end_coords The ending coordinates of the target cell range.
   * @param left_coords The returned preceding coordinates.
   * @param right_coords The returned succeeding coordinates.
   * @param left_retrieved *true* if the preceding coordinates are retrieved.
   * @param right_retrieved *true* if the succeeding coordinates are retrieved.
   * @param target_exists *true* if the target coordinates exist in the tile.
   * @return TILEDB_RS_OK on success and TILEDB_RS_ERR on error.
   */
  template<class T>
  int get_enclosing_coords(
      int tile_i,
      const T* target_coords,
      const T* start_coords,
      const T* end_coords,
      T* left_coords,
      T* right_coords,
      bool& left_retrieved,
      bool& right_retrieved,
      bool& target_exists);

  /**
   * Retrieves the cell position range corresponding to the input cell range,
   * for the case of **sparse** fragments.
   *
   * @tparam T The coordinates type.
   * @param fragment_info The (fragment id, tile position) pair corresponding
   *     to the cell position range to be retrieved.
   * @param cell_range The targeted cell range.
   * @param fragment_cell_pos_range The result cell position range.
   * @return TILEDB_RS_OK on success and TILEDB_RS_ERR on error.
   */
  template<class T>
  int get_fragment_cell_pos_range_sparse(
      const FragmentInfo& fragment_info,
      const T* cell_range,
      FragmentCellPosRange& fragment_cell_pos_range);

  /**
   * Computes the fragment cell ranges corresponding to the current search
   * tile. Applicable only to **dense**.
   *
   * @tparam T The coordinates type.
   * @param fragment_i The fragment id. 
   * @param fragment_cell_ranges The output fragment cell ranges.
   * @return TILEDB_RS_OK on success and TILEDB_RS_ERR on error.
   */
  template<class T>
  int get_fragment_cell_ranges_dense(
      int fragment_i,
      FragmentCellRanges& fragment_cell_ranges); 

  /**
   * Computes the fragment cell ranges corresponding to the current search
   * tile. Applicable only to **sparse** fragments for **dense** arrays.
   *
   * @tparam T The coordinates type.
   * @param fragment_i The fragment id. 
   * @param fragment_cell_ranges The output fragment cell ranges.
   * @return TILEDB_RS_OK on success and TILEDB_RS_ERR on error.
   */
  template<class T>
  int get_fragment_cell_ranges_sparse(
      int fragment_i,
      FragmentCellRanges& fragment_cell_ranges); 

  /**
   * Computes the fragment cell ranges corresponding to the current search
   * tile, which are contained within the input start and end coordinates.
   * Applicable only to **sparse** fragments for **sparse** arrays.
   *
   * @tparam T The coordinates type.
   * @param fragment_i The fragment id. 
   * @param start_coords The start coordinates of the specified range.
   * @param end_coords The end coordinates of the specified range.
   * @param fragment_cell_ranges The output fragment cell ranges.
   * @return TILEDB_RS_OK on success and TILEDB_RS_ERR on error.
   */
  template<class T>
  int get_fragment_cell_ranges_sparse(
      int fragment_i,
      const T* start_coords,
      const T* end_coords,
      FragmentCellRanges& fragment_cell_ranges); 

  /**
   * Gets the next overlapping tile from the fragment, which may overlap or not
   * with the tile specified by the input tile coordinates. This is applicable
   * only to **dense** fragments.
   *
   * @tparam T The coordinates type.
   * @param tile_coords The input tile coordinates.
   * @return void
   */
  template<class T>
  void get_next_overlapping_tile_dense(const T* tile_coords);

  /**
   * Gets the next overlapping tile from the fragment. This is applicable
   * only to **sparse** arrays.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template<class T>
  void get_next_overlapping_tile_sparse();

  /**
   * Gets the next overlapping tile from the fragment, such that it overlaps or
   * succeeds the tile with the input tile coordinates. This is applicable
   * only to **sparse** fragments for **dense** arrays.
   *
   * @tparam T The coordinates type.
   * @param tile_coords The input tile coordinates.
   * @return void
   */
  template<class T>
  void get_next_overlapping_tile_sparse(const T* tile_coords);




 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array the fragment belongs to. */
  const Array* array_;
  /** The array schema. */
  const ArraySchema* array_schema_;
  /** The number of array attributes. */
  int attribute_num_;
  /** The book-keeping of the fragment the read state belongs to. */
  BookKeeping* book_keeping_;
  /** The size of the array coordinates. */
  size_t coords_size_;

  /** Internal buffers associated with the attribute files */
  std::vector<StorageBuffer *> file_buffer_;
  std::vector<StorageBuffer *> file_var_buffer_;

  /** Cache attribute filesizes for fragment */
  std::vector<ssize_t> file_size_;
  std::vector<ssize_t> file_var_size_;
  
  /** Compression per attribute */
  std::vector<Codec *> codec_;
  std::vector<Codec *> offsets_codec_;

  /** Indicates if the read operation on this fragment finished. */
  bool done_;
  /** Keeps track of which tile is in main memory for each attribute. */ 
  std::vector<int64_t> fetched_tile_;
  /** The fragment the read state belongs to. */
  const Fragment* fragment_;

  /** 
   * Last investigated tile coordinates. Applicable only to **sparse** fragments
   * for **dense** arrays.
   */
  void* last_tile_coords_;
  /** A buffer for each attribute used by mmap for mapping a tile from disk. */
  std::vector<void*> map_addr_;
  /** The corresponding lengths of the buffers in map_addr_. */
  std::vector<size_t> map_addr_lengths_;
  /** A buffer mapping a compressed tile from disk. */
  void* map_addr_compressed_;
  /** The corresponding length of the map_addr_compressed_ buffer. */
  size_t map_addr_compressed_length_;
  /** 
   * A buffer for each attribute used by mmap for mapping a variable tile from
   * disk. 
   */
  std::vector<void*> map_addr_var_;
  /** The corresponding lengths of the buffers in map_addr_var_. */
  std::vector<size_t> map_addr_var_lengths_;
  /** 
   * The overlap between an MBR and the current tile under investigation
   * in the case of **sparse** fragments in **dense** arrays. The overlap
   * can be one of the following:
   *    - 0: No overlap
   *    - 1: The query subarray fully covers the search tile
   *    - 2: Partial overlap
   *    - 3: Partial overlap contig
   */
  int mbr_tile_overlap_;
  /** Indicates buffer overflow for each attribute. */ 
  std::vector<bool> overflow_;
  /**
   * The type of overlap of the current search tile with the query subarray
   * is full or not. It can be one of the following:
   *    - 0: No overlap
   *    - 1: The query subarray fully covers the search tile
   *    - 2: Partial overlap
   *    - 3: Partial overlap contig
   */
  int search_tile_overlap_;
  /** The overlap between the current search tile and the query subarray. */
  void* search_tile_overlap_subarray_;
  /** The positions of the currently investigated tile. */
  int64_t search_tile_pos_;
  /** 
   * True if the fragment non-empty domain fully covers the subarray area
   * in the current overlapping tile.
   */
  bool subarray_area_covered_;
  /** Internal buffer used in the case of compression. */
  void* tile_compressed_;
  /** Allocated size for internal buffer used in the case of compression. */
  size_t tile_compressed_allocated_size_;
  /** File offset for each attribute tile. */
  std::vector<off_t> tiles_file_offsets_;
  /** File offset for each variable-sized attribute tile. */
  std::vector<off_t> tiles_var_file_offsets_;
  /** 
   * Local tile buffers, one per attribute, plus two for coordinates 
   * (the second one is for searching). 
   */
  std::vector<void*> tiles_;
  /** Current offsets in tiles_ (one per attribute). */
  std::vector<size_t> tiles_offsets_;
  /**
   * The tile position range the search for overlapping tiles with the 
   * subarray query will focus on.
   */
  int64_t tile_search_range_[2];
  /** Sizes of tiles_ (one per attribute). */
  std::vector<size_t> tiles_sizes_;
  /** Local variable tile buffers (one per attribute). */
  std::vector<void*> tiles_var_;
  /** Allocated sizes for the local variable tile buffers. */
  std::vector<size_t> tiles_var_allocated_size_;
  /** Current offsets in tiles_var_ (one per attribute). */
  std::vector<size_t> tiles_var_offsets_;
  /** Sizes of tiles_var_ (one per attribute). */
  std::vector<size_t> tiles_var_sizes_;
  /** Temporary coordinates. */
  void* tmp_coords_;
  /** Temporary offset. */
  size_t tmp_offset_;




  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Helper method to construct filename for given attribute
   */
  std::string construct_filename(int attribute_id, bool is_var=false);

  /**
   * Resets all internal buffers associated with attribute files.
   */
  void reset_file_buffers();

  /**
   * Reads a segment from the file associated with the attribute,
   * given an offset and size.
   * @param attribute_id The id of the attribute.
   * @param is_var Boolean to specify whether the attribute is var.
   * @param offset The offset of the segment to be read from the file.
   * @param segment Pointer to preallocated segment.
   * @param length Length of the preallocated segment.
   * @return TILEDB_RS_OK on success and TILEDB_RS_ERR on error.
   */
  int read_segment(int attribute_id, bool is_var, off_t offset, void *segment, size_t length);

  /**
   * Compares input coordinates to coordinates from the search tile.
   *
   * @param buffer The data buffer to be compared.
   * @param tile_offset The offset in the tile where the data comparison 
   *     starts form.
   * @return 1 if the compared data are equal, 0 if they are not equal and 
   *     TILEDB_RS_ERR for error.
   */
  int CMP_COORDS_TO_SEARCH_TILE(
      const void* buffer,
      size_t tile_offset);

  /**
   * Computes the number of bytes to copy from the local tile buffers of a given
   * attribute in the case of variable-sized cells. It takes as input a range
   * of cells that should be copied ideally if the buffer sizes permit it.
   * Then, based on the available buffer sizes, this range is adjusted and the
   * final bytes to be copied for the two buffers are returned.
   *
   * @param attribute_id The id of the attribute.
   * @param start_cell_pos The starting position of the cell range.
   * @param end_cell_pos The ending position of the cell range. The function may
   *     alter this value upon termination.
   * @param buffer_free_space The free space in the buffer that will store the
   *     starting offsets of the variable-sized cells.
   * @param buffer_var_free_space The free space in the buffer that will store
   *     the actual variable-sized cells.
   * @param bytes_to_copy The returned bytes to copy into the the buffer that
   *     will store the starting offsets of the variable-sized cells.
   * @param bytes_var_to_copy The returned bytes to copy into the the buffer
   *     that will store the actual variable-sized cells.
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int compute_bytes_to_copy(
      int attribute_id,
      int64_t start_cell_pos,
      int64_t& end_cell_pos,
      size_t buffer_free_space, 
      size_t buffer_var_free_space,
      size_t& bytes_to_copy,
      size_t& bytes_var_to_copy);

  /** 
   * Computes the ranges of tile positions that need to be searched for finding
   * overlapping tiles with the query subarray.
   *
   * @return void
   */
  void compute_tile_search_range();

  /** 
   * Computes the ranges of tile positions that need to be searched for finding
   * overlapping tiles with the query subarray.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template<class T>
  void compute_tile_search_range();

  /** 
   * Computes the ranges of tile positions that need to be searched for finding
   * overlapping tiles with the query subarray. This function focuses on the
   * case of column- or row-major cell orders.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template<class T>
  void compute_tile_search_range_col_or_row();

  /** 
   * Computes the ranges of tile positions that need to be searched for finding
   * overlapping tiles with the query subarray. This function focuses on the
   * case of the Hilbert cell order.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template<class T>
  void compute_tile_search_range_hil();

  /**
   * Decompresses a tile.
   * 
   * @param attribute_id The id of the attribute the tile belongs to.
   * @param tile_compressed The compressed tile to be decompressed.
   * @param tile_compressed_size The size of the compressed tile.
   * @param tile The resulting decompressed tile.
   * @param tile_size The expected size of the decompressed tile (for checking 
   *     for errors).
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int decompress_tile(
      int attribute_id,
      unsigned char* tile_compressed,
      size_t tile_compressed_size,
      unsigned char* tile,
      size_t tile_size,
      bool decompress_offsets = false);

  /** 
   * Returns the cell position in the search tile that is after the
   * input coordinates.
   *
   * @tparam T The coordinates type.
   * @param coords The input coordinates.
   * @return The cell position in the search tile that is after the
   *     input coordinates.
   */
  template<class T>
  int64_t get_cell_pos_after(const T* coords);

  /** 
   * Returns the cell position in the search tile that is at or after the
   * input coordinates.
   *
   * @tparam T The coordinates type.
   * @param coords The input coordinates.
   * @return The cell position in the search tile that is at or after the
   *     input coordinates.
   */
  template<class T>
  int64_t get_cell_pos_at_or_after(const T* coords);

  /** 
   * Returns the cell position in the search tile that is at or before the
   * input coordinates.
   *
   * @tparam T The coordinates type.
   * @param coords The input coordinates.
   * @return The cell position in the search tile that is at or before the
   *     input coordinates.
   */
  template<class T>
  int64_t get_cell_pos_at_or_before(const T* coords);

  /**
   * Retrieves the pointer of the i-th coordinates in the search tile.
   *
   * @param i Indicates the i-th coordinates pointer to be retrieved.
   * @param coords The destination pointer.
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int GET_COORDS_PTR_FROM_SEARCH_TILE(
      int64_t i,
      const void*& coords);

  /**
   * Retrieves the pointer of the i-th cell in the offset tile of a
   * variable-sized attribute.
   *
   * @param attribute_id The attribute id.
   * @param i Indicates the i-th offset pointer to be retrieved.
   * @param offset The destination pointer.
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int GET_CELL_PTR_FROM_OFFSET_TILE(
      int attribute_id,
      int64_t i,
      const size_t*& offset);

  /** Returns *true* if the file of the input attribute is empty. */
  bool is_empty_attribute(int attribute_id) const;

  /** 
   * Maps a tile from the disk for an attribute into a local buffer, using 
   * memory map (mmap). This function works with any compression.
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size. 
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int map_tile_from_file_cmp(
      int attribute_id,
      off_t offset,
      size_t tile_size);

  /** 
   * Maps a variable-sized tile from the disk for an attribute into a local 
   * buffer, using memory map (mmap). This function works with any compression.
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size. 
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int map_tile_from_file_var_cmp(
      int attribute_id,
      off_t offset,
      size_t tile_size);

  /** 
   * Maps a tile from the disk for an attribute into a local buffer, using 
   * memory map (mmap). This function focuses on the case of no compression.
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size.
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int map_tile_from_file_cmp_none(
      int attribute_id,
      off_t offset,
      size_t tile_size);

  /** 
   * Maps a variable-sized tile from the disk for an attribute into a local 
   * buffer, using memory map (mmap). This function focuses on the case of
   * no compression.
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size.
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int map_tile_from_file_var_cmp_none(
      int attribute_id,
      off_t offset,
      size_t tile_size);

#ifdef HAVE_MPI
  /** 
   * Reads a tile from the disk for an attribute into a local buffer, using 
   * MPI-IO. This function focuses on the case of GZIP compression.
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size. 
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int mpi_io_read_tile_from_file_cmp(
      int attribute_id,
      off_t offset,
      size_t tile_size);

  /** 
   * Reads a variable-sized tile from the disk for an attribute into a local 
   * buffer, using MPI-IO. This function focuses on the case of any
   * compression.
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size. 
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int mpi_io_read_tile_from_file_var_cmp(
      int attribute_id,
      off_t offset,
      size_t tile_size);
#endif

  /**
   * Prepares a tile from the disk for reading for an attribute.    
   *
   * @param attribute_id The id of the attribute the tile is prepared for. 
   * @param tile_i The tile position on the disk.
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int prepare_tile_for_reading(int attribute_id, int64_t tile_i);

  /**
   * Prepares a variable-sized tile from the disk for reading for an attribute.
   *
   * @param attribute_id The id of the attribute the tile is prepared for. 
   * @param tile_i The tile position on the disk.
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int prepare_tile_for_reading_var(int attribute_id, int64_t tile_i);

  /**
   * Prepares a tile from the disk for reading for an attribute.    
   * This function focuses on the case there is any compression.
   *
   * @param attribute_id The id of the attribute the tile is prepared for. 
   * @param tile_i The tile position on the disk.
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int prepare_tile_for_reading_cmp(int attribute_id, int64_t tile_i);

  /**
   * Prepares a tile from the disk for reading for an attribute.    
   * This function focuses on the case there is no compression.
   *
   * @param attribute_id The id of the attribute the tile is prepared for. 
   * @param tile_i The tile position on the disk.
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int prepare_tile_for_reading_cmp_none(int attribute_id, int64_t tile_i);

  /**
   * Prepares a tile from the disk for reading for an attribute.    
   * This function focuses on the case of variable-sized tiles with any
   * compression.
   *
   * @param attribute_id The id of the attribute the tile is prepared for. 
   * @param tile_i The tile position on the disk.
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int prepare_tile_for_reading_var_cmp(int attribute_id, int64_t tile_i);

  /**
   * Prepares a tile from the disk for reading for an attribute.    
   * This function focuses on the case of variable-sized tiles with no 
   * compression.
   *
   * @param attribute_id The id of the attribute the tile is prepared for. 
   * @param tile_i The tile position on the disk.
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int prepare_tile_for_reading_var_cmp_none(int attribute_id, int64_t tile_i);

  /**
   * Reads data from an attribute tile into an input buffer.
   *
   * @param attribute_id The attribute id.
   * @param buffer The destination buffer.
   * @param tile_offset The offset in the tile where the read starts from.
   * @param bytes_to_copy The number of bytes to copy from the tile into the 
   *     buffer.
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int READ_FROM_TILE(
      int attribute_id,
      void* buffer,
      size_t tile_offset,
      size_t bytes_to_copy);

  /**
   * Reads data from a variable-sized attribute tile into an input buffer.
   *
   * @param attribute_id The attribute id.
   * @param buffer The destination buffer.
   * @param tile_offset The offset in the tile where the read starts from.
   * @param bytes_to_copy The number of bytes to copy from the tile into the 
   *     buffer.
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int READ_FROM_TILE_VAR(
      int attribute_id,
      void* buffer,
      size_t tile_offset,
      size_t bytes_to_copy);

  /** 
   * Reads a tile from the disk for an attribute into a local buffer. This
   * function focuses on the case there is any compression. 
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size. 
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int read_tile_from_file_cmp(
      int attribute_id,
      off_t offset,
      size_t tile_size);

  /** 
   * Reads a tile from the disk for an attribute into a local buffer. This
   * function focuses on the case of variable-sized tiles and any compression. 
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size. 
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int read_tile_from_file_var_cmp(
      int attribute_id,
      off_t offset,
      size_t tile_size);

  /** 
   * Saves in the read state the file offset for an attribute tile.
   * This will be used in subsequent read requests.
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int set_tile_file_offset(
      int attribute_id,
      off_t offset);

  /** 
   * Saves in the read state the file offset for a variable-sized attribute 
   * tile. This will be used in subsequent read requests.
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int set_tile_var_file_offset(
      int attribute_id,
      off_t offset);

  /**
   * Shifts the offsets stored in the tile buffer of the input attribute, such
   * that the first starts from 0 and the rest are relative to the first one.
   *
   * @param attribute_id The id of the attribute the tile corresponds to.
   * @return void.
   */
  void shift_var_offsets(int attribute_id);

  /**
   * Shifts the offsets stored in the input buffer such that they are relative
   * to the input "new_start_offset".
   *
   * @param buffer The input buffer that stores the offsets.
   * @param offset_num The number of offsets in the buffer.
   * @param new_start_offset The new starting offset, i.e., the first element
   *     in the buffer will be equal to this value, and the rest of the offsets
   *     will be shifted relative to this offset.
   * @return void.
   */
  void shift_var_offsets(
      void* buffer, 
      int64_t offset_num, 
      size_t new_start_offset);
};

#endif
