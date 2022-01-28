/**
 * @file   expression.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 * @copyright Copyright (c) 2019, 2022 Omics Data Automation, Inc.
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
 * This file defines the class Expression to store and process filters 
 */

#ifndef __EXPRESSION_H__
#define __EXPRESSION_H__

#include "mpParser.h"
#include "array_schema.h"
#include <typeinfo>

/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Stores potential error messages. */
extern std::string tiledb_expr_errmsg;


/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_EXPR_OK          0
#define TILEDB_EXPR_ERR        -1
/**@}*/

/** Default error message. */
#define TILEDB_EXPR_ERRMSG std::string("[TileDB::Expression] Error: ")

//Forward declaration
class ArrayReadState;

class Expression {
 public:

  /* ********************************* */
  /*            MUTATORS               */
  /* ********************************* */
  Expression();

  Expression(std::string expression, std::vector<std::string> attribute_vec,
             const ArraySchema* array_schema);

  ~Expression() {
    delete parser_;
  }

  void set_array_schema(const ArraySchema *array_schema);

  void add_attribute(std::string name);

  int add_expression(std::string expression);

  bool evaluate_cell(void **buffers, size_t* buffer_sizes, std::vector<int64_t>& positions);

  /**
   * The evaluate method is called after array read is done.
   * FIXME: This is extremely inefficient and only works
   * for the POC on filters. The idea is to change this
   * quickly with on-disk secondary indexing of attributes
   * which have been annotated to be indexed
   */
  int evaluate(void** buffers, size_t* buffer_sizes);

 private:
  void fixup_return_buffers(void** buffers, size_t* buffer_sizes, int number_of_cells, std::vector<int> cells_to_be_dropped);
  
  std::string expression_;
  std::vector<std::string> attributes_;
  const ArraySchema* array_schema_;

  mup::ParserX *parser_ = new mup::ParserX(mup::pckALL_NON_COMPLEX | mup::pckMATRIX);
  std::map<std::string, mup::Value> attribute_map_;

  int coords_index_ = 0;
  int coords_index_in_buffer_ = 0;

  std::vector<int64_t> last_processed_buffer_index_;

  void assign_single_cell_value(const int attribute_id, void** buffers, size_t *buffer_sizes,
                                const uint64_t buffer_index, const int64_t position);
  void assign_fixed_cell_values(const int attribute_id, void** buffers, size_t *buffer_sizes,
                                const uint64_t buffer_index, const int64_t position);
  void assign_var_cell_values(const int attribute_id, void** buffers, size_t *buffer_sizes,
                                const uint64_t buffer_index, const int64_t position);

  inline const int get_cell_val_num(const std::string& attribute_name) const {
    return array_schema_->cell_val_num(array_schema_->attribute_id(attribute_name));
  }

  inline const size_t get_cell_size(const std::string& attribute_name) const {
    int attribute_id = array_schema_->attribute_id(attribute_name);
    int cell_val_num = get_cell_val_num(attribute_name);
    if (cell_val_num == TILEDB_VAR_NUM) {
      return sizeof(size_t);
    } else if (attribute_name.compare(TILEDB_COORDS) == 0) {
      return array_schema_->type_size(attribute_id)*array_schema_->dim_num();
    } else {
      return array_schema_->type_size(attribute_id)*cell_val_num;
    }
  }

  inline const size_t get_var_cell_type_size(const std::string& attribute_name) const {
    return array_schema_->type_size(array_schema_->attribute_id(attribute_name));
  }
};

#endif // __EXPRESSION_H__
