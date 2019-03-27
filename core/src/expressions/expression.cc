/**
 * @file   expression.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file implements the Expression class.
 */

#include "array_read_state.h"
#include "error.h"
#include "expression.h"
#include "tiledb.h"
#include <algorithm>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define EXPRESSION_ERROR(MSG) TILEDB_ERROR(TILEDB_EXPR_ERRMSG, MSG, tiledb_expr_errmsg)

/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_expr_errmsg = "";

Expression::Expression(std::string expression, std::vector<std::string> attributes,
                       const ArraySchema *array_schema) :
    expression_(expression), attributes_(attributes), array_schema_(array_schema) {
  if (array_schema->dense()) {
    EXPRESSION_ERROR("Expression parsing for dense arrays not yet implemented");
  } else if (expression_.size() != 0) {
    parser_->EnableOptimizer(true);

    // Setup muparserx variables for the attributes
    try {
      parser_->SetExpr(expression_);
      // Get map of all variables used in the expression
      mup::var_maptype vmap = parser_->GetExprVar();
      for (mup::var_maptype::iterator item = vmap.begin(); item!=vmap.end(); ++item) {
        add_attribute(item->first);
      }
    } catch (mup::ParserError const &e) {
      EXPRESSION_ERROR("Parser SetExpr error: " + e.GetMsg());
    }

    last_processed_buffer_index_.resize(attributes_.size());
  }
}

// TODO: Initial support only for attributes of type char/int/float and for 1 value per cell per attribute
void Expression::add_attribute(std::string name) {
  int attribute_id = array_schema_->attribute_id(name);
  int attribute_type = array_schema_->type(attribute_id);

  if (array_schema_->cell_val_num(attribute_id) == 1 && array_schema_->type_size(attribute_id) == array_schema_->cell_size(attribute_id)) {
    switch (attribute_type) {
      case TILEDB_CHAR: {
        mup::Value x_char(mup::char_type('0'));
        attribute_map_.insert(std::make_pair(name, x_char));
        break;
      }
      case TILEDB_INT32: {
        mup::Value x_int(mup::int_type(0));
        attribute_map_.insert(std::make_pair(name, x_int));
        break;
      }
      case TILEDB_FLOAT32: {
        mup::Value x_float(mup::float_type(0.0));
        attribute_map_.insert(std::make_pair(name, x_float));
        break;
      }
    }

    parser_->DefineVar(name, (mup::Variable)&(attribute_map_[name]));
  }
}

// Get a map of all variables used by muParserX
void print_parser_varmap(mup::ParserX *parser) {
#if 0
  mup::var_maptype vmap = parser->GetVar();
  std::cout << "Map of all variables used by parser" << std::endl;
  for (mup::var_maptype::iterator item = vmap.begin(); item!=vmap.end(); ++item)
    std::cout << item->first << "=" << (mup::Variable&)(*(item->second)) << std::endl;
  std::cout << "Map of all variables used by parser - END" << std::endl;
#endif
}

// Get a map of all the variables used in the expression
void print_parser_expr_varmap(mup::ParserX *parser)
{
#if 0
  mup::var_maptype vmap = parser->GetExprVar();
  std::cout << "Map of all variables used in the expression to the parser" << std::endl;
  for (mup::var_maptype::iterator item = vmap.begin(); item!=vmap.end(); ++item)
    std::cout << item->first << "=" << (mup::Variable&)(*(item->second)) << std::endl;
  std::cout << "Map of all variables used in the expression to the parser - END" << std::endl;
#endif
}

struct EmptyValueException : public std::exception {
  const char* what () const throw () {
    return "Empty Value Exception";
  }
};

template<typename T>
T get_value(const void *buffer, const uint64_t buffer_index) {
  if (typeid(T) != typeid(char) && typeid(T) != typeid(int) && typeid(T) == typeid(float)) {
    throw EmptyValueException();
  }
  return *(reinterpret_cast<const T *>(buffer) + buffer_index);;
}

bool Expression::evaluate_cell(void** buffers, size_t* buffer_sizes, std::vector<int64_t>& buffer_indexes) {
  if (expression_.size() == 0 || attributes_.size() == 0 || attribute_map_.size() == 0) {
    return true;
  }

  for (auto i = 0u, j = 0u; i < attributes_.size(); i++, j++) {
    int attribute_id = array_schema_->attribute_id(attributes_[i]);
    if (attribute_map_.find(attributes_[i]) != attribute_map_.end()) {
      int attribute_type = array_schema_->type(attribute_id);
      try {
        switch (attribute_type) {
          case TILEDB_CHAR:
            attribute_map_[attributes_[i]] = get_value<char>(buffers[j], buffer_indexes[i]);
            break;
          case TILEDB_INT32:
            attribute_map_[attributes_[i]] = get_value<int>(buffers[j], buffer_indexes[i]);
            break;
          case TILEDB_FLOAT32: {
            attribute_map_[attributes_[i]] = get_value<float>(buffers[j], buffer_indexes[i]);
            break;
          }
        }
      } catch (EmptyValueException& e) {
        return true; // TODO: Filter expressions do not handle empty values yet.
      }
    }

    // Increment buffer index for variable types
    if (array_schema_->cell_size(attribute_id) == TILEDB_VAR_SIZE) j++;
  }

  bool keep_cell = true;
  
  try {
    mup::Value value = parser_->Eval();
#if 0
    // print the result
    mup::console() << "Ret=" << value << std::endl;
#endif
    // TODO: Only supports expressions evaluating to booleans for now.
    if (value.GetType() == 'b') {
      keep_cell = value.GetBool();
    }
  } catch (mup::ParserError const &e) {
    EXPRESSION_ERROR("Parser evaluate error, possibly due to bad filter expression: " + "\n\t" + e.GetMsg());
    throw e;
  }

  return keep_cell;
}

int get_num_cells(const ArraySchema *array_schema, int attribute_id, size_t* buffer_sizes, int buffer_index) {
  if (array_schema->cell_size(attribute_id) == TILEDB_VAR_SIZE) {
    return buffer_sizes[buffer_index]/TILEDB_CELL_VAR_OFFSET_SIZE;
  } else {
    return buffer_sizes[buffer_index]/array_schema->cell_size(attribute_id);
  }
}

int Expression::evaluate(void** buffers, size_t* buffer_sizes) {
  if (expression_.size() == 0 || attributes_.size() == 0 || attribute_map_.size() == 0) {
    return TILEDB_EXPR_OK;
  }

  // Get minimum number of cells in buffers for evaluation to account for overflow.
  int number_of_cells = 0;
  for (auto i = 0u, j = 0u; i < attributes_.size(); i++, j++) {
    int attribute_id = array_schema_->attribute_id(attributes_[i]);
    int ncells = 0;
    if (buffer_sizes[j] != 0) {
      ncells = get_num_cells(array_schema_, attribute_id, buffer_sizes, j);
    }
    number_of_cells = (!number_of_cells || ncells < number_of_cells)?ncells:number_of_cells;
    last_processed_buffer_index_[i] = 0;

    // Increment buffer index for variable types
    if (array_schema_->cell_size(attribute_id) == TILEDB_VAR_SIZE) j++;
  }

  if (number_of_cells == 0) {
    return TILEDB_EXPR_OK;
  }
  
  std::vector<int> cells_to_be_dropped;

  print_parser_varmap(parser_);
  print_parser_expr_varmap(parser_);

  for (int i_cell = 0; i_cell < number_of_cells; i_cell++) {
    try {
      if (!evaluate_cell(buffers, buffer_sizes, last_processed_buffer_index_)) {
        cells_to_be_dropped.push_back(i_cell);
      }
    } catch (mup::ParserError const &e) {
      return TILEDB_EXPR_ERR;
    }
    
    for (auto i = 0u; i<attributes_.size(); i++) {
      last_processed_buffer_index_[i]++;
    }
  }

  fixup_return_buffers(buffers, buffer_sizes, number_of_cells, cells_to_be_dropped);

  return TILEDB_EXPR_OK;
}

void Expression::fixup_return_buffers(void** buffers, size_t* buffer_sizes, int number_of_cells, std::vector<int> cells_to_be_dropped) {
  for (int current_cell=0, next_cell=0; next_cell < number_of_cells; current_cell++, next_cell++) {
    int reduce_by = 0;
    bool next_cell_dropped;
    do {
      next_cell_dropped = std::find(cells_to_be_dropped.begin(), cells_to_be_dropped.end(), next_cell) != cells_to_be_dropped.end();
      if (next_cell_dropped && next_cell < number_of_cells) {
        reduce_by++;
        next_cell++;
      }
    } while (next_cell_dropped);

    for (auto i=0u, j=0u; i < attributes_.size(); i++, j++) {
      int attribute_id = array_schema_->attribute_id(attributes_[i]);
      int cell_size;
      int cell_val_num = array_schema_->cell_val_num(attribute_id);
      if (cell_val_num == TILEDB_VAR_NUM) {
        cell_size = sizeof(size_t);
      } else if (attributes_[i].compare(TILEDB_COORDS) == 0) {
        cell_size =  array_schema_->type_size(attribute_id)*array_schema_->dim_num();
      } else {
        cell_size = array_schema_->type_size(attribute_id)*cell_val_num;
      }

      if (current_cell !=  next_cell) {
        // Move next cell contents into current contents
        void *src = static_cast<char *>(buffers[j])+cell_size*next_cell;
        void *dest = static_cast<char *>(buffers[j])+cell_size*current_cell;
        memmove(dest, src, cell_size);
      }

      buffer_sizes[j] -= reduce_by*cell_size;
      
      // TODO: Ignoring variable types for now
      if (cell_val_num == TILEDB_VAR_NUM) j++;
    }
  }  
}
