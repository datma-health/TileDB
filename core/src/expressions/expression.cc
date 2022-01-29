/**
 * @file   expression.cc
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
 * This file implements the Expression class.
 */

#include "array_read_state.h"
#include "error.h"
#include "expression.h"
#include "tiledb.h"
#include <algorithm>
#include <typeindex>


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
        if (std::find(attributes.begin(), attributes.end(), item->first) != attributes.end()) {
          add_attribute(item->first);
        } else {
          EXPRESSION_ERROR("Attribute " + item->first + " in expression filter not present in the array schema");
        }
      }
    } catch (mup::ParserError const &e) {
      EXPRESSION_ERROR("Parser SetExpr error: " + e.GetMsg());
    }
    last_processed_buffer_index_.resize(attributes_.size());
  }
}

void Expression::add_attribute(std::string name) {
  int attribute_id = array_schema_->attribute_id(name);
  int attribute_type = array_schema_->type(attribute_id);
  int attribute_cell_val_num = array_schema_->cell_val_num(attribute_id)==TILEDB_VAR_NUM?0:array_schema_->cell_val_num(attribute_id);

  switch (attribute_type) {
    case TILEDB_CHAR: {
      if (attribute_cell_val_num == 1) {
        mup::Value x_int(mup::int_type(""));
        attribute_map_.insert(std::make_pair(name, x_int));
      } else {
        mup::Value x_str(mup::string_type(""));
        attribute_map_.insert(std::make_pair(name, x_str));
      }
      break;
    }
    case TILEDB_INT8: case TILEDB_INT16: case TILEDB_INT32: case TILEDB_INT64:
    case TILEDB_UINT8: case TILEDB_UINT16: case TILEDB_UINT32: case TILEDB_UINT64: {
      if (attribute_cell_val_num == 1) {
        mup::Value x_int(mup::int_type(0));
        attribute_map_.insert(std::make_pair(name, x_int));
      } else {
        mup::Value x_int_array(mup::int_type(attribute_cell_val_num), mup::int_type(0));
        attribute_map_.insert(std::make_pair(name, x_int_array));
      }
      break;
    }
    case TILEDB_FLOAT32: case TILEDB_FLOAT64: {
      if (attribute_cell_val_num == 1) {
        mup::Value x_float(mup::float_type(0.0));
        attribute_map_.insert(std::make_pair(name, x_float));
      } else {
        mup::Value x_float_array(mup::int_type(attribute_cell_val_num), mup::float_type(0.0));
        attribute_map_.insert(std::make_pair(name, x_float_array));
      }
      break;
    }
  }

  parser_->DefineVar(name, (mup::Variable)&(attribute_map_[name]));
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
  T val = *(reinterpret_cast<const T *>(buffer) + buffer_index);
  if ((typeid(T) == typeid(char) && val == TILEDB_EMPTY_CHAR)
      || (typeid(T) == typeid(int) && val == TILEDB_EMPTY_INT32)
      || (typeid(T) == typeid(float) && val == TILEDB_EMPTY_FLOAT32)) {
    throw EmptyValueException();
  }
  return val;
}

inline mup::Value get_single_cell_value(const int attribute_type, void** buffers, size_t *buffer_sizes,
                                        const uint64_t buffer_index, const int64_t position) {
  switch (attribute_type) {
    case TILEDB_CHAR:
      return mup::int_type(get_value<char>(buffers[buffer_index], position));
    case TILEDB_UINT8:
      return mup::int_type(get_value<uint8_t>(buffers[buffer_index], position));
    case TILEDB_UINT16:
      return mup::int_type(get_value<uint16_t>(buffers[buffer_index], position));
    case TILEDB_UINT32:
      return mup::int_type(get_value<uint32_t>(buffers[buffer_index], position));
    case TILEDB_UINT64:
      return mup::int_type(get_value<uint64_t>(buffers[buffer_index], position));
    case TILEDB_INT8:
      return mup::int_type(get_value<int8_t>(buffers[buffer_index], position));
    case TILEDB_INT16:
      return mup::int_type(get_value<int16_t>(buffers[buffer_index], position));
    case TILEDB_INT32:
      return mup::int_type(get_value<int32_t>(buffers[buffer_index], position));
    case TILEDB_INT64:
      return mup::int_type(get_value<int64_t>(buffers[buffer_index], position));
    case TILEDB_FLOAT32:
      return mup::float_type(get_value<float>(buffers[buffer_index], position));
    case TILEDB_FLOAT64:
      return mup::float_type(get_value<double>(buffers[buffer_index], position));
    default:
      std::string msg = "Attribute Type " + std::to_string(attribute_type) + " not supported in expressions";
      throw std::range_error(msg);
  }
}

void Expression::assign_single_cell_value(const int attribute_id, void** buffers, size_t *buffer_sizes,
                                          const uint64_t buffer_index, const int64_t position) {
  auto& attribute_name = array_schema_->attribute(attribute_id);
  attribute_map_[attribute_name] = get_single_cell_value(array_schema_->type(attribute_id), buffers, buffer_sizes,
                                                         buffer_index, position);
}

void Expression::assign_fixed_cell_values(const int attribute_id, void** buffers, size_t *buffer_sizes,
                                          const uint64_t buffer_index, const int64_t position) {
  auto& attribute_name = array_schema_->attribute(attribute_id);
  auto attribute_type = array_schema_->type(attribute_id);
  int num_cells = array_schema_->cell_val_num(attribute_id);
  switch (attribute_type) {
    case TILEDB_CHAR: {
      attribute_map_[attribute_name] = mup::string_type(get_value<char>(buffers[buffer_index], position*num_cells), num_cells);
      break;
    }
    default: {
      auto& x_array = attribute_map_[attribute_name];
      for (auto i=0u; i<num_cells; i++) {
        x_array.At(i) = get_single_cell_value(array_schema_->type(attribute_id), buffers, buffer_sizes,
                                              buffer_index, position*num_cells+i);
      }
    }
  }
}

// returns offset and length for the cell under consideration
template<typename T>
std::pair<size_t, size_t> get_var_cell_info(void** buffers, size_t* buffer_sizes, const uint64_t buffer_index,
                                            const int64_t buffer_position) {
  size_t offset = get_value<size_t>(buffers[buffer_index], buffer_position);
  size_t length;
  if ((buffer_position+1) < (buffer_sizes[buffer_index]/sizeof(size_t))) {
    length = get_value<size_t>(buffers[buffer_index], buffer_position+1)-offset;
  } else {
    length=buffer_sizes[buffer_index+1]-offset;
  }
  return std::make_pair(offset, length/sizeof(T));
}

void Expression::assign_var_cell_values(const int attribute_id, void** buffers, size_t *buffer_sizes,
                                        const uint64_t buffer_index, const int64_t position) {
  auto& attribute_name = array_schema_->attribute(attribute_id);
  auto attribute_type = array_schema_->type(attribute_id);
  switch (attribute_type) {
    case TILEDB_CHAR: {
       std::pair<size_t, size_t> info = get_var_cell_info<char>(buffers, buffer_sizes, buffer_index, position);
       attribute_map_[attribute_name] =
           mup::string_type(std::string((char *)buffers[buffer_index+1]+info.first, info.second));
       break;
    }
    default: {
      parser_->RemoveVar(attribute_name);
      std::pair<size_t, size_t> info = get_var_cell_info<int>(buffers, buffer_sizes, buffer_index, position);
      mup::Value x_array(mup::int_type(info.first), mup::int_type(info.second));
      for (auto i=0u; i<info.second; i++) {
        x_array.At(i) = get_single_cell_value(attribute_type, buffers, buffer_sizes, buffer_index, info.first+i);
      }
      parser_->DefineVar(attribute_name, (mup::Variable)&x_array);
      break;
    }
  }
}

bool Expression::evaluate_cell(void** buffers, size_t* buffer_sizes, std::vector<int64_t>& positions) {
  if (expression_.size() == 0 || attributes_.size() == 0 || attribute_map_.size() == 0) {
    return true;
  }

  for (auto i = 0u, j = 0u; i < attributes_.size(); i++, j++) {
    int attribute_id = array_schema_->attribute_id(attributes_[i]);
    if (attribute_map_.find(attributes_[i]) != attribute_map_.end()) {
      try {
        switch (array_schema_->cell_val_num(attribute_id)) {
          case 1:
            assign_single_cell_value(attribute_id, buffers, buffer_sizes, j, positions[i]);
            break;
          case TILEDB_VAR_NUM :
            assign_var_cell_values(attribute_id, buffers, buffer_sizes, j, positions[i]);
            break;
          default:
            assign_fixed_cell_values(attribute_id, buffers, buffer_sizes, j, positions[i]);
        }
      } catch (EmptyValueException& e) {
        return true; // TODO: Filter expressions do not handle empty values yet.
      }
    }

    // Increment buffer index for variable types
    if (array_schema_->cell_size(attribute_id) == TILEDB_VAR_SIZE) {
      j++;
    }
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
  size_t number_of_cells = 0;
  for (auto i = 0u, j = 0u; i < attributes_.size(); i++, j++) {
    int attribute_id = array_schema_->attribute_id(attributes_[i]);
    size_t ncells = 0;
    if (buffer_sizes[j] != 0) {
      ncells = get_num_cells(array_schema_, attribute_id, buffer_sizes, j);
      last_processed_buffer_index_[i] = 0;
    }
    number_of_cells = (!number_of_cells || ncells < number_of_cells)?ncells:number_of_cells;

    // Increment buffer index for variable types
    if (array_schema_->cell_size(attribute_id) == TILEDB_VAR_SIZE) j++;
  }

  if (number_of_cells == 0) {
    return TILEDB_EXPR_OK;
  }
  
  std::vector<size_t> cells_to_be_dropped;

  print_parser_varmap(parser_);
  print_parser_expr_varmap(parser_);

  for (auto i_cell = 0u; i_cell < number_of_cells; i_cell++) {
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

  if (cells_to_be_dropped.size()) {
    fixup_return_buffers(buffers, buffer_sizes, number_of_cells, cells_to_be_dropped);
  }

  return TILEDB_EXPR_OK;
}

void Expression::fixup_return_buffers(void** buffers, size_t* buffer_sizes, size_t number_of_cells, std::vector<size_t> cells_to_be_dropped) {
  std::map<int, size_t> adjust_offsets;
  std::vector<size_t> num_cells(attributes_.size());

  // Initialize num_cells for all attributes as the buffer_sizes are being updated in place
  for (auto i=0u, j=0u; i < attributes_.size(); i++, j++) {
    num_cells[i] = buffer_sizes[j]/get_cell_size(attributes_[i]);
    if (get_cell_val_num(attributes_[i])  == TILEDB_VAR_NUM) j++;
  }

  auto max_num_cells = std::max_element(num_cells.begin(), num_cells.end());
  for (int current_cell=0, next_cell=0; next_cell < *max_num_cells; current_cell++, next_cell++) {
    size_t reduce_by = 0;
    bool next_cell_dropped = false;
    do {
      next_cell_dropped = std::find(cells_to_be_dropped.begin(), cells_to_be_dropped.end(), next_cell) != cells_to_be_dropped.end();
      if (next_cell_dropped && next_cell < number_of_cells) {
        reduce_by++;
        next_cell++;
      }
    } while (next_cell_dropped);

    for (auto i=0u, j=0u; i < attributes_.size(); i++, j++) {
      int cell_val_num = get_cell_val_num(attributes_[i]);
      size_t cell_size = get_cell_size(attributes_[i]);

      if (current_cell !=  next_cell && next_cell < num_cells[i]) {
        void *next = static_cast<char *>(buffers[j])+cell_size*next_cell;
        void *current = static_cast<char *>(buffers[j])+cell_size*current_cell;

        if (cell_val_num == TILEDB_VAR_NUM) {
          if (adjust_offsets.find(j) == adjust_offsets.end()) adjust_offsets[j] = 0;

          size_t next_length = 0;
          if ((next_cell+1) < num_cells[i]) {
            next_length = *(reinterpret_cast<size_t *>(next)+1) -  *(reinterpret_cast<size_t *>(next));
          } else {
            next_length = buffer_sizes[j+1] -  *(reinterpret_cast<size_t *>(next));
          }

          void *var_next = static_cast<char *>(buffers[j+1]) + *(reinterpret_cast<size_t *>(next));
          void *var_current =  static_cast<char *>(buffers[j+1]) + adjust_offsets[j];
          // Shift cell contents into (dropped)current contents
          memmove(var_current, var_next, next_length*get_var_cell_type_size(attributes_[i]));
          // Adjust the current cell's offsets
          memmove(current, &adjust_offsets[j], sizeof(size_t));
          adjust_offsets[j] += next_length;
        } else {
          // Move next cell contents into current contents
          memmove(current, next, cell_size);
        }
      }

      if (reduce_by) buffer_sizes[j] -= reduce_by*cell_size;

      if (cell_val_num == TILEDB_VAR_NUM) j++;
    }
  }

  // Fixup buffer_sizes for attributes with variable cell size
  for (auto i=0u, j=0u; i < attributes_.size(); i++, j++) {
    if (get_cell_val_num(attributes_[i])  == TILEDB_VAR_NUM) {
      buffer_sizes[j+1] = adjust_offsets[j];
      j++;
    }
  }
}
