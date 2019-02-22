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
  if (expression_.size() != 0) {
    parser_->EnableOptimizer(true);
    parser_->EnableAutoCreateVar(true);
    // Setup muparserx variables for the attributes
    for (auto i = 0; i < attributes.size(); ++i) {
      add_attribute(attributes[i]);
    }
  }
}

// TODO: Initial support only for attributes of type char/int/float and for 1 value per cell
void Expression::add_attribute(std::string name) {
  int attribute_id = array_schema_->attribute_id(name);
  int attribute_type = array_schema_->type(attribute_id);
  if (array_schema_->cell_val_num(attribute_id) == 1) {
    assert(array_schema_->type_size(attribute_id) == array_schema_->cell_size(attribute_id));
    switch (attribute_type) {
      case TILEDB_CHAR: {
        assert(array_schema_->cell_size(0) == sizeof(char));
        mup::Value x_char(mup::char_type('0'));
        attribute_map_.insert(std::make_pair(name, x_char));
        break;
      }
      case TILEDB_INT32: {
        assert(array_schema_->cell_size(0) == sizeof(int));
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
#ifdef DEBUG
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
#ifdef DEBUG
  mup::var_maptype vmap = parser->GetExprVar();
  std::cout << "Map of all variables used in the expression to the parser" << std::endl;
  for (mup::var_maptype::iterator item = vmap.begin(); item!=vmap.end(); ++item)
    std::cout << item->first << "=" << (mup::Variable&)(*(item->second)) << std::endl;
  std::cout << "Map of all variables used in the expression to the parser - END" << std::endl;
#endif
}

bool drop_cell(mup::Value value) {
  if (value.GetType() == 'b') {
    return !value.GetBool();
  } else {
    return false;
  }
}

int Expression::evaluate(void** buffers, size_t* buffer_sizes) {
  if (expression_.size() == 0 || attributes_.size() == 0 || attribute_map_.size() == 0) {
    // Nothing to do!
    return TILEDB_EXPR_OK;
  }

  // Calculate the number of cells based on the first attribute in the attribute map
  int first_attribute_id = array_schema_->attribute_id(attribute_map_.begin()->first);
  int type_size = array_schema_->type_size(first_attribute_id);
  int cell_size = array_schema_->cell_size(first_attribute_id);
  assert(type_size == cell_size);

  int number_of_cells = buffer_sizes[0]/cell_size;
  
  std::vector<int> cells_to_be_dropped;

  try {
    print_parser_varmap(parser_);
    parser_->SetExpr(expression_);
    print_parser_varmap(parser_);
  } catch (mup::ParserError const &e) {
    EXPRESSION_ERROR("Parser SetExpr error: " + e.GetMsg());
    return TILEDB_EXPR_ERR;
  }
  
  for (int i_cell = 0; i_cell < number_of_cells; i_cell++) {
    for (int i = 0; i < attributes_.size(); ++i) {
      if (attribute_map_.find(attributes_[i]) != attribute_map_.end()) {
        int attribute_id = array_schema_->attribute_id(attributes_[i]);
        int attribute_type = array_schema_->type(attribute_id);
        switch (attribute_type) {
          case TILEDB_CHAR: {
            char val = *(reinterpret_cast<char *>(buffers[i]) + i_cell);
            attribute_map_[attributes_[i]] = val;
            break;
          }
          case TILEDB_INT32: {
            int val = *(reinterpret_cast<int *>(buffers[i]) + i_cell);
            attribute_map_[attributes_[i]] = val;
            break;
          }
          case TILEDB_FLOAT32: {
            float val = *(reinterpret_cast<float *>(buffers[i]) + i_cell);
            attribute_map_[attributes_[i]] = val;
            break;
          }
        }
      }
    }
    
    try {
      mup::Value ret = parser_->Eval();
      // print the result
      mup::console() << "Ret=" << ret << std::endl;
      // drop the cell from buffer if the return type is a boolean
      if (drop_cell(ret)) {
        cells_to_be_dropped.push_back(i_cell);
      }
    } catch (mup::ParserError const &e) {
      EXPRESSION_ERROR("Parser evaluate error, possibly due to bad filter expression: " + "\n\t" + e.GetMsg());
      return TILEDB_EXPR_ERR;
    }
  }

  // TODO: support only for sparse arrays intially.
  if (!array_schema_->dense()) {
    fixup_return_buffers(buffers, buffer_sizes, number_of_cells, cells_to_be_dropped);
  }

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

    for (int i=0, j=0; i < attributes_.size(); i++, j++) {
      int attribute_id = array_schema_->attribute_id(attributes_[i]);
      int cell_size;
      int cell_val_num = array_schema_->cell_val_num(attribute_id);
      if (cell_val_num == TILEDB_VAR_NUM) {
        cell_size = sizeof(size_t);
      } else if (cell_val_num == 0) {
        assert(attributes_[i].compare(TILEDB_COORDS) == 0);
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
