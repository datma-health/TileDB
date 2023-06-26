/**
 * @file   expression.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 * @copyright Copyright (c) 2019, 2022-2023 Omics Data Automation, Inc.
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
class SplitCompare;

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

  bool evaluate_cell(void** buffers, size_t* buffer_sizes, int64_t* positions);

  /**
   * Only used for unit testing.
   */
  int evaluate(void** buffers, size_t* buffer_sizes);

 private:
  void fixup_return_buffers(void** buffers, size_t* buffer_sizes, size_t number_of_cells, std::vector<size_t> cells_to_be_dropped);

  
  std::string expression_;
  std::vector<std::string> attributes_;
  const ArraySchema* array_schema_;

  mup::ParserX *parser_ = new mup::ParserX(mup::pckALL_NON_COMPLEX | mup::pckMATRIX);
  std::map<std::string, mup::Value> attribute_map_;

  int coords_index_ = 0;
  int coords_index_in_buffer_ = 0;

  std::vector<int64_t> last_processed_buffer_index_;

  void assign_single_cell_value(const int attribute_id, void** buffers, const uint64_t buffer_index, const uint64_t position);
  void assign_fixed_cell_values(const int attribute_id, void** buffers, const uint64_t buffer_index, const uint64_t position);
  void assign_var_cell_values(const int attribute_id, void** buffers, size_t *buffer_sizes, const uint64_t buffer_index, const uint64_t position);

  inline const int get_cell_val_num(const std::string& attribute_name) const {
    if (attribute_name == TILEDB_COORDS) {
      return array_schema_->dim_num();
    } else {
      return array_schema_->cell_val_num(array_schema_->attribute_id(attribute_name));
    }
  }

  inline const size_t get_cell_size(const std::string& attribute_name) const {
    int attribute_id = array_schema_->attribute_id(attribute_name);
    int cell_val_num = get_cell_val_num(attribute_name);
    if (cell_val_num == TILEDB_VAR_NUM) {
      return sizeof(size_t);
    } else {
      return array_schema_->type_size(attribute_id)*cell_val_num;
    }
  }

  inline const size_t get_var_cell_type_size(const std::string& attribute_name) const {
    return array_schema_->type_size(array_schema_->attribute_id(attribute_name));
  }

  inline void *offset_pointer(const std::string& attribute_name, const void* src, size_t offset) const {
    switch (get_var_cell_type_size(attribute_name)) {
      case 1:
        return (void *)((uint8_t *)(src) + offset);
      case 2:
        return (void *)((uint16_t *)(src) + offset);
      case 4:
        return (void *)((uint32_t *)(src) + offset);
      case 8:
        return (void *)((uint64_t *)(src) + offset);
      default:
        throw std::range_error("Attribute Type for " + attribute_name + " not supported in expressions");
    }
  }
};

/**
 * SplitCompare accepts 3 arguments
 *       Input attribute name where the attributes is represented internally as string that is a list of strings
 *                separated by some delimiter
 *       Delimiter that is the ASCII integer value, note : muparserx does not accept characters as an arg
 *       Comparison String that is compared with each token from the input string and returns true if there is a match
 *                with any of the tokens, false otherwise.
 */
class SplitCompare : public mup::ICallback {
 public:
  SplitCompare():mup::ICallback(mup::cmFUNC, "splitcompare", 3){}

  void Eval(mup::ptr_val_type &ret, const mup::ptr_val_type *a_pArg, int a_iArgc) {
    mup::string_type input = a_pArg[0]->GetString();
    mup::char_type delimiter = a_pArg[1]->GetInteger();
    mup::string_type with =  a_pArg[2]->GetString();

    // The return type is boolean
    *ret = (mup::bool_type)false;
    std::stringstream ss(input);
    std::string word;
    while (!ss.eof()) {
      std::getline(ss, word, delimiter);
      if (word.compare(with) == 0) {
        *ret = (mup::bool_type)true;
        break;
      }
    }
  }

  const mup::char_type* GetDesc() const {
    return "splitcompare(input, delimiter, compare_string) - splitcompare tokenizes the string for the input attribute using the delimiter(specified as an ASCII integer) and then compares for any token match with the given string";
  }

  mup::IToken* Clone() const {
    return new SplitCompare(*this);
  }
};

/**
 * OprtSplitCompare, similar to SplitCompare above, but using a special operator "|=" where the LHS of the expression is the input
 * attribute name and the RHS is the string to be compared with. The delimiter is "|" for tokenizing the input string.
 */
class OprtSplitCompare : public mup::IOprtBin {
 public:
  OprtSplitCompare() : mup::IOprtBin(("|="), (int)mup::prRELATIONAL1, mup::oaLEFT) {}

  void Eval(mup::ptr_val_type &ret, const mup::ptr_val_type *a_pArg, int) {
    mup::string_type input = a_pArg[0]->GetString();
    mup::char_type delimiter = '|';
    mup::string_type with =  a_pArg[1]->GetString();

    // The return type is boolean
    *ret = (mup::bool_type)false;
    std::stringstream ss(input);
    std::string word;
    while (!ss.eof()) {
      std::getline(ss, word, delimiter);
      if (word.compare(with) == 0) {
        *ret = (mup::bool_type)true;
        break;
      }
    }
  }

  const mup::char_type* GetDesc() const {
    return "<attribute> |= string - the operator tokenizes the attribute using the delimiter '|' and then looks for any token match with string";
  }

  mup::IToken* Clone() const {
    return new OprtSplitCompare(*this);
  }
};

/**
 * OprtSplitCompareAll uses the special operator "&=" where the LHS of the expression is the input attribute name
 * represented internally in TileDB as an array of integers in muparserx and the RHS is the string to be compared with.
 * The with string can optionally have delimiters, currently it should be either "|" or "/".
 */
class OprtSplitCompareAll : public mup::IOprtBin {
 public:
  OprtSplitCompareAll() : mup::IOprtBin(("&="), (int)mup::prRELATIONAL1, mup::oaLEFT) {}

  void Eval(mup::ptr_val_type &ret, const mup::ptr_val_type *a_pArg, int) {
    mup::matrix_type input = a_pArg[0]->GetArray();
    mup::string_type delimiter = "|/";
    mup::string_type with =  a_pArg[1]->GetString();

    with.erase(std::remove(with.begin(), with.end(), delimiter[0]), with.end());
    with.erase(std::remove(with.begin(), with.end(), delimiter[1]), with.end());

    // The return type is boolean
    *ret = (mup::bool_type)false;

    // A vector is represented as a matrix in muparserx with nCols=1
    if (with.length() > 0 && input.GetRows()/2+1u == with.length()) {
      *ret = (mup::bool_type)true;
      for (int i=0, j=0; i<input.GetRows(); i++) {
        if (i%2 == 0) {
          if (input.At(i).GetType() != 'i' || input.At(i).GetInteger() != with[j++]-'0') {
            *ret = (mup::bool_type)false;
            break;
          }
        }
      }
    }
  }

  const mup::char_type* GetDesc() const {
    return "<attribute> &= string - the operator works on a string that is a list of integers with optional delimiters '|' or '/', any delimiters in the string are erased and then compared with the array of integer values for the attribute.";
  }

  mup::IToken* Clone() const {
    return new OprtSplitCompareAll(*this);
  }
};

#endif // __EXPRESSION_H__
