/**
 * @file   expression.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 * @copyright Copyright (c) 2019, 2022-2023 Omics Data Automation, Inc.
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
  Expression(std::string expression) : expression_(expression) {};

  ~Expression() {
    delete parser_;
  }

  int init(const std::vector<int>& attribute_ids, const ArraySchema* array_schema);

  void add_attribute(std::string name);

  int add_expression(std::string expression);

  int evaluate_cell(void **buffers, size_t* buffer_sizes, std::vector<int64_t>& positions);

  int evaluate_cell(void** buffers, size_t* buffer_sizes, int64_t* positions);

  /**
   * Only used for unit testing.
   */
  int evaluate(void** buffers, size_t* buffer_sizes);

 private:
  void fixup_return_buffers(void** buffers, size_t* buffer_sizes, size_t number_of_cells, std::vector<size_t> cells_to_be_dropped);

  
  std::string expression_;
  std::vector<std::string> attributes_;
  const ArraySchema* array_schema_;
  bool is_initialized = false;

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

#define PIPED_SEP '|'
#define SLASHED_SEP '/'

/**
 * Resolve accepts 3 arguments
 *       Input attribute name where the attributes are represented internally as an array of integers separated
 *               by a delimiter. Each input integer is compared against the input comparison strings.
 *       First Comparison String which is compared with input integer if -eq 0
 *       Second Comparison String which is a delimited string, with the delimited position specified by the input
                 integer if -gt 0
 * Returns string of the same length as input attribute substituted with their character representations surmised
 *       from the comparison strings
 */
class Resolve : public mup::ICallback {
 public:
  Resolve():mup::ICallback(mup::cmFUNC, "resolve", 3){}

  void Eval(mup::ptr_val_type &ret, const mup::ptr_val_type *a_pArg, int a_iArgc) {
    mup::matrix_type input = a_pArg[0]->GetArray();
    mup::string_type cmp_str1 = a_pArg[1]->GetString();
    mup::string_type cmp_str2 =  a_pArg[2]->GetString();

    // The return type is string
    mup::string_type ret_val;
    // A vector is represented as a matrix in muparserx with nCols=1
    for (int i=0; i<input.GetRows(); i++) {
      if (input.At(i).GetType() == 'i') {
        auto val = input.At(i).GetInteger();
        if (i%2) { // Phase
          if (val) ret_val += PIPED_SEP; else ret_val += SLASHED_SEP;
        } else if (val) { // ALT
          ret_val += get_segment(cmp_str2, val);
        } else { // REF
          ret_val += cmp_str1;
        }
      }
    }
    *ret = ret_val;
  }

  const mup::char_type* GetDesc() const {
    return  "resolve(input, compare_string1, compare_string2) - the function works on a list of integers with optional delimiters '|' or '/' and compares against compare_string1 if the integer is 0 and with compare_string2 otherwise";
  }

  mup::IToken* Clone() const {
    return new Resolve(*this);
  }

 private:
  std::string_view get_segment(std::string_view str, int segment_pos) {
    std::string::size_type pos;
    std::string::size_type next_pos = -1;
    for (int j=0; j<segment_pos; j++) {
      pos = next_pos + 1;
      next_pos = str.find(PIPED_SEP);
      if (next_pos == std::string::npos) {
        assert(j == segment_pos-1);
        return str.substr(pos, str.length());
      }
    }
    return str.substr(pos, next_pos);
  }
};

/**
 * OprtCompareAll uses the special operator "&=" where the LHS of the expression is a string, possibly the result of
 * running resolve() and the RHS is the string to be compared with. The RHS string is first compared simply and then
 * checked for all available segments in the LHS if the delimiter '/' instead of '|' is used. In the case of no
 * delimiter in the RHS, any match from the LHS will return true.
 */
class OprtCompareAll : public mup::IOprtBin {
 public:
  OprtCompareAll() : mup::IOprtBin(("&="), (int)mup::prRELATIONAL1, mup::oaLEFT) {}

  void Eval(mup::ptr_val_type &ret, const mup::ptr_val_type *a_pArg, int) {
    mup::string_type input = a_pArg[0]->GetString();
    mup::string_type with =  a_pArg[1]->GetString();

    // The return type is boolean
    *ret = (mup::bool_type)false;
    if (with.length() > 0) {
      if (input == with) {
        *ret = (mup::bool_type)true;
      } else {
        auto delimit = delimiter(with);
        if (delimit == SLASHED_SEP) {
          *ret = (mup::bool_type)match_noorder(input, with);
        } else if (delimit == 0) {
          *ret = (mup::bool_type)match_any(input, with);
        }
      }
    } else {
      std::string errmsg = "LHS="+input+" RHS=EMPTY";
      throw mup::ParserError(errmsg, mup::ecUNEXPECTED_VAL);
    }
  }

  const mup::char_type* GetDesc() const {
    return "string1 &= string2 - the operator works on strings that are composed of segments with optional delimiters '|' or '/'. ";
  }

  mup::IToken* Clone() const {
    return new OprtCompareAll(*this);
  }

 private:
  char delimiter(const std::string& str) {
    if (str.find(PIPED_SEP) != std::string::npos) {
      return PIPED_SEP;
    } else if (str.find(SLASHED_SEP) != std::string::npos) {
      return SLASHED_SEP;
    } else {
      return 0;
    }
  }

  bool match_noorder(std::string_view input, std::string_view with) {
    std::vector<std::string_view> vals;
    std::string::size_type pos = 0, next_pos;
    while ((next_pos = next(with, pos)) != std::string::npos) {
      vals.push_back(with.substr(pos, next_pos));
      pos = next_pos+1;
    }
    vals.push_back(with.substr(pos));

    pos = 0;
    while ((next_pos = next(input, pos)) != std::string::npos) {
      auto found = std::find(vals.begin(), vals.end(), input.substr(pos, next_pos));
      if (found != vals.end()) {
        vals.erase(found);
      } else {
        return false;
      }
      pos = next_pos+1;
    }
    return (vals.size() == 1) && input.substr(pos) == vals[0];
  }

  bool match_any(std::string_view input, std::string_view with) {
    std::string::size_type pos = 0, next_pos;
    while ((next_pos = next(input, pos)) != std::string::npos) {
      if (input.substr(pos, next_pos) == with) {
        return true;
      }
      pos = next_pos+1;
    }
    return input.substr(pos) == with;
  }

  std::string::size_type next(std::string_view str,  std::string::size_type pos) {
    auto piped_pos = str.find(PIPED_SEP, pos);
    auto slashed_pos = str.find(SLASHED_SEP, pos);
    if (piped_pos < slashed_pos) {
      return piped_pos;
    } else {
      return slashed_pos;
    }
  }
};

/**
 * IsHomRef accepts 1 argument
 *       Input attribute name where the attributes are represented internally as an array of integers separated
 *               by a delimiter. Each input integer is compared against the input comparison strings.
 * Returns true if all the integers ignoring delimiters are zero.
 */
class IsHomRef : public mup::ICallback {
 public:
  IsHomRef():mup::ICallback(mup::cmFUNC, "ishomref", 1){}

  void Eval(mup::ptr_val_type &ret, const mup::ptr_val_type *a_pArg, int a_iArgc) {
     mup::matrix_type input = a_pArg[0]->GetArray();

    // The return type is boolean
    // A vector is represented as a matrix in muparserx with nCols=1
    for (int i=0; i<input.GetRows(); i+=2) {
      auto val = input.At(i).GetInteger();
      if (val!=0) {
        *ret = (mup::bool_type)false;
        return; //false
      }
    }
    *ret = (mup::bool_type)true;
  }

  const mup::char_type* GetDesc() const {
    return  "ishomref(input) 0 - the function takes a list of integers and compares them to zero, ignoring delimiter positions";
  }

  mup::IToken* Clone() const {
    return new IsHomRef(*this);
  }
};

/**
 * IsHomAlt accepts 1 argument
 *       Input attribute name where the attributes are represented internally as an array of integers separated
 *               by a delimiter. Each input integer is compared against the input comparison strings.
 * Returns true if all the integers ignoring delimiters are the same value.
 */
class IsHomAlt : public mup::ICallback {
 public:
  IsHomAlt():mup::ICallback(mup::cmFUNC, "ishomalt", 1){}

  void Eval(mup::ptr_val_type &ret, const mup::ptr_val_type *a_pArg, int a_iArgc) {
    mup::matrix_type input = a_pArg[0]->GetArray();

    // The return type is boolean
    // A vector is represented as a matrix in muparserx with nCols=1
    mup::int_type first_val = 0;
    for (int i=0; i<input.GetRows(); i+=2) {
      auto val = input.At(i).GetInteger();
      if (val == 0) {
        *ret = (mup::bool_type)false;
        return;
      } else if (i==0) {
        first_val = val;
      } else if (val!=first_val) {
        *ret = (mup::bool_type)false;
        return;
      }
    }
    *ret = (mup::bool_type)true;
  }

  const mup::char_type* GetDesc() const {
    return  "ishomalt(input) 0 - the function takes a list of integers, ignoring even positions, checks if they are all the same value and greater than zero";
  }

  mup::IToken* Clone() const {
    return new IsHomAlt(*this);
  }
};

/**
 * IsHet accepts 1 argument
 *       Input attribute name where the attributes are represented internally as an array of integers separated
 *               by a delimiter. Each input integer is compared against the input comparison strings.
 * Returns true if all the integers ignoring delimiters are not *unknown* and not the same(IsHomRef/IsHomAlt).
 */
class IsHet : public mup::ICallback {
 public:
  IsHet():mup::ICallback(mup::cmFUNC, "ishet", 1){}

  void Eval(mup::ptr_val_type &ret, const mup::ptr_val_type *a_pArg, int a_iArgc) {
    mup::matrix_type input = a_pArg[0]->GetArray();

    // The return type is boolean
    *ret = (mup::bool_type)false;
    // input.GetRows() is zero for *unknown* values
    if (input.GetRows() > 0) {
      mup::int_type first_val = 0;
      for (int i=0; i<input.GetRows(); i+=2) {
        auto val = input.At(i).GetInteger();
        if (i==0) {
          first_val = val;
        } else if (val!=first_val) {
          *ret = (mup::bool_type)true;
          return;
        }
      }
    }
  }

  const mup::char_type* GetDesc() const {
    return  "ishet(input) 0 - the function takes a list of integers, ignoring even positions, checks if the values are not unknown or HomRef or HomAlt";
  }

  mup::IToken* Clone() const {
    return new IsHet(*this);
  }
};

#endif // __EXPRESSION_H__
