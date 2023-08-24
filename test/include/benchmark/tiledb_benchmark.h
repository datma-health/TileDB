/**
 * @file   tiledb_benchmark.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 Omics Data Automation, Inc.
 * @copyright Copyright (C) 2023 dātma, inc™
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
 * Benchmarking Configuration
 */


#ifndef TILEDB_TESTS_H
#define TILEDB_TESTS_H

#include "array_schema.h"
#include "tiledb.h"
#include "tiledb_constants.h"
#include "tiledb_storage.h"
#include "tiledb_utils.h"
#include "utils.h"

#include <iostream>
#include <fstream>
#include <algorithm>

class BenchmarkConfig: public TempDir {
 public:    
  BenchmarkConfig() {
    do_benchmark_ = is_env_set("TILEDB_BENCHMARK");
    if (!do_benchmark_) {
      return;
    }

    std::ifstream config_stream(g_benchmark_config);
    if (config_stream.is_open()) {
      std::string line;
      while(getline(config_stream, line)) {
        line.erase(std::remove_if(line.begin(), line.end(), isspace),
                   line.end());
        if(line[0] != '#' && !line.empty()) {
          auto found = line.find("=");
          if (found != std::string::npos) {
            auto name = line.substr(0, found);
            auto value = line.substr(found + 1);
            parse(name, value);
          }
        }
      }
      CHECK(attributes_.size() == attribute_cell_val_nums_.size());
      CHECK(attributes_.size() == attribute_data_types_.size());
      // Add data and compression for coords
      attribute_data_types_.push_back(TILEDB_INT64);
      if (compression_.size() == attributes_.size()) {
        compression_.push_back(TILEDB_NO_COMPRESSION); 
      }
    } else {
      throw std::invalid_argument("Could not open benchmark config " + g_benchmark_config + " for reading");
    }
  }

  template <typename T> 
  void* create_buffer(int64_t size, T** buffer, size_t* bytes, bool is_write, bool is_offsets=false) {
    T *typed_buffer= *buffer;
    typed_buffer = new T[size];
    *bytes = (size)*sizeof(T);
    for(int64_t i = 0; is_write && i < size; ++i) {
      if (is_offsets) {
        typed_buffer[i] = (T)i;
      } else {
	// TODO: Make this a template function and configurable via benchmark.config later
	// typed_buffer[i] = (T)(i<<2);
	// typed_buffer[i] = (T)(i%2);
	// typed_buffer[i] = (T)((i%2)-1);
	// typed_buffer[i] = (T)(i%3);
	// typed_buffer[i] = (T)((i%3)-1);
	// typed_buffer[i] = (T)(rand()%2);
	// typed_buffer[i] = (T)(rand()%3);
        typed_buffer[i] = (T)((rand()%3)-1);
      }
    }
    return reinterpret_cast<void *>(typed_buffer);
  }

  void create_buffers(bool is_write) {
    buffers_.clear();
    buffer_sizes_.clear();
    size_t nbytes;
    int num_cells = is_write?num_cells_to_write_:num_cells_to_read_;
    for(auto i=0ul; i<attributes_.size(); i++) {
      if (attribute_cell_val_nums_[i] == TILEDB_VAR_NUM) {
        int64_t* buffer;
        buffers_.push_back(create_buffer(num_cells, &buffer, &nbytes, is_write, /*offsets*/true));
        buffer_sizes_.push_back(nbytes);
      }
      switch (attribute_data_types_[i]) {
        case TILEDB_INT32:
          int32_t* int32_buffer;
          buffers_.push_back(create_buffer(num_cells, &int32_buffer, &nbytes, is_write));
          break;
        case TILEDB_INT64:
          int64_t* int64_buffer;
          buffers_.push_back(create_buffer(num_cells, &int64_buffer, &nbytes, is_write));
          break;
        case TILEDB_CHAR:
          char* char_buffer;
          buffers_.push_back(create_buffer(num_cells, &char_buffer, &nbytes, is_write));
        default:
          break;
      }
      buffer_sizes_.push_back(nbytes);
    }
    int64_t* buffer; // coords
    buffers_.push_back(create_buffer(num_cells*dimensions_.size(), &buffer, &nbytes, is_write));
    buffer_sizes_.push_back(nbytes);
  }

  void free_buffers() {
    for(auto i=0ul, j=0ul; i<attributes_.size(); i++) {
      if (attribute_cell_val_nums_[i] == TILEDB_VAR_NUM) {
	delete [] (int64_t *)buffers_[i+j];
	buffers_[i+j] = NULL;
	j++;
      }
      switch (attribute_data_types_[i]) {
        case TILEDB_INT32:
          delete [] (int32_t *)buffers_[i+j];
          break;
        case TILEDB_INT64:
          delete [] (int64_t *)buffers_[i+j];
          break;
        case TILEDB_CHAR:
          delete [] (char *)buffers_[i+j];
        default:
          break;
      }
      buffers_[i+j] = NULL;
    }
    delete [] (int64_t *)buffers_[buffers_.size()-1]; // coords
    buffers_[buffers_.size()-1] = NULL;
    buffers_.clear();
    buffer_sizes_.clear();
  }

  bool do_benchmark_ = false;
  
  std::string workspace_ = get_temp_dir() + "/WORKSPACE";
  std::vector<std::string> array_names_;
  int io_read_mode_ = TILEDB_IO_MMAP;
  int io_write_mode_ = TILEDB_IO_WRITE;
  int array_read_mode_ = TILEDB_ARRAY_READ;
  int array_write_mode_ = TILEDB_ARRAY_WRITE;
  int fragments_per_array_ = 1;
  std::vector<std::string> attribute_names_;
  std::vector<const char *> attributes_;
  std::vector<int> attribute_data_types_;
  std::vector<int> attribute_cell_val_nums_;
  int num_var_attributes_ = 0;
  bool sparse_ = true;
  std::vector<std::string> dimension_names_;
  std::vector<const char *> dimensions_;
  std::vector<int64_t> domain_;
  std::vector<int> compression_;
  std::vector<int> compression_level_;
  std::vector<int> offsets_compression_;
  std::vector<int> offsets_compression_level_;

  int num_cells_per_tile_ = 0;

  int num_cells_to_write_ = 1024;
  int num_cells_to_read_ = 1024;

  bool human_readable_sizes_ = true;
  bool print_array_schema_ = true;

  std::vector<void *> buffers_;
  std::vector<size_t> buffer_sizes_;

 private:
  void parse(const std::string& name, const std::string& value) {
    if (!name.empty() && !value.empty()) {
      std::stringstream value_stream(value);
      std::string token;
      if (name == "Array_Names") {
        parse_array_names(value);
      } else if (name == "IO_Write_Mode") {
        io_write_mode_ = std::stoi(value);
      } else if (name == "IO_Read_Mode") {
        io_read_mode_ = std::stoi(value);
      } else if (name == "Array_Write_Mode") {
        array_write_mode_ = std::stoi(value);
      } else if (name == "Array_Read_Mode") {
        array_read_mode_ = std::stoi(value);
      } else if (name == "Fragments_Per_Array") {
        fragments_per_array_ = std::stoi(value);
      } else if (name == "Attribute_Names") {
        parse_names(attribute_names_, value);
        transform(attribute_names_, attributes_);
      } else if (name == "Attribute_Data_Types") {
        parse_attribute_data_types(value);
      } else if (name == "Attribute_Cell_Val_Num") {
        parse_attribute_cell_val_num(value);
      } else if (name == "Dimension_Names") {
        parse_names(dimension_names_, value);
        transform(dimension_names_, dimensions_);
      } else if (name == "Domain") {
        parse_domain(value);
      } else if (name == "Compression") {
        parse_compression(compression_, value);
      } else if (name == "Compression_Levels") {
        parse_compression(compression_level_, value);
      } else if (name == "Offsets_Compression") {
        parse_compression(offsets_compression_, value);
      } else if (name == "Offsets_Compression_Levels") {
        parse_compression(offsets_compression_level_, value);
      } else if (name == "Cells_Per_Tile") {
        num_cells_per_tile_ = std::stoi(value);
      } else if (name == "Cells_To_Write") {
        num_cells_to_write_ = std::stoi(value);
      } else if (name == "Cells_To_Read") {
        num_cells_to_read_ = std::stoi(value);
      } else if (name == "Print_Human_Readable_Sizes") {
        human_readable_sizes_ = (std::stoi(value) != 0);
      } else if (name == "Print_Array_Schema") {
	print_array_schema_ = (std::stoi(value) != 0);
      } else {
        std::cerr << "Unrecognized benchmark config name : " << name << std::endl;
      }
    }
  }

  void parse_names(std::vector<std::string>& names, const std::string& value) {
    std::stringstream value_stream(value);
    std::string token;
    while (getline(value_stream, token, ',')) {
      names.push_back(token);
    }
  }

  void transform(std::vector<std::string>& names, std::vector<const char *>& char_names) {
    char_names.resize(names.size());
    for(auto i=0u; i<names.size(); i++) {
      char_names[i] = names[i].c_str();
    }
  }

  void parse_array_names(const std::string& value) {
    std::stringstream value_stream(value);
    std::string token;
    while (getline(value_stream, token, ',')) {
      array_names_.push_back(workspace_+"/"+token);
    }
  }

  void parse_attribute_data_types(const std::string& value) {
    std::stringstream value_stream(value);
    std::string token;
    while (getline(value_stream, token, ',')) {
      if (token == "int32") {
        attribute_data_types_.push_back(TILEDB_INT32);
      } else if (token == "int64") {
        attribute_data_types_.push_back(TILEDB_INT64);
      } else if (token == "char") {
        attribute_data_types_.push_back(TILEDB_CHAR);
      } else {
        throw std::invalid_argument("Invalid Attribute_Data_Type : "+token);
      }
    }
  }

  void parse_attribute_cell_val_num(const std::string& value) {
    std::stringstream value_stream(value);
    std::string token;
    while (getline(value_stream, token, ',')) {
      int cell_val_num = std::stoi(token);
      if (cell_val_num > 0) {
        attribute_cell_val_nums_.push_back(cell_val_num);
      } else {
        attribute_cell_val_nums_.push_back(TILEDB_VAR_NUM);
        num_var_attributes_++;
      }
    }
  }

  void parse_domain(const std::string& value) {
    std::stringstream value_stream(value);
    std::string token;
    while (getline(value_stream, token, ',')) {
      domain_.push_back(std::stoll(token));
    }
    if (dimensions_.size() > 0) {
      CHECK(domain_.size() == dimensions_.size()*2);
    }
  }

  void parse_compression(std::vector<int>& compression, const std::string& value) {
    std::stringstream value_stream(value);
    std::string token;
    while (getline(value_stream, token, ',')) {
      compression.push_back(std::stoi(token));
    }
  }
};

// TileDB  Benchmark Utils
void create_workspace(BenchmarkConfig* config) {
  TileDB_CTX* tiledb_ctx;
  TileDB_Config tiledb_config = {};
  tiledb_config.home_ = config->get_temp_dir().c_str();
  REQUIRE(tiledb_ctx_init(&tiledb_ctx, &tiledb_config) == TILEDB_OK);
  REQUIRE(tiledb_workspace_create(tiledb_ctx, config->workspace_.c_str()) == TILEDB_OK);
  std::cerr << "Workspace: " << config->workspace_ << "\n\n";
  CHECK(tiledb_ctx_finalize(tiledb_ctx) == TILEDB_OK);
}

void delete_arrays(BenchmarkConfig* config) {
  TileDB_CTX* tiledb_ctx;
  TileDB_Config tiledb_config = {};
  tiledb_config.home_ = config->get_temp_dir().c_str();
  REQUIRE(tiledb_ctx_init(&tiledb_ctx, &tiledb_config) == TILEDB_OK);
  for(auto dir : get_dirs(tiledb_ctx, config->workspace_)) {
    std::cerr<<"deleting "<< dir<<"\n";
   delete_dir(tiledb_ctx, dir);
  }
  CHECK(tiledb_ctx_finalize(tiledb_ctx) == TILEDB_OK);
}

void create_arrays(BenchmarkConfig* config, int i) {
  TileDB_ArraySchema array_schema;
  REQUIRE(tiledb_array_set_schema(&array_schema,
                                  config->array_names_[i].c_str(),
                                  config->attributes_.data(),
                                  config->attributes_.size(),
                                  config->num_cells_per_tile_,
                                  TILEDB_COL_MAJOR,
                                  config->attribute_cell_val_nums_.data(),
                                  config->compression_.data(),
                                  config->compression_level_.data(),
                                  config->offsets_compression_.data(),
                                  config->offsets_compression_level_.data(),
                                  0, //sparse array - hardcoded
                                  config->dimensions_.data(),
                                  config->dimensions_.size(),
                                  config->domain_.data(),
                                  config->dimensions_.size()*2*sizeof(int64_t), // size of domain
                                  NULL, // tile extents
                                  0, // tile extents length in bytes
                                  0, // tile order
                                  config->attribute_data_types_.data()) == TILEDB_OK);

  TileDB_CTX* tiledb_ctx;
  TileDB_Config tiledb_config = {};
  tiledb_config.home_ = config->get_temp_dir().c_str();
  REQUIRE(tiledb_ctx_init(&tiledb_ctx, &tiledb_config) == TILEDB_OK);
  REQUIRE(tiledb_array_create(tiledb_ctx, &array_schema) == TILEDB_OK);
  CHECK(tiledb_array_free_schema(&array_schema) == TILEDB_OK);
  CHECK(tiledb_ctx_finalize(tiledb_ctx) == TILEDB_OK);
}

void write_arrays(BenchmarkConfig* config, int i) {
  TileDB_CTX* tiledb_ctx;
  TileDB_Config tiledb_config = {};
  tiledb_config.home_ = config->get_temp_dir().c_str();
  tiledb_config.write_method_ = config->io_write_mode_;
  REQUIRE(tiledb_ctx_init(&tiledb_ctx, &tiledb_config) == TILEDB_OK);
 
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,                       
      &tiledb_array,                    
      config->array_names_[i].c_str(),  
      config->array_write_mode_,
      NULL, // entire domain
      NULL, // all attributes
      0);   // number of attributes   

  REQUIRE(tiledb_array_write(tiledb_array, const_cast<const void **>(config->buffers_.data()), config->buffer_sizes_.data()) == TILEDB_OK);
  CHECK(tiledb_array_finalize(tiledb_array) == TILEDB_OK);
  CHECK(tiledb_ctx_finalize(tiledb_ctx) == TILEDB_OK);
}

void read_arrays(BenchmarkConfig* config, int i) {
  TileDB_CTX* tiledb_ctx;
  TileDB_Config tiledb_config = {};
  tiledb_config.home_ = config->get_temp_dir().c_str();
  tiledb_config.read_method_ = config->io_read_mode_;
  REQUIRE(tiledb_ctx_init(&tiledb_ctx, &tiledb_config) == TILEDB_OK);
 
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,                       
      &tiledb_array,                    
      config->array_names_[i].c_str(),  
      config->array_read_mode_,
      NULL, // entire domain
      NULL, // all attributes
      0);   // number of attributes

  // std::vector<void *> buffers;
  // std::vector<size_t> buffer_sizes;
  // create_buffers(config, buffers, buffer_sizes, false);

  REQUIRE(tiledb_array_read(tiledb_array, config->buffers_.data(), config->buffer_sizes_.data()) == TILEDB_OK);
  CHECK(tiledb_array_finalize(tiledb_array) == TILEDB_OK);
  CHECK(tiledb_ctx_finalize(tiledb_ctx) == TILEDB_OK);
}

// Benchmark Filesystem Stat Utils
std::string get_io_write_mode(int mode) {
  switch(mode) {
    case 0:
      return "TILEDB_IO_WRITE";
    default:
      std::cerr << "TILEDB_IO_MODE=" << std::to_string(mode) << "not recognized\n";
      return "";
  }
}

std::string get_io_read_mode(int mode) {
  switch(mode) {
    case 0:
      return "TILEDB_IO_MMAP";
    case 1:
      return "TILEDB_IO_READ";
    default:
      std::cerr << "TILEDB_IO_MODE=" << std::to_string(mode) << "not recognized\n";
      return "";
  }
}

std::string get_array_mode(int mode) {
  switch(mode) {
    case 0:
      return "TILEDB_ARRAY_READ";
    case 1:
      return "TILEDB_ARRAY_READ_SORTED_COL";
    case 2:
      return "TILEDB_ARRAY_READ_SORTED_ROW";
    case 3:
      return "TILEDB_ARRAY_WRITE";
    case 4:
      return "TILEDB_ARRAY_WRITE_SORTED_COL";
    case 5:
      return "TILEDB_ARRAY_WRITE_SORTED_ROW";
    case 6:
      return  "TILEDB_ARRAY_WRITE_UNSORTED"; 
    default:
      std::cerr << "TILEDB_ARRAY_MODE=" << std::to_string(mode) << "not recognized\n";
      return "";
  }
}

std::string get_name(const std::string& path) {
  auto found = path.rfind("/");
  if (found == std::string::npos) {
    return path;
  } else {
    return path.substr(found+1);
  }
}

std::string readable_size(size_t size, bool human_readable_sizes) {
  if (human_readable_sizes) {
    std::vector<std::string> suffix = { "B", "KB", "MB", "GB", "TB" };
    auto i = 0u;
    while (size > 0) {
      if (i > suffix.size()) break;
      if (size/1024 == 0) {
        return std::to_string(size) + suffix[i]; 
      } else {
        size /= 1024;
        i++;
      }
    }
  }
  return std::to_string(size);
}

std::string get_attribute_info(BenchmarkConfig* config, const std::string& filename) {
  std::string attribute_info;
  if (filename.find(TILEDB_COORDS) != std::string::npos) {
    attribute_info.append(" type=int64 dimensions=" + std::to_string(config->dimensions_.size()));
    return attribute_info;
  } 
  for (auto i=0u; i<config->attributes_.size(); i++) {
    if (filename.find(config->attribute_names_[i]) != std::string::npos) {
      if (config->attribute_data_types_[i] == TILEDB_INT32) {
        attribute_info.append(" type=int32");
      } else if (config->attribute_data_types_[i] == TILEDB_INT64) {
        attribute_info.append(" type=int64");
      } else if (config->attribute_data_types_[i] == TILEDB_CHAR) {
        attribute_info.append(" type=char");
      }
      if (config->attribute_cell_val_nums_[i] == TILEDB_VAR_NUM) {
        if (filename.find("_var.") == std::string::npos) {
          attribute_info.append(" offset_file");
        } else {
          attribute_info.append(" num_cells=var");
        }
      } else {
        attribute_info.append(" num_cells="+std::to_string(config->attribute_cell_val_nums_[i]));
        break;
      }
    }
  }
  return attribute_info;
}

void print_array_schema(TileDB_CTX* tiledb_ctx, const std::string& array) {
  std::string array_schema_filename = array+'/'+TILEDB_ARRAY_SCHEMA_FILENAME;
  size_t buffer_size = file_size(tiledb_ctx, array_schema_filename);
  void *buffer = malloc(buffer_size);
  
  // Load array schema into buffer
  REQUIRE(read_file(tiledb_ctx, array_schema_filename, 0, buffer, buffer_size) == TILEDB_OK);

  // Initialize array schema from buffer
  ArraySchema* array_schema = new ArraySchema(NULL);
  REQUIRE(array_schema->deserialize(buffer, buffer_size) == TILEDB_OK);

  // Print the schema
  array_schema->print();

  delete array_schema;
  free(buffer);

  std::cerr << "\n\n";
}

void print_fragment_sizes(BenchmarkConfig* config, bool human_readable_sizes) {
  TileDB_CTX* tiledb_ctx;
  TileDB_Config tiledb_config = {};
  tiledb_config.home_ = config->get_temp_dir().c_str();
  REQUIRE(tiledb_ctx_init(&tiledb_ctx, &tiledb_config) == TILEDB_OK);

  bool is_printed = false;
  for(auto array : get_dirs(tiledb_ctx, config->workspace_)) {
    if (!is_array(tiledb_ctx, array)) continue;
    std::cerr << "\n\nArray " << get_name(array) << ":\n";
    if (config->print_array_schema_) {
      print_array_schema(tiledb_ctx, array);
    }
    for(auto fragment: get_dirs(tiledb_ctx, array)) {
      if (!is_fragment(tiledb_ctx, fragment)) continue;
      std::cerr << "  Fragment " << get_name(fragment) << ":\n";
      for (auto file: get_files(tiledb_ctx, fragment)) {
        std::string filename = get_name(file); 
        if (filename.compare(TILEDB_FRAGMENT_FILENAME) == 0) continue;
        std::cerr << "    File " << filename << get_attribute_info(config, filename)
                  << " size=" << readable_size(file_size(tiledb_ctx, file), human_readable_sizes) << "\n";
        is_printed = true;
      }
      if (is_printed) break;
      std::cerr << "\n";
    }
    if (is_printed) break;
  }

  CHECK(tiledb_ctx_finalize(tiledb_ctx) == TILEDB_OK);
}

#endif // TILEDB_TESTS_H
