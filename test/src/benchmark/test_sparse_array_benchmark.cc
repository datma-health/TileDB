/**
 * @file   test_sparse_array.cc
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
 * Benchmark Sparse Arrays
 */

#define CATCH_CONFIG_ENABLE_BENCHMARKING
#include "catch.h"

#include "tiledb_benchmark.h"

#include <iostream>
#include <string>
#include <thread>
#include <utility>


TEST_CASE_METHOD(BenchmarkConfig, "Benchmark sparse array", "[benchmark_sparse]") {
  if (!do_benchmark_) {
    return;
  }

  create_workspace(this);

  // Create Arrays
  std::cout << "Number of arrays to create=" << std::to_string(array_names_.size()) << std::endl;
  Catch::Timer t;
  t.start();
  std::vector<std::thread> threads;
  for (auto i=0ul; i<array_names_.size(); i++) {
    std::thread thread_object(create_arrays, this, i);
    threads.push_back(std::move(thread_object));
  }
  for (auto i=0ul; i<threads.size(); i++) {
    threads[i].join();
  }
  std::cout << "Create arrays elapsed time = " << t.getElapsedMilliseconds() << "ms" << std::endl;

  // Write Arrays
  std::cout << "\nNumber of cells to write= " << std::to_string(num_cells_to_write_) << std::endl;
  auto total_elapsed_time = 0ul;
  for (auto j=0; j<fragments_per_array_; j++) {
    threads.clear();
    create_buffers(true);
    t.start();
    for (auto i=0ul; i<array_names_.size(); i++) {
      std::thread thread_object(write_arrays, this, i);
      threads.push_back(std::move(thread_object));
    }
    for (auto i=0ul; i<threads.size(); i++) {
      threads[i].join();
    }
    total_elapsed_time += t.getElapsedMilliseconds();
    free_buffers();
  }
  std::cerr << "Write I/O Mode=" << get_io_write_mode(io_write_mode_) << std::endl;
  std::cerr << "Write Mode=" << get_array_mode(array_write_mode_) << std::endl;
  std::cerr << "Number of fragments per array = " << std::to_string(fragments_per_array_) << std::endl;
  std::cerr << "Write arrays elapsed time = " << total_elapsed_time << "ms" << std::endl;
  if (fragments_per_array_ > 1) {
    std::cout << "             mean time = " << total_elapsed_time/fragments_per_array_ << "ms" << std::endl;
  }

  // Read Arrays
  std::cout << "\nNumber of cells to write= " << std::to_string(num_cells_to_read_) << std::endl;
  threads.clear();
  create_buffers(false);
  for (auto i=0ul; i<array_names_.size(); i++) {
    std::thread thread_object(read_arrays, this, i);
    threads.push_back(std::move(thread_object));
  }
  for (auto i=0ul; i<threads.size(); i++) {
    threads[i].join();
  }
  std::cerr << "Read I/O Mode=" << get_io_read_mode(io_read_mode_) << std::endl;
  std::cerr << "Read Mode=" << get_array_mode(array_read_mode_) << std::endl;
  std::cout << "Read arrays elapsed time = " << t.getElapsedMilliseconds() << "ms" << std::endl;
  free_buffers();
  
  print_fragment_sizes(this, human_readable_sizes_);
}

