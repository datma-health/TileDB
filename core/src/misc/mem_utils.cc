/**
 * @file   mem_utils.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2022 Omics Data Automation, Inc.
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
 * This file implements utilities to help profile memory
 */

#include <time.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

typedef struct statm_t{
  unsigned long size=0,resident=0,share=0,text=0,lib=0,data=0,dt=0;
} statm_t;

void print_time() {
  time_t track_time = time(NULL);
  tm* current = localtime(&track_time);
  std::cerr << current->tm_hour << ":" << current->tm_min << ":" << current->tm_sec << " ";
}

std::string readable_size(size_t size) {
  std::vector<std::string> suffix = { "B", "KB", "MB", "GB", "TB" };
  auto i = 0u;
  size = size*4096;
  while (size > 0) {
    if (i > suffix.size()) break;
    if (size/1024 == 0) {
      return std::to_string(size) + suffix[i];
    } else {
      size /= 1024;
      i++;
    }
  }
  return std::to_string(size);
}
    
void print_memory_stats(const std::string& msg) {
#ifdef __linux
  const char* statm_path = "/proc/self/statm";

  statm_t result;

  FILE *f = fopen(statm_path,"r");
  if (!f) {
    perror(statm_path);
    abort();
  }
  if (7 != fscanf(f,"%lu %lu %lu %lu %lu %lu %lu",
                  &result.size, &result.resident, &result.share, &result.text, &result.lib, &result.data, &result.dt)) {
    perror(statm_path);
    abort();
  }
  fclose(f);

  print_time();
  std::cerr << "Memory stats(pages) " << msg << " size=" << readable_size(result.size)
            << " resident=" << readable_size(result.resident) << " share=" << readable_size(result.share)
            << " text=" << readable_size(result.text) << " lib=" << readable_size(result.lib)
            << " data=" << readable_size(result.data) << " dt=" << readable_size(result.dt) << std::endl;
#else
  print_time();
  std::cerr << "TBD: Memory stats " << msg << std::endl;
#endif
}
