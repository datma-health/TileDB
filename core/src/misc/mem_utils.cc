/**
 * @file   mem_utils.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2022 Omics Data Automation, Inc.
 * @copyright Copyright (c) 2024 dātma, inc™
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

#include <ctime>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include <sys/time.h>
#include <sys/resource.h>

#ifdef __linux
#include <malloc.h>
#endif

typedef struct statm_t{
  unsigned long size=0,resident=0,share=0,text=0,lib=0,data=0,dt=0;
} statm_t;

void print_time() {
  time_t track_time = time(NULL);
  tm* current = localtime(&track_time);
  char buffer[40];
  // Format %c(locale dependent) e.g. Fri Mar 18 16:13:48 2022 for EN_US
  std::strftime(buffer, sizeof(buffer), "%c ", current);
  std::cerr << buffer;
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

void print_rusage(const std::string& msg) {
  rusage usage;
  int result = getrusage(RUSAGE_SELF, &usage);
  std::cerr << msg << std::endl;
  if (result == 0) {
    std::cerr << "\tuser cpu time=" << usage.ru_utime.tv_sec << "seconds " << usage.ru_utime.tv_usec << "microseconds" << std::endl;
    std::cerr << "\tsys cpu time=" << usage.ru_stime.tv_sec << "seconds " << usage.ru_stime.tv_usec << "microseconds" << std::endl;
#ifdef __linux
    std::cerr << "\tmaximum resident set size: " << usage.ru_maxrss << "KB" << std::endl;
#else
    std::cerr << "\tmaximum resident set size: " << usage.ru_maxrss << "B" << std::endl;
#endif
  }
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
  std::cerr << "Memory stats " << msg << " size=" << readable_size(result.size)
            << " resident=" << readable_size(result.resident) << " share=" << readable_size(result.share)
            << " text=" << readable_size(result.text) << " lib=" << readable_size(result.lib)
            << " data=" << readable_size(result.data) << " dt=" << readable_size(result.dt) << std::endl;
#else
  print_time();
  print_rusage(msg);
#endif
}

void trim_memory() {
#ifdef __linux
  if (malloc_trim(0)) {
#ifdef DO_MEMORY_PROFILING
    print_memory_stats("Memory from the heap was successfully trimmed");
#endif
  }
#endif
}
