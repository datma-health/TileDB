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

typedef struct {
  unsigned long size=0,resident=0,share=0,text=0,lib=0,data=0,dt=0;
} statm_t;

void print_time() {
  time_t track_time = time(NULL);
  tm* current = localtime(&track_time);
  std::cerr << current->tm_hour << ":" << current->tm_min << ":" << current->tm_sec << " ";
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
  std::cerr << "Memory stats(pages) " << msg << " size=" << result.size
            << " resident=" << result.resident << " share=" << result.share
            << " text=" << result.text << " lib=" << result.lib
            << " data=" << result.data << " dt=" << result.dt << std::endl;
#else
  print_time();
  std::cerr << "TBD: Memory stats " << msg << std::endl;
#endif
}
