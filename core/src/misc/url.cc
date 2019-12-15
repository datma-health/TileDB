/**
 * @file   url.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 University of California, Los Angeles and Intel Corporation
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
 * URL Parsing
 */

#include <stdint.h>
#include <string>
#include <algorithm>
#include <cctype>
#include <functional>
#include <system_error>
#include <stdlib.h>

#include "url.h"

// Constructor
url::url(const std::string& url_s) {
  parse(url_s);
}

// Accessors
std::string url::protocol() {
  return protocol_;
}

std::string url::host() {
  return host_;
}

std::string url::port() {
  return port_;
}

int16_t url::nport() {
  return nport_;
}

std::string url::path() {
  return path_;
}

std::string url::query() {
  return query_;
}

// Private Methods
void url::parse(const std::string& url_s)
{
  if (url_s.empty()) {
    throw std::system_error(EINVAL, std::generic_category(), "Cannot parse empty string as an URL");
  }
  
  const std::string::const_iterator start_iter = url_s.begin();
  const std::string::const_iterator end_iter = url_s.end();
  
  const std::string protocol_end("://");
  std::string::const_iterator protocol_iter = std::search(start_iter, end_iter, protocol_end.begin(), protocol_end.end());
  if (protocol_iter == url_s.end()) {
    throw std::system_error(EINVAL, std::generic_category(), "String does not seem to be a URL");
  }

  // protocol is case insensitive
  protocol_.reserve(std::distance(start_iter, protocol_iter));
  std::transform(start_iter, protocol_iter, back_inserter(protocol_), std::ptr_fun<int,int>(tolower));
  if (protocol_iter == end_iter) {
    return;
  }
    
  std::advance(protocol_iter, protocol_end.length());

  std::string::const_iterator path_iter = find(protocol_iter, end_iter, '/');
  std::string::const_iterator port_iter = find(protocol_iter, path_iter, ':');

  // host is case insensitive
  host_.reserve(distance(protocol_iter, port_iter));
  std::transform(protocol_iter, port_iter, back_inserter(host_), std::ptr_fun<int,int>(tolower));

  if (port_iter != path_iter) {
    ++port_iter;
    port_.assign(port_iter, path_iter);
    
    // Convert port into int16_t
    const char *start_ptr = port_.c_str();
    char *end_ptr;
    errno = 0;
    long port_val = strtol(start_ptr, &end_ptr, 10);
    if (errno == ERANGE || port_val > UINT16_MAX){
      throw std::system_error(ERANGE, std::generic_category(), "URL has a bad port #");
    }
    if (start_ptr != end_ptr) {
      nport_ = (uint16_t)port_val;
    }
  }

  std::string::const_iterator query_iter = find(path_iter, end_iter, '?');
  path_.assign(path_iter, query_iter);

  if (query_iter != end_iter) {
    ++query_iter;
    query_.assign(query_iter, end_iter);
  }
}

azure_url::azure_url(const std::string& url_s) : url(url_s) {
  std::size_t begin = this->host().find('@');
  std::size_t end = this->host().find('.');
  if (begin != std::string::npos && end != std::string::npos) {
    account_ = this->host().substr(begin+1, end-begin-1);
  }
  if (begin != std::string::npos) {
    container_ = this->host().substr(0, begin);
  }
}

std::string azure_url::account() {
  return account_;
}

std::string azure_url::container() {
  return container_;
}

