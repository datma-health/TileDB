/**
 * @file   uri.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 University of California, Los Angeles and Intel Corporation
 * @copyright Copyright (c) 2021 Omics Data Automation, Inc.
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
 * URI Parsing
 */

#include <stdint.h>
#include <string>
#include <algorithm>
#include <cctype>
#include <functional>
#include <system_error>
#include <stdlib.h>
#include "uri.h"
#include <cstring>
// Constructor
uri::uri(const std::string& uri_s) {
  parse(uri_s);
}

// Accessors
std::string uri::protocol() {
  return protocol_;
}

std::string uri::host() {
  return host_;
}

std::string uri::port() {
  return port_;
}

int16_t uri::nport() {
  return nport_;
}

std::string uri::path() {
  return path_;
}

std::unordered_map<std::string, std::string> uri::query() {
  return query_;
}

// Private Methods
std::string uri::urlDecode(const std::string& uri_s) {
  std::string result;
  result.reserve(uri_s.size());

  for (std::size_t i = 0; i < uri_s.size(); ++i) {
    auto ch = uri_s[i];

    if (ch == '%' && (i + 2) < uri_s.size()) {
      auto hex = uri_s.substr(i + 1, 2);
      auto dec = static_cast<char>(std::strtol(hex.c_str(), nullptr, 16));
      result.push_back(dec);
      i += 2;
    } else {
      result.push_back(ch);
    }
  }

  return result;
}

void uri::parse(const std::string& uri_s)
{
  if (uri_s.empty()) {
    throw std::system_error(EINVAL, std::generic_category(), "Cannot parse empty string as an URI");
  }
  
  const std::string::const_iterator start_iter = uri_s.begin();
  const std::string::const_iterator end_iter = uri_s.end();
  
  const std::string protocol_end("://");
  std::string::const_iterator protocol_iter = std::search(start_iter, end_iter, protocol_end.begin(), protocol_end.end());
  if (protocol_iter == uri_s.end()) {
    throw std::system_error(EINVAL, std::generic_category(), "String does not seem to be a URI");
  }

  // protocol is case insensitive
  protocol_.reserve(std::distance(start_iter, protocol_iter));
  //std::transform(start_iter, protocol_iter, back_inserter(protocol_), std::ptr_fun<int,int>(tolower));
  std::transform(start_iter, protocol_iter, back_inserter(protocol_), [](int c){return std::tolower(c);});
  if (protocol_iter == end_iter) {
    return;
  }
    
  std::advance(protocol_iter, protocol_end.length());

  std::string::const_iterator path_iter = find(protocol_iter, end_iter, '/');
  std::string::const_iterator port_iter = find(protocol_iter, path_iter, ':');

  // host is case insensitive
  host_.reserve(distance(protocol_iter, port_iter));
  //std::transform(protocol_iter, port_iter, back_inserter(host_), std::ptr_fun<int,int>(tolower));
  std::transform(protocol_iter, port_iter, back_inserter(host_), [](int c){return std::tolower(c);});

  if (port_iter != path_iter) {
    ++port_iter;
    port_.assign(port_iter, path_iter);
    
    // Convert port into int16_t
    const char *start_ptr = port_.c_str();
    char *end_ptr;
    errno = 0;
    long port_val = strtol(start_ptr, &end_ptr, 10);
    if (errno == ERANGE || port_val > UINT16_MAX){
      throw std::system_error(ERANGE, std::generic_category(), "URI has a bad port #");
    }
    if (start_ptr != end_ptr) {
      nport_ = (uint16_t)port_val;
    }
  }
  std::string::const_iterator query_iter = find(path_iter, end_iter, '?');
  path_.assign(path_iter, query_iter);
  if (query_iter != end_iter) {
    ++query_iter;
    std::string queryTemp(urlDecode(std::string(query_iter, end_iter)));
    char* save_ptr;
    for (char* token = strtok_r(queryTemp.data(), "&", &save_ptr);
         token != NULL; token = strtok_r(NULL, "&", &save_ptr)) {
      char* search = strchr(token, '=');
      if (search == NULL || (std::size_t)(search - token) == 0) {
        throw std::system_error(EINVAL, std::generic_category(),
                                "Query is in incorrect format");
      }
      std::string key(
          std::string(token).substr(0, (std::size_t)(search - token)));
      query_[key] = std::string(++search);
    }
  }
}

azure_uri::azure_uri(const std::string& uri_s) : uri(uri_s) {
  if (this->protocol().compare("azb") == 0) {
    account_ = this->query()["account"];
    container_ = this->host();
    endpoint_ = this->query()["endpoint"];
  } else {
    std::size_t begin = this->host().find('@');
    std::size_t end = this->host().find('.');
    if (begin != std::string::npos && end != std::string::npos) {
      account_ = this->host().substr(begin + 1, end - begin - 1);
      endpoint_ = this->host().substr(begin + 1,
                                      this->host().find('/', end) - begin - 1);
    }
    if (begin != std::string::npos) {
      container_ = this->host().substr(0, begin);
    } else {
      container_ = this->host();
    }
  }
}

std::string azure_uri::account() {
  return account_;
}

std::string azure_uri::container() {
  return container_;
}

std::string azure_uri::endpoint() { return endpoint_; }

s3_uri::s3_uri(const std::string& uri_s) : uri(uri_s) {
  bucket_ = this->host();
}

std::string s3_uri::bucket() {
  return bucket_;
}
gcs_uri::gcs_uri(const std::string& uri_s) : uri(uri_s) {
  bucket_ = this->host();
}

std::string gcs_uri::bucket() {
  return bucket_;
}

