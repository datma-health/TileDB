/**
 * @file   uri.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 University of California, Los Angeles and Intel Corporation
 * @copyright Copyright (c) 2021 Omics Data Automation, Inc.
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

std::string uri::query() {
  return query_;
}

// Private Methods
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
  std::string::const_iterator after_proto_iter = protocol_iter;

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

  std::string::const_iterator query_iter = find(after_proto_iter, end_iter, '?');
  if(path_iter < query_iter)
    path_.assign(path_iter, query_iter);

  if (query_iter != end_iter) {
    ++query_iter;
    query_.assign(query_iter, end_iter);
  }
}

azure_uri::azure_uri(const std::string& uri_s) : uri(uri_s) {

  if(this->protocol().compare("azb") == 0) {
    is_azb_uri = true;
  } else if((this->protocol().compare("az") != 0)) {
    throw std::system_error(EPROTONOSUPPORT, std::generic_category(), "Azure Blob FS only supports az:// or azb:// URI protocols");
  }
  azure_uri_parse();
}

void azure_uri::azure_uri_parse() {

  if(!is_azb_uri) {
    std::size_t begin = this->host().find('@');
    std::size_t end = this->host().find('.');
    if (begin != std::string::npos && end != std::string::npos) {
      account_ = this->host().substr(begin+1, end-begin-1);
    }
    if (begin != std::string::npos) {
      container_ = this->host().substr(0, begin);
    }

    std::size_t dot_pos = std::string::npos;
    //For end_point Get the string after the 3rd '.' from the end
    if( (dot_pos = this->host().find_last_of('.') != std::string::npos) ) {
      if( (dot_pos = this->host().find_last_of('.', dot_pos) != std::string::npos) ) {
        if( (dot_pos = this->host().find_last_of('.', dot_pos) != std::string::npos) ) {
          endpoint_ = this->host().substr(dot_pos+1);
        }
      }
    }
  } else {
    //Parse query string
    std::string query_uri = this->query();
    if(!query_uri.empty()) {
      const std::string account_str = "account=";
      const std::string endpoint_str = "endpoint=";

      account_ = retrieve_from_query_string(query_uri, account_str);
      endpoint_ = retrieve_from_query_string(query_uri, endpoint_str);

      if( (account_.empty()) && (endpoint_.empty()) ) {
        throw std::system_error(EPROTO, std::generic_category(), "Azure Blob URI, in azb:// format, neither have endpoint nor account field");
      } 
    } else {
      throw std::system_error(EPROTO, std::generic_category(), "Azure Blob URI, in azb:// format, doesn't have query part");
    } 
  }
}

std::string azure_uri::account() {
  return account_;
}

std::string azure_uri::container() {
  return container_;
}

std::string azure_uri::endpoint() {
  return endpoint_;
}

std::string azure_uri::retrieve_from_query_string(const std::string &query_in, const std::string& keyname)
{
  size_t key_start_pos = std::string::npos, key_end_pos = std::string::npos;
  std::string key_value; 

  //Parse query string for passed key value
  if( (key_start_pos = query_in.find(keyname)) != std::string::npos) {
    key_start_pos = key_start_pos + keyname.size();
    if( (key_end_pos = query_in.find('&', key_start_pos)) != std::string::npos) {
      key_value = query_in.substr(key_start_pos, key_end_pos - key_start_pos);
    } else {
      key_value = query_in.substr(key_start_pos);
    } 
  }
  return key_value;
}

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

