/**
 * @file   uri.h
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
 * URI Parsing Header File
 */

#ifndef URI_HH_
#define URI_HH_    
#include <string>

struct uri {
  uri(const std::string& uri_s);

  // Accessors
  std::string protocol();
  std::string host();
  std::string port();
  int16_t nport();
  std::string path();
  std::string query();
  std::string endpoint();

 private:
  void parse(const std::string& uri_s);

 private:
  std::string protocol_;
  std::string host_;
  std::string port_;
  int16_t nport_ = 0;
  std::string query_;

protected:
  std::string endpoint_;
  std::string path_;
};

struct azure_uri : uri {
  azure_uri(const std::string& uri_s);
  std::string account();
  std::string container();
  bool is_azb_uri(void);

private:
  std::string retrieve_from_query_string(const std::string& query_in, 
      const std::string& keyname);
  void azure_uri_parse(void);

 private:
  std::string account_;
  std::string container_;
  bool azb_uri_ = false;
};

struct s3_uri : uri {
  s3_uri(const std::string& uri_s);
  std::string bucket();

 private:
  std::string bucket_;
};

struct gcs_uri : uri {
  gcs_uri(const std::string& uri_s);
  std::string bucket();

 private:
  std::string bucket_;
};


#endif /* URI_HH_ */
