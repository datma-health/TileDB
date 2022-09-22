#
# cmake/Modules/SetCatch2Version.cmake
#
# The MIT License
#
# Copyright (c) 2022 Omics Data Automation, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# Find Catch2 installation and setup Catch2 version to allow
# test/include/Catch2/catch.h to support both major versions, 2 and 3 for now.
#

find_package(Catch2 REQUIRED)

set(CATCH2_HEADER "catch2/catch.hpp")
find_path(CATCH2_INCLUDE_FILE ${CATCH2_HEADER})
if(CATCH2_INCLUDE_FILE)
  set(CATCH2_MAJOR_VERSION 2)
else()
  set(CATCH2_HEADER "catch2/catch_all.hpp")
  find_path(CATCH2_INCLUDE_FILE ${CATCH2_HEADER})
  if(CATCH2_INCLUDE_FILE)
    set(CATCH2_MAJOR_VERSION 3)
  else()
    message(FATAL "Could not figure out Catch2 versions. Try using CMAKE_PREFIX_PATH to point to a Catch2 installation")
  endif()
endif()

message(STATUS "Found Catch2: ${CATCH2_INCLUDE_FILE}/${CATCH2_HEADER} Version=${CATCH2_MAJOR_VERSION}")
add_definitions(-DCATCH2_MAJOR_VERSION=${CATCH2_MAJOR_VERSION})
