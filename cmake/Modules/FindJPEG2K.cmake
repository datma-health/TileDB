#
# FindJPEG2K.cmake
#
#
# The MIT License
#
# Copyright (c) 2016 MIT and Intel Corporation
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
# Finds the JPEG2K library. This module defines:
#   - JPEG2K_INCLUDE_DIR, directory containing headers
#   - JPEG2K_LIBRARIES, the JPEG2K library path
#   - JPEG2K_FOUND, whether JPEG2K has been found

# Find header files  
if(JPEG2K_SEARCH_HEADER_PATHS)
  find_path( 
      JPEG2K_INCLUDE_DIR openjpeg.h 
      PATHS ${JPEG2K_SEARCH_HEADER_PATHS}   
      NO_DEFAULT_PATH
  )
else()
  find_path(JPEG2K_INCLUDE_DIR openjpeg.h)
endif()

# Find library
if(JPEG2K_SEARCH_LIB_PATH)
  find_library(
      JPEG2K_LIBRARIES NAMES openjp2
      PATHS ${JPEG2K_SEARCH_LIB_PATH}$
      NO_DEFAULT_PATH
  )
else()
  find_library(JPEG2K_LIBRARIES NAMES openjp2)
endif()

if(JPEG2K_INCLUDE_DIR AND JPEG2K_LIBRARIES)
  message(STATUS "Found JPEG2K: ${JPEG2K_LIBRARIES}")
  set(JPEG2K_FOUND TRUE)
else()
  set(JPEG2K_FOUND FALSE)
endif()

if(JPEG2K_FIND_REQUIRED AND NOT JPEG2K_FOUND)
  message(FATAL_ERROR "Could not find the JPEG2K library.")
endif()

