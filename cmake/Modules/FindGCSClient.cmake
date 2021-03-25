#
# FindGCSClient.cmake
#
# The MIT License
#
# Copyright (c) 2021 Omics Data Automation, Inc.
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
# Finds and/or builds the gcs static libraries

add_custom_target(gcssdk-ep)

message(CHECK_START "Finding GCS SDK Library")

include(GNUInstallDirs)
if(GCSSDK_ROOT_DIR)
  set(GCSSDK_PREFIX "${GCSSDK_ROOT_DIR}")
elseif(DEFINED ENV{GCSSDK_ROOT_DIR})
  set(GCSSDK_PREFIX $ENV{GCSSDK_ROOT_DIR})
endif()
set(GCSSDK_INCLUDE_DIR "${GCSSDK_PREFIX}/${CMAKE_INSTALL_INCLUDEDIR}")
set(GCSSDK_LIB_DIR "${CMAKE_INSTALL_LIBDIR}")

set(PWD $ENV{PWD})
if(EXISTS ${GCSSDK_PREFIX}/include AND EXISTS ${GCSSDK_PREFIX}/${CMAKE_INSTALL_LIBDIR})
  get_filename_component(GCSSDK_PREFIX ${GCSSDK_PREFIX} ABSOLUTE BASE_DIR ${PWD})
  set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${GCSSDK_PREFIX})
endif()
find_package(google_cloud_cpp_storage PATHS ${CMAKE_PREFIX_PATH})

if( ${google_cloud_cpp_storage_FOUND})
  # Look for ${storage_client_DIR}
  message(STATUS "Found GCS SDK headers: ${GCSSDK_INCLUDE_DIR}")
  message(STATUS "Found GCS SDK libraries: ${GCSSDK_LINK_LIBRARIES}")
  message(CHECK_PASS "found")
elseif(NOT GCSSDK_PREFIX)
  message(CHECK_FAIL "not found")
  message(FATAL_ERROR "Try invoking cmake with -DGCSSDK_ROOT_DIR=/path/to/gcssdk or -DCMAKE_INSTALL_PREFIX=/path/to/gcssdk or set environment variable GCSSDK_ROOT_DIR before invoking cmake")
elseif(NOT GCSSDK_FOUND)
  # Try building from source
  message(STATUS "Adding GCS SDK as an external project")
  include(ExternalProject)

  # gcssdk has nlohmann json as its dependency
  ExternalProject_Add(nlohmann-build
    PREFIX ${GCSSDK_PREFIX}
    URL "https://github.com/nlohmann/json/archive/v3.9.1.zip"
    CMAKE_ARGS
        -DJSON_BuildTests=OFF
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_CXX_STANDARD=11 
        -DCMAKE_INSTALL_PREFIX=${GCSSDK_PREFIX}
    )

  # gcssdk has abseil as its dependency, so build that first
  ExternalProject_Add(abseil-build
    PREFIX ${GCSSDK_PREFIX}
    URL "https://github.com/abseil/abseil-cpp/archive/20200923.3.zip"
    CMAKE_ARGS
        -DBUILD_SHARED_LIBS=OFF
        -DABSL_RUN_TESTS=OFF
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_CXX_STANDARD=11 
        -DCMAKE_INSTALL_PREFIX=${GCSSDK_PREFIX}
        )

  ExternalProject_Add(crc32-build
    PREFIX ${GCSSDK_PREFIX}
    URL "https://github.com/google/crc32c/archive/1.1.0.tar.gz"
    CMAKE_ARGS
        -DBUILD_SHARED_LIBS=OFF
        -DCRC32C_BUILD_TESTS=OFF
        -DCRC32C_BUILD_BENCHMARKS=OFF
        -DCRC32C_USE_GLOG=OFF
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_CXX_STANDARD=11 
        -DCMAKE_INSTALL_PREFIX=${GCSSDK_PREFIX}
        )

  ExternalProject_Add(gcssdk-build
    PREFIX ${GCSSDK_PREFIX}
    URL ${GCSSDK_URL}
    BUILD_IN_SOURCE 1
    CMAKE_ARGS
        -DBUILD_SHARED_LIBS=OFF
        -DBUILD_TESTING=OFF
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DGOOGLE_CLOUD_CPP_ENABLE=storage
        -DCMAKE_BUILD_TYPE=Release
        -DCMAKE_INSTALL_PREFIX=${GCSSDK_PREFIX}
        -DCMAKE_PREFIX_PATH=${GCSSDK_PREFIX}
        )

  add_dependencies(gcssdk-build nlohmann-build)
  add_dependencies(gcssdk-build abseil-build)
  add_dependencies(gcssdk-build crc32-build)
  add_dependencies(gcssdk-ep gcssdk-build)
endif()

# Note: abseil is a mess with library organization...
set(_GCSSDK_LIBS "google_cloud_cpp_storage"
                 "google_cloud_cpp_common"
                 "crc32c"
                 "absl_bad_optional_access"
                 "absl_bad_variant_access"
                 "absl_strings_internal"
                 "absl_str_format_internal"
                 "absl_strings"
                 "absl_throw_delegate"
                 "absl_time"
                 "absl_time_zone"
                 "absl_raw_logging_internal"
                 "absl_int128"
                 )
set(GCSSSDK_LINK_LIBRARIES)
file(MAKE_DIRECTORY ${GCSSDK_INCLUDE_DIR})
foreach(_GCSSDK_LIB ${_GCSSDK_LIBS})
  set(
    _GCSSDK_STATIC_LIBRARY
    "${GCSSDK_PREFIX}/${GCSSDK_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${_GCSSDK_LIB}${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(_GCSSDK_TARGET_NAME GCS::${_GCSSDK_LIB})
  add_library(${_GCSSDK_TARGET_NAME} STATIC IMPORTED)
  set_target_properties(${_GCSSDK_TARGET_NAME} PROPERTIES
    IMPORTED_LOCATION ${_GCSSDK_STATIC_LIBRARY}
    INTERFACE_INCLUDE_DIRECTORIES ${GCSSDK_INCLUDE_DIR})
  list(APPEND GCSSDK_LINK_LIBRARIES ${_GCSSDK_TARGET_NAME})
endforeach()
