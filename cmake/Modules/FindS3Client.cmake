#
# FindS3Client.cmake
#
# The MIT License
#
# Copyright (c) 2022-2023 Omics Data Automation, Inc.
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
# Finds and/or builds the s3 static library

add_custom_target(awssdk-ep)

message(CHECK_START "Finding AWS SDK Library")

include(GNUInstallDirs)

if(AWSSDK_ROOT_DIR)
  set(AWSSDK_PREFIX "${AWSSDK_ROOT_DIR}")
elseif(DEFINED ENV{AWSSDK_ROOT_DIR})
  set(AWSSDK_PREFIX $ENV{AWSSDK_ROOT_DIR})
endif()
set(AWSSDK_INCLUDE_DIR "${AWSSDK_PREFIX}/${CMAKE_INSTALL_INCLUDEDIR}")
set(AWSSDK_LIB_DIR "${CMAKE_INSTALL_LIBDIR}")

set(PWD $ENV{PWD})
set(S3_COMPONENTS config s3 transfer identity-management sts)
if(EXISTS ${AWSSDK_PREFIX}/include AND EXISTS ${AWSSDK_PREFIX}/${CMAKE_INSTALL_LIBDIR})
  get_filename_component(AWSSDK_PREFIX ${AWSSDK_PREFIX} ABSOLUTE BASE_DIR ${PWD})
  set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${AWSSDK_PREFIX})
  find_package(AWSSDK QUIET COMPONENTS ${S3_COMPONENTS} PATHS ${CMAKE_PREFIX_PATH})
else()
  find_package(AWSSDK QUIET COMPONENTS ${S3_COMPONENTS})
endif()

if(AWSSDK_FOUND)
  message(STATUS "Found AWS SDK headers: ${AWSSDK_INCLUDE_DIR}")
  message(STATUS "Found AWS SDK libraries: ${AWSSDK_LINK_LIBRARIES}")
  message(CHECK_PASS "found")
elseif(NOT AWSSDK_PREFIX)
  message(CHECK_FAIL "not found")
  message(FATAL_ERROR "Try invoking cmake with -DAWSSDK_ROOT_DIR=/path/to/awssdk or -DCMAKE_INSTALL_PREFIX=/path/to/awssdk or set environment variable AWSSDK_ROOT_DIR before invoking cmake")
elseif(NOT AWSSDK_FOUND)
  # Try building from source
  message(STATUS "Adding AWS SDK as an external project")
  include(ExternalProject)

  ExternalProject_Add(aws-c-common-build
    URL "https://github.com/awslabs/aws-c-common/archive/v0.6.9.tar.gz"
    CMAKE_ARGS
    -DBUILD_SHARED_LIBS=OFF
    -DCMAKE_INSTALL_PREFIX=${AWSSDK_PREFIX}
    -DCMAKE_PREFIX_PATH=${AWSSDK_PREFIX}
    -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR})

  ExternalProject_Add(aws-checksums-build
    URL "https://github.com/awslabs/aws-checksums/archive/v0.1.12.tar.gz"
    CMAKE_ARGS 
    -DBUILD_SHARED_LIBS=OFF
    -DCMAKE_INSTALL_PREFIX=${AWSSDK_PREFIX}
    -DCMAKE_PREFIX_PATH=${AWSSDK_PREFIX}
    -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}
    DEPENDS aws-c-common-build)
  
  ExternalProject_Add(aws-c-event-stream-build
    URL "https://github.com/awslabs/aws-c-event-stream/archive/v0.1.5.tar.gz"
    CMAKE_ARGS ${AWSSDK_COMMON_CMAKE_ARGS}
    -DBUILD_SHARED_LIBS=OFF
    -DCMAKE_INSTALL_PREFIX=${AWSSDK_PREFIX}
    -DCMAKE_PREFIX_PATH=${AWSSDK_PREFIX}
    -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}
    DEPENDS aws-checksums-build)

  ExternalProject_Add(awssdk-build
    PREFIX ${AWSSDK_PREFIX}
    URL ${AWSSDK_URL}
    PATCH_COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/core/include/misc/tdb_openssl_shim.h 
                  ${CMAKE_CURRENT_SOURCE_DIR}/core/include/misc/dl_utils.h 
                  ${AWSSDK_PREFIX}/src/awssdk-build/aws-cpp-sdk-core/include/aws/core/utils/crypto/openssl &&
                  cp ${CMAKE_CURRENT_SOURCE_DIR}/core/src/misc/tdb_openssl_shim.cc
                  ${CMAKE_CURRENT_SOURCE_DIR}/core/src/misc/dl_utils.cc 
                  ${AWSSDK_PREFIX}/src/awssdk-build/aws-cpp-sdk-core/source/utils/crypto/openssl &&
                  patch -p1 < ${CMAKE_CURRENT_SOURCE_DIR}/cmake/patches/awssdk/build.patch &&
                  patch -p1 < ${CMAKE_CURRENT_SOURCE_DIR}/cmake/patches/awssdk/cjson.patch &&
		  patch -p1 < ${CMAKE_CURRENT_SOURCE_DIR}/cmake/patches/awssdk/eventstreamdecoder.patch &&
		  patch -p1 < ${CMAKE_CURRENT_SOURCE_DIR}/cmake/patches/awssdk/aws_ossl.patch
    CMAKE_ARGS
    -DBUILD_SHARED_LIBS=OFF
    -DENABLE_TESTING=OFF
    -DENABLE_UNITY_BUILD=ON
    -DCUSTOM_MEMORY_MANAGEMENT=OFF
    -DBUILD_DEPS=OFF
    -DBUILD_ONLY=config\\$<SEMICOLON>s3\\$<SEMICOLON>transfer\\$<SEMICOLON>identity-management\\$<SEMICOLON>sts
    -DMINIMIZE_SIZE=ON
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_INSTALL_PREFIX=${AWSSDK_PREFIX}
    -DCMAKE_PREFIX_PATH=${AWSSDK_PREFIX}
    -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR})

  add_dependencies(awssdk-build aws-c-common-build)
  add_dependencies(awssdk-build aws-c-event-stream-build)
  add_dependencies(awssdk-build aws-checksums-build)
  add_dependencies(awssdk-ep awssdk-build)

   # AWS C++ SDK related libraries to link statically
  set(_AWSSDK_LIBS
      aws-cpp-sdk-identity-management
      aws-cpp-sdk-sts
      aws-cpp-sdk-cognito-identity
      aws-cpp-sdk-s3
      aws-cpp-sdk-core
      aws-c-event-stream
      aws-checksums
      aws-c-common)

  set(AWSSDK_LINK_LIBRARIES)

  file(MAKE_DIRECTORY ${AWSSDK_INCLUDE_DIR})
    
  foreach(_AWSSDK_LIB ${_AWSSDK_LIBS})
    # aws-c-common -> AWS-C-COMMON
    string(TOUPPER ${_AWSSDK_LIB} _AWSSDK_LIB_UPPER)
    # AWS-C-COMMON -> AWS_C_COMMON
    string(REPLACE "-" "_" _AWSSDK_LIB_NAME_PREFIX ${_AWSSDK_LIB_UPPER})
    set(
      _AWSSDK_STATIC_LIBRARY
      "${AWSSDK_PREFIX}/${AWSSDK_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${_AWSSDK_LIB}${CMAKE_STATIC_LIBRARY_SUFFIX}"
      )
    if(${_AWSSDK_LIB} MATCHES "^aws-cpp-sdk-")
      set(_AWSSDK_TARGET_NAME ${_AWSSDK_LIB})
    else()
      set(_AWSSDK_TARGET_NAME AWS::${_AWSSDK_LIB})
    endif()
    add_library(${_AWSSDK_TARGET_NAME} STATIC IMPORTED)
    set_target_properties(${_AWSSDK_TARGET_NAME} PROPERTIES
                IMPORTED_LOCATION ${_AWSSDK_STATIC_LIBRARY}
                INTERFACE_INCLUDE_DIRECTORIES ${AWSSDK_INCLUDE_DIR})
    list(APPEND AWSSDK_LINK_LIBRARIES ${_AWSSDK_TARGET_NAME})
  endforeach()
  if(APPLE)
    # AWSSDK has some sort of transitive dependency on CoreFoundation,
    # add this explicitly even though CMAKE_FIND_FRAMEWORK is set to LAST
    list(APPEND AWSSDK_LINK_LIBRARIES "-framework CoreFoundation")
  endif()
  # aws-c libraries have a dl dependency
  list(APPEND AWSSDK_LINK_LIBRARIES dl)
  add_dependencies(aws-cpp-sdk-s3 awssdk-ep)
  message(STATUS "To be built AWS SDK headers: ${AWSSDK_INCLUDE_DIR}")
  message(STATUS "To be built AWS SDK libraries: ${AWSSDK_LINK_LIBRARIES}")
  message(CHECK_PASS "AWS SDK to be built when make is invoked")
endif()




