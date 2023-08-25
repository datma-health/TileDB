#
# core/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2016 MIT and Intel Corporation
# Copyright (c) 2023 Omics Data Automation, Inc.
# Copyright (c) 2023 dātma, inc™
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

# Include core header directories
file(GLOB TILEDB_CORE_INCLUDE_DIRS "include/*" 
	"src/codec/external/lz4"
	"src/codec/external/bitshuffle")
include_directories(${TILEDB_CORE_INCLUDE_DIRS})

# Gather the core source files
file(GLOB_RECURSE TILEDB_CORE_SOURCES "src/*.cc" "src/codec/external/bitshuffle/*.c")
if(LZ4_FOUND)
    Message(STATUS "Using LZ4 from ${LZ4_LIBRARIES}")
else()
    list(APPEND TILEDB_CORE_SOURCES "src/codec/external/lz4/lz4.c")
endif()

# Compile sources with PIC
add_library(TILEDB_CORE_OBJECTS OBJECT ${TILEDB_CORE_SOURCES})
set_property(TARGET TILEDB_CORE_OBJECTS PROPERTY POSITION_INDEPENDENT_CODE ON)

add_dependencies(TILEDB_CORE_OBJECTS awssdk-ep)
add_dependencies(TILEDB_CORE_OBJECTS gcssdk-ep)

# Create static and shared libraries
if(USE_HDFS)
    add_library(tiledb_static STATIC $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS> $<TARGET_OBJECTS:hdfs_OBJECTS>)
    add_library(tiledb_shared SHARED $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS> $<TARGET_OBJECTS:hdfs_OBJECTS>)
else()
    add_library(tiledb_static STATIC $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>)
    add_library(tiledb_shared SHARED $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>)
endif()

set_target_properties(tiledb_static tiledb_shared PROPERTIES OUTPUT_NAME "tiledb")

# See https://glandium.org/blog/?p=2764 for workaound for undefined weak symbols in MacOS
if(APPLE)
set(TILEDB_LINK_OPTIONS "-Wl,-U,_EVP_MAC_CTX_free;-Wl,-U,_EVP_MAC_CTX_new;-Wl,-U,_EVP_MAC_fetch;;-Wl,-U,_EVP_MAC_final;-Wl,-U,_EVP_MAC_free;-Wl,-U,_EVP_MAC_init;-Wl,-U,_EVP_MAC_update;-Wl,-U,_EVP_MD_get_size;-Wl,-U,_OSSL_PARAM_construct_end;-Wl,-U,_OSSL_PARAM_construct_utf8_string")
target_link_options(tiledb_static PUBLIC ${TILEDB_LINK_OPTIONS})
target_link_options(tiledb_shared PUBLIC ${TILEDB_LINK_OPTIONS})
endif()

target_link_libraries(tiledb_static muparserx azure-storage-lite azure-storage-adls ${AWSSDK_LINK_LIBRARIES} ${GCSSDK_LINK_LIBRARIES})
target_link_libraries(tiledb_shared ${TILEDB_LIB_DEPENDENCIES})

# Install libraries
install(
    TARGETS tiledb_static tiledb_shared 
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)
file(GLOB TILEDB_HEADERS "include/c_api/*.h")
install(FILES ${TILEDB_HEADERS} DESTINATION include)

