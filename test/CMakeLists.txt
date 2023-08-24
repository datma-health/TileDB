#
# test/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2016 MIT and Intel Corporation
# Copyright (C) 2023 dātma, inc™
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

# Include test header directories
file(GLOB TILEDB_TEST_INCLUDE_DIRS "include/*")
include_directories(${TILEDB_TEST_INCLUDE_DIRS})

# Include TileDB core header directories
file(GLOB TILEDB_CORE_INCLUDE_DIRS "${CMAKE_CURRENT_LIST_DIR}/../core/include/*")
include_directories(${TILEDB_CORE_INCLUDE_DIRS})

# Get test source files
file(GLOB_RECURSE TILEDB_TEST_SOURCES "src/*/test*.cc")

# Add TILEDB_TEST_DIR macro
add_definitions(-DTILEDB_TEST_DIR="${CMAKE_CURRENT_SOURCE_DIR}")

# Catch2 major versions are both supported, so find Catch2 and set version
include(SetCatch2Version)

# Build one executable per test
foreach(TILEDB_TEST_SOURCE ${TILEDB_TEST_SOURCES})
  get_filename_component(FILENAME ${TILEDB_TEST_SOURCE} NAME)
  string(REGEX REPLACE ".cc$" "" TARGET ${FILENAME})
  
  add_executable(${TARGET} EXCLUDE_FROM_ALL ${TILEDB_TEST_SOURCE})
  target_link_libraries(${TARGET} tiledb_static ${TILEDB_LIB_DEPENDENCIES} -ldl Catch2::Catch2)

  add_test(${TARGET} ${TARGET})
  set(ALL_CMDS ${ALL_CMDS} ${TARGET})
endforeach()

# Add custom target 'check'
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} -V DEPENDS ${ALL_CMDS})

# Add check to all tests
add_dependencies(tests ${ALL_CMDS})
