#
# examples/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2016 MIT and Intel Corporation
# Copyright (c) 2018-2019 Omics Data Automation, Inc.
# Copyright (c) 2023-2024 dātma, inc™
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

# Function that builds an executable per example
function(build_TileDB_example TARGET)
  add_executable(${TARGET} EXCLUDE_FROM_ALL src/${TARGET}.cc)
  target_link_libraries(${TARGET} tiledb_static ${TILEDB_LIB_DEPENDENCIES})
endfunction()

# Get the example sources
file(GLOB TILEDB_EXAMPLE_SOURCES "src/*.cc")

# Include TileDB C API headers
include_directories("${CMAKE_SOURCE_DIR}/core/include/c_api/")

# Initialize name for example binaries
set(EXAMPLE_BINS)

# Iterate over all example sources and call the build function
foreach(EXAMPLE_SOURCE ${TILEDB_EXAMPLE_SOURCES})
  # Get the binary name
  get_filename_component(EXAMPLE_STRIPPED ${EXAMPLE_SOURCE} NAME)
  STRING(REGEX REPLACE ".cc$" "" EXAMPLE_BIN ${EXAMPLE_STRIPPED})
  set(EXAMPLE_BINS ${EXAMPLE_BINS} ${EXAMPLE_BIN})

  # Build example executable
  build_TileDB_example(${EXAMPLE_BIN})
endforeach()

# Add custom target 'examples'
add_custom_target(examples DEPENDS tiledb_static)
add_dependencies(examples ${EXAMPLE_BINS})

# Run a subset of examples for Continuous Integration Testing
if(NOT TILEDB_DISABLE_TESTING)
  set(CI_TESTS ${CMAKE_SOURCE_DIR}/examples/run_examples.sh &&
    echo "Running diff with expected_results" &&
    diff log ${CMAKE_SOURCE_DIR}/examples/expected_results &&
    ${CMAKE_SOURCE_DIR}/examples/run_examples_parallel.sh)
  add_test(NAME ci_tests COMMAND ${CI_TESTS})
  add_custom_target(ci_tests COMMAND ${CI_TESTS} DEPENDS examples)
  add_dependencies(tests ci_tests)
endif()
