#!/bin/bash
#
# run_examples_base.sh
#
#
# The MIT License
#
# Copyright (c) 2024 dātma, inc™
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

source $(dirname $0)/run_examples_base.sh "$@"

run_example ./tiledb_array_parallel_write_dense_1 $1 1
run_example ./tiledb_array_parallel_write_dense_2 $1 2
run_example ./tiledb_array_parallel_write_sparse_1 $1 3
run_example ./tiledb_array_parallel_write_sparse_2 $1 4
run_example ./tiledb_array_parallel_read_dense_1 $1 5
run_example ./tiledb_array_parallel_read_dense_2 $1 6
run_example ./tiledb_array_parallel_read_sparse_1 $1 7
run_example ./tiledb_array_parallel_read_sparse_2 $1 8
run_example ./tiledb_array_parallel_consolidate_dense $1 9
run_example ./tiledb_array_parallel_consolidate_sparse $1 10

# Cannot do a diff for parallel tests as there is no deterministic order
# for the results, so just run them
# diff $LOGFILENAME $(dirname $0)/expected_results_parallel

