#!/bin/bash
#
# run_examples.sh
#
#
# The MIT License
#
# Copyright (c) 2018 Omics Data Automation Inc. and Intel Corporation
# Copyright (c) 2019 Omics Data Automation Inc.
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

# This runs a small subset of example binaries from <build>/examples/ directory.

# Usage without arguments(workspace will be a default "my_workspace" in <build/examples/) :
#    ./run_examples.sh 
# Usage with args(path can either be a posix filesystem path or a hdfs/emrfs/gs URL)
#    ./run_examples.sh <path>
# Results are either in a "log" file for the default case or <last_segment_of_specified_path>.log
#  if path is specified.
#    e.g for ./run_examples.sh gs://my_bucket/my_dir/my_test, expect results in test.log
# Check the log file against the <install>/examples/expected_results file.

check_rc() {
  if [[ $# -eq 1 ]]; then
    if [[ $1 -ne 0 ]]; then
      echo
      echo "Exit Status=$1. Quitting execution of run_examples.sh"
      exit $1
    fi
  fi
}

run_example() {
  if [[ $# -eq 3 ]]
  then
    logfile=`basename $2`.log
    echo "Example $3: Running $1..." | tee -a ${logfile}
    $1 $2 | tee -a ${logfile}
    check_rc ${PIPESTATUS[0]}
    echo "Example $3: Done running $1" | tee -a ${logfile}
  else
    echo "Example $2: Running $1..." | tee -a log
    $1 | tee -a log
    check_rc ${PIPESTATUS[0]}
    echo "Example $2: Done running $1" | tee -a log
  fi
}

if [[ -n $1 ]]
then
  rm -fr `basename $1`.log
else
  rm -fr log
fi

run_example ./tiledb_workspace_group_create $1 1
run_example ./tiledb_ls_workspaces $1 2
run_example ./tiledb_array_create_dense $1 3
run_example ./tiledb_array_create_sparse $1 4
run_example ./tiledb_array_primitive $1 5
run_example ./tiledb_array_write_dense_1 $1 6
run_example ./tiledb_array_write_sparse_1 $1 7
sleep 5
run_example ./tiledb_ls $1 8
run_example ./tiledb_array_read_dense_1 $1 9
run_example ./tiledb_array_write_dense_2 $1 10
sleep 5
run_example ./tiledb_array_read_dense_1 $1 11
run_example ./tiledb_array_read_dense_2 $1 12
run_example ./tiledb_array_write_dense_sorted $1 13
sleep 5
run_example ./tiledb_array_read_dense_1 $1 14
run_example ./tiledb_array_update_dense_1 $1 15
sleep 5
run_example ./tiledb_array_read_dense_1 $1 16
run_example ./tiledb_array_update_dense_2 $1 17
sleep 5
run_example ./tiledb_array_read_dense_1 $1 18
run_example ./tiledb_array_write_sparse_1 $1 19
sleep 5
export TILEDB_CACHE=1
run_example ./tiledb_array_read_sparse_1 $1 20
unset TILEDB_CACHE
run_example ./tiledb_array_read_sparse_filter_1 $1 21
run_example ./tiledb_array_iterator_sparse $1 22
run_example ./tiledb_array_iterator_sparse_filter $1 23
run_example ./tiledb_array_read_sparse_2 $1 24
run_example ./tiledb_array_read_sparse_filter_2 $1 25
run_example ./tiledb_array_write_sparse_2 $1 26
sleep 5
run_example ./tiledb_array_read_sparse_1 $1 27
run_example ./tiledb_array_read_sparse_2 $1 28
run_example ./tiledb_array_read_sparse_filter_2 $1 29
run_example ./tiledb_array_update_sparse_1 $1 30
sleep 5
run_example ./tiledb_array_read_sparse_1 $1 31
run_example ./tiledb_array_read_sparse_filter_1 $1 32
run_example ./tiledb_array_read_sparse_filter_2 $1 33
run_example ./tiledb_array_iterator_sparse $1 34
run_example ./tiledb_array_iterator_sparse_filter $1 35
run_example ./tiledb_array_read_sparse_2 $1 36
run_example ./tiledb_array_consolidate $1 37
sleep 5
run_example ./tiledb_array_read_dense_1 $1 38
run_example ./tiledb_array_read_sparse_1 $1 39
run_example ./tiledb_array_read_dense_2 $1 40
run_example ./tiledb_array_read_sparse_2 $1 41
sleep 5
run_example ./tiledb_array_3d $1 42
