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


check_rc() {
  if [[ $# -eq 1 ]]; then
    if [[ $1 -ne 0 ]]; then
      echo
      echo "Exit Status=$1. Quitting execution of run_examples.sh"
      exit $1
    fi
  fi
}

log_filename() {
  echo Number of args = $#
  echo $0
  echo $1
  if [[ $# -ge 1 ]]
  then
    if [[ $(basename $0) == "run_examples_parallel.sh" ]]
    then
      export LOGFILENAME=`basename $1`.parallel.log
    else
      export LOGFILENAME=`basename $1`.log
    fi
  else
    if [[ $(basename $0) == "run_examples_parallel.sh" ]]
    then
      export LOGFILENAME=parallel.log
    else
      export LOGFILENAME=log
    fi
  fi
}

run_example() {
  if [[ $# -eq 3 ]]
  then
    logfile=`basename $2`.log
    echo "Example $3: Running $1..." | tee -a $LOGFILENAME
    $1 $2 | tee -a $LOGFILENAME
    check_rc ${PIPESTATUS[0]}
    echo "Example $3: Done running $1" | tee -a $LOGFILENAME
  else
    echo "Example $2: Running $1..." | tee -a $LOGFILENAME
    $1 | tee -a $LOGFILENAME
    check_rc ${PIPESTATUS[0]}
    echo "Example $2: Done running $1" | tee -a $LOGFILENAME
  fi
}

log_filename $@
rm -f $LOGFILENAME
