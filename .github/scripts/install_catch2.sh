#
# install_catch2.sh
#
# The MIT License
#
# Copyright (c) 2022 Omics Data Automation, Inc.
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

#!/bin/bash

# Error out on first failure
set -e

INSTALL_DIR=${INSTALL_DIR:-/usr/local}
CATCH2_VER=${CATCH2_VER:-v3.1.0}

if [[ $INSTALL_DIR == "/usr/local" ]]; then
  SUDO="sudo"
  INSTALL_PREFIX=""
else
  SUDO=""
  INSTALL_PREFIX="-DCMAKE_INSTALL_PREFIX=$INSTALL_DIR"
fi

if [[ ! -d $INSTALL_DIR/include/catch2 ]]; then
  git clone https://github.com/catchorg/Catch2.git -b $CATCH2_VER
  cd Catch2
  cmake -Bbuild -H. -DBUILD_TESTING=OFF $INSTALL_PREFIX
  $SUDO cmake --build build/ --target install
fi
