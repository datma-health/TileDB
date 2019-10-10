#!/bin/bash
set -ev

# Install OpenJPEG2000 library with memory stream support 
pushd ~
git clone -b clay-dev https://github.com/OmicsDataAutomation/openjpeg.git

cd openjpeg
mkdir build ; cd build
cmake ..
make clean
make 
sudo make install
   
echo "OpenJPEG2000 library installed successfully"

popd
