#!/bin/bash
set -ev

# Install OpenJPEG2000 library with memory stream support 

if [[ $INSTALL_TYPE != basic && $TRAVIS_OS_NAME == linux ]]; then

   git clone -b clay-dev https://github.com/OmicsDataAutomation/openjpeg.git

   pushd openjpeg
   mkdir build ; cd build
   cmake ..
   make clean
   make 
   sudo make install
   
   echo "OpenJPEG2000 library installed successfully"
   
   popd
fi
