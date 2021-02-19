#!/bin/bash

INSTALL_DIR=${INSTALL_DIR:-/usr}

# Install Zstandard - Not enabled - Seg faults when Code Coverage is ON
# echo "Installing ZStd..."
# pushd `pwd`
# wget https://github.com/facebook/zstd/archive/v1.0.0.tar.gz &&
#	tar xf v1.0.0.tar.gz &&
#	cd zstd-1.0.0 &&
#	sudo make install PREFIX=$INSTALL_DIR &&
#	export ENABLE_ZSTD=1
# popd
# echo "Installing ZStd DONE"
export ENABLE_ZSTD=0

# Install Blosc
echo "Installing Blosc..."
pushd `pwd`
git clone https://github.com/Blosc/c-blosc.git &&
	cd c-blosc &&
	mkdir build &&
	cd build &&
	cmake -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR .. &&
	cmake --build . &&
	sudo cmake --build . --target install &&
	export ENABLE_BLOSC=1
popd
echo "Installing Blosc DONE"

# Install OpenJPEG2000 library with memory stream support
pushd `pwd`
git clone https://github.com/OmicsDataAutomation/openjpeg.git &&
	cd openjpeg &&
	mkdir build &&
  cd build &&
	cmake -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR .. &&
  sudo make install &&
	export ENABLE_JPEG2K=1 &&
  export ENABLE_JPEG2K_RGB=1
popd
