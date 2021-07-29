#!/bin/bash

INSTALL_DIR=${INSTALL_DIR:-/usr}

# Install ZStd 
sudo apt-get install -y libzstd-dev &&
export ENABLE_ZSTD=1

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
