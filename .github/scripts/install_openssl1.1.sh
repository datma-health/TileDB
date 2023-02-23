#!/bin/bash

INSTALL_PREFIX=/usr/local
OPENSSL_VERSION=1.1.1o

OPENSSL_PREFIX=$INSTALL_PREFIX

echo "Installing OpenSSL"

pushd /tmp
wget -nv https://www.openssl.org/source/openssl-$OPENSSL_VERSION.tar.gz &&
tar -xvzf openssl-$OPENSSL_VERSION.tar.gz &&
cd openssl-$OPENSSL_VERSION &&
CFLAGS=-fPIC ./config shared --prefix=$OPENSSL_PREFIX --openssldir=$OPENSSL_PREFIX
make && make install && echo "Installing OpenSSL DONE"
rm -fr /tmp/openssl*
popd

export OPENSSL_ROOT_DIR=$OPENSSL_PREFIX

