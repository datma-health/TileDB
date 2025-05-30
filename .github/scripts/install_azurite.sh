#!/bin/bash

# Error out on first failure
set -e

sudo apt-get -y install nodejs
echo "node version = `node --version`"
sudo npm install -g azurite
AZURITE_DIR=$INSTALL_DIR/azurite
mkdir $AZURITE_DIR
ls $AZURITE_DIR
mv cert.pem ${AZURITE_DIR}/cert.pem
mv key.pem ${AZURITE_DIR}/key1.pem
sudo cp $AZURITE_DIR/cert.pem /usr/local/share/ca-certificates/ca-certificates.crt
sudo update-ca-certificates
which azurite

# Start azurite
azurite --location $AZURITE_DIR --cert $AZURITE_DIR/cert.pem --key $AZURITE_DIR/key1.pem &
