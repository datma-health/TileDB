#!/bin/bash

# Error out on first failure
set -e

wget https://github.com/FiloSottile/mkcert/releases/download/v1.4.3/mkcert-v1.4.3-linux-amd64
mv mkcert-v1.4.3-linux-amd64 /usr/local/bin/mkcert
chmod +x /usr/local/bin/mkcert
TRUST_STORES=system,java,nss mkcert -install

CAROOT=. mkcert -cert-file cert.pem -key-file key.pem 0.0.0.0 localhost 127.0.0.1 azurite ::1