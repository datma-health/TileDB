#!/bin/bash

# Error out on first failure
set -e

gpg --quiet --batch --yes --decrypt --passphrase=$AWS_TAR --output ${HOME}/aws.tar $GITHUB_WORKSPACE/.github/scripts/aws.tar.gpg
tar xvf ${HOME}/aws.tar -C $HOME

# Install aws cli for sanity checking
pushd /tmp
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -s -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
popd

# Sanity check
echo "aws version=`aws --version`"




