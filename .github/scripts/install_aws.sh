#!/bin/bash

# Error out on first failure
set -e

gpg --quiet --batch --yes --decrypt --passphrase=$AWS_TAR --output ${HOME}/aws.tar $GITHUB_WORKSPACE/.github/scripts/aws.tar.gpg
tar xvf ${HOME}/aws.tar -C $HOME

# Install aws cli for sanity checking
if [[ ! -n $(which aws) ]]; then
  sudo apt-get -q update
  sudo apt-get -y install awscli
fi

# Sanity check
echo "aws version=`aws --version`"




