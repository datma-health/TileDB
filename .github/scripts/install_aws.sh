#!/bin/bash

# Error out on first failure
set -e

# Install aws cli for sanity checking
if [[ ! -n $(which aws) ]]; then
  sudo apt-get -q update
  sudo apt-get -y install awscli
fi

# Sanity check
echo "aws version=`aws --version`"




