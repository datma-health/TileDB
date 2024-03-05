#!/bin/bash

# Error out on first failure
set -e

setup_env() {
  export AWS_ACCESS_KEY_ID=minio_s3 > $HOME/aws_env.sh
  export AWS_SECRET_ACCESS_KEY=minio_s3_secret_key >> $HOME/aws_env.sh
  export AWS_DEFAULT_REGION=us-west-2 >> $HOME/aws_env.sh
  export MINIO_ACCESS_KEY=$AWS_ACCESS_KEY_ID >> $HOME/aws_env.sh
  export MINIO_SECRET_KEY=$AWS_SECRET_ACCESS_KEY >> $HOME/aws_env.sh

  export AWS_CA_BUNDLE=${HOME}/.minio/certs/cert.pem >> $HOME/aws_env.sh
  export AWS_ENDPOINT_URL=https://127.0.0.1:9000 >> $HOME/aws_env.sh
  export AWS_ENDPOINT_OVERRIDE=https://127.0.0.1:9000 >> $HOME/aws_env.sh
}

sudo wget -nv -P /usr/local/bin https://dl.min.io/server/minio/release/linux-amd64/minio
sudo chmod +x /usr/local/bin/minio

setup_env
source $HOME/aws_env.sh

# same certificates as used for azurite
mkdir -p ${HOME}/.minio/certs
mv cert.pem ${HOME}/.minio/certs/cert.pem
mv key.pem ${HOME}/.minio/certs/key1.pem
cp ${HOME}/.minio/certs/cert.pem ${HOME}/.minio/certs/public.crt
cp ${HOME}/.minio/certs/key1.pem ${HOME}/.minio/certs/private.key
sudo cp ${HOME}/.minio/certs/public.crt /usr/local/share/ca-certificates/ca-certificates.crt
sudo update-ca-certificates

# Start minio
mkdir -p /tmp/minio/data
minio server /tmp/minio/data &

# Test with curl
sleep 10
echo "Testing with curl..."
curl -vsSL --tlsv1.2 -X GET $AWS_ENDPOINT_URL
echo "Testing with curl DONE"

if [[ ! -n $(which aws) ]]; then
  sudo apt-get -q update
  sudo apt-get -y install awscli 
fi

echo "aws version=`aws --version`"
echo "Listing Buckets..."
aws configure set default.s3.signature_version s3v4
aws --endpoint-url $AWS_ENDPOINT_URL s3 ls
aws --endpoint-url $AWS_ENDPOINT_URL s3 mb s3://test
aws --endpoint-url $AWS_ENDPOINT_URL s3 ls
echo "Listing Buckets DONE"
