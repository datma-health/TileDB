#!/bin/bash

tiledb_utils_tests() {
  $CMAKE_BUILD_DIR/test/test_tiledb_utils [initialize_workspace] --test-dir $1 &&
  $CMAKE_BUILD_DIR/test/test_tiledb_utils [create_workspace] --test-dir $1 &&
  $CMAKE_BUILD_DIR/test/test_tiledb_utils [array_exists] --test-dir $1 &&
  $CMAKE_BUILD_DIR/test/test_tiledb_utils [get_fragment_names] --test-dir $1 &&
  $CMAKE_BUILD_DIR/test/test_tiledb_utils [file_utils_multi_threads] --test-dir $1 &&
  $CMAKE_BUILD_DIR/test/test_tiledb_utils [file_ops] --test-dir $1 &&
  $CMAKE_BUILD_DIR/test/test_tiledb_utils [move_across_filesystems] --test-dir $1
}

setup_azurite() {
  # TODO: Ignoring "SSL SecurityWarning: Certificate has no subjectAltName", see https://docs.oracle.com/cd/E52668_01/E66514/html/ceph-issues-24424028.html for a fix
  export AZURE_CLI_DISABLE_CONNECTION_VERIFICATION=anycontent
  export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

  # Create container called test as TileDB expects the container to be already created
  az storage container create -n test --connection-string "DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=https://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=https://127.0.0.1:10001/devstoreaccount1;"

  # Env to run tests
  export AZURE_STORAGE_ACCOUNT=devstoreaccount1
  export AZURE_STORAGE_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
  export AZURE_BLOB_ENDPOINT="https://127.0.0.1:10000/devstoreaccount1"
}

make -j4 && make test_tiledb_utils && make test_azure_blob_storage && make test_s3_storage && make examples && cd examples

TEST=github_test_$RANDOM
if [[ $INSTALL_TYPE == hdfs ]]; then
  echo "JAVA_HOME=$JAVA_HOME"
  java -version
  echo "1 CLASSPATH=$CLASSPATH"
  tiledb_utils_tests "hdfs://localhost:9000/$TEST" &&
    time $GITHUB_WORKSPACE/examples/run_examples.sh "hdfs://localhost:9000/$TEST"

elif [[ $INSTALL_TYPE == gcs ]]; then
  tiledb_utils_tests "gs://$GS_BUCKET/$TEST" &&
    time  $GITHUB_WORKSPACE/examples/run_examples.sh "gs://$GS_BUCKET/$TEST"

elif [[ $INSTALL_TYPE == azure ]]; then
  export AZURE_CONTAINER_NAME="build"
  #echo "Listing Azure Container $AZURE_CONTAINER_NAME@$AZURE_STORAGE_ACCOUNT";hdfs dfs -ls wasbs://$AZURE_CONTAINER_NAME@$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/; echo "Listing Azure DONE"
  #echo "wasbs schema utils test" && tiledb_utils_tests "wasbs://$AZURE_CONTAINER_NAME@$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/github_unit_test" &&
    echo "az schema utils test" && tiledb_utils_tests "az://$AZURE_CONTAINER_NAME@$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$TEST" &&
    echo "az schema storage test" && $CMAKE_BUILD_DIR/test/test_azure_blob_storage --test-dir "az://$AZURE_CONTAINER_NAME@$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$TEST" &&
    echo "az schema examples" && time $GITHUB_WORKSPACE/examples/run_examples.sh "az://$AZURE_CONTAINER_NAME@$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$TEST"
    #    echo "wasbs schema examples" && time  $GITHUB_WORKSPACE/examples/run_examples.sh "wasbs://$AZURE_CONTAINER_NAME@$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$TEST"

elif [[ $INSTALL_TYPE == azurite ]]; then
  setup_azurite
  tiledb_utils_tests "az://test@devstoreaccount1.blob.core.windows.net/$TEST" &&
  $CMAKE_BUILD_DIR/test/test_azure_blob_storage --test-dir "az://test@devstoreaccount1.blob.core.windows.net/$TEST" &&
  $GITHUB_WORKSPACE/examples/run_examples.sh "az://test@devstoreaccount1.blob.core.windows.net/$TEST"

elif [[ $INSTALL_TYPE == aws ]]; then
  echo "Testing aws type"
  $CMAKE_BUILD_DIR/test/test_s3_storage --test-dir s3://github-actions-1/$TEST &&
  $GITHUB_WORKSPACE/examples/run_examples.sh s3://github-actions-1/$TEST

elif [[  $INSTALL_TYPE == minio ]]; then
  source $HOME/aws_env.sh &&
  $CMAKE_BUILD_DIR/test/test_s3_storage --test-dir s3://test/$TEST &&
  $GITHUB_WORKSPACE/examples/run_examples.sh s3://test/$TEST
fi

if [[ -f $TEST.log ]]; then
  diff $TEST.log  $GITHUB_WORKSPACE/examples/expected_results
else
  echo "$TEST.log from run_examples.sh does not seem to exist. Check the results of running run_examples.sh"
  exit 1
fi
