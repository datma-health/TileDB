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

check_results_from_examples() {
   if [[ -f $TEST.log ]]; then
      if diff $TEST.log  $GITHUB_WORKSPACE/examples/expected_results; then
        exit 0
      else
        echo "$TEST.log from run_examples.sh is different from expected results"
        exit 1
      fi
    else
      echo "$TEST.log from run_examples.sh does not seem to exist. Check the results of running run_examples.sh"
      exit 1
    fi
}

run_azure_tests() {
  source $1
  echo "Running with $1"
  echo "az schema utils test" && tiledb_utils_tests "az://$AZURE_CONTAINER_NAME@$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$TEST" &&
    echo "az schema storage test" && $CMAKE_BUILD_DIR/test/test_azure_blob_storage --test-dir "az://$AZURE_CONTAINER_NAME@$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$TEST" &&
    echo "az schema storage buffer test" && $CMAKE_BUILD_DIR/test/test_storage_buffer --test-dir "az://$AZURE_CONTAINER_NAME@$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$TEST" &&
    echo "az schema examples" && time $GITHUB_WORKSPACE/examples/run_examples.sh "az://$AZURE_CONTAINER_NAME@$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$TEST" &&
    echo "Running with $1 DONE" &&
    check_results_from_examples
}

make -j4 &&
make test_tiledb_utils &&
make test_azure_blob_storage &&
make test_sparse_array_benchmark &&
make test_s3_storage &&
make test_gcs_storage &&
make test_storage_buffer
make examples

if [ $? -ne 0 ]; then
  echo "Errors encountered during TileDB build"
  exit 1
fi

cd examples

TEST=github_test_$RANDOM
if [[ $INSTALL_TYPE == hdfs ]]; then
  echo "JAVA_HOME=$JAVA_HOME"
  java -version
  echo "1 CLASSPATH=$CLASSPATH"
  tiledb_utils_tests "hdfs://localhost:9000/$TEST" &&
    time $GITHUB_WORKSPACE/examples/run_examples.sh "hdfs://localhost:9000/$TEST"

elif [[ $INSTALL_TYPE == gcs ]]; then
  tiledb_utils_tests "gs://$GS_BUCKET/$TEST" &&
    $CMAKE_BUILD_DIR/test/test_gcs_storage --test-dir "gs://$GS_BUCKET/$TEST" &&
    $CMAKE_BUILD_DIR/test/test_storage_buffer --test-dir "gs://$GS_BUCKET/$TEST" &&
    $GITHUB_WORKSPACE/examples/run_examples.sh "gs://$GS_BUCKET/$TEST" &&
    echo "Running GCS_HDFS_CONNECTOR tests..." &&
    time TILEDB_USE_GCS_HDFS_CONNECTOR=1 $GITHUB_WORKSPACE/examples/run_examples.sh "gs://$GS_BUCKET/hdfs_$TEST" &&
    echo "Running GCS_HDFS_CONNECTOR tests DONE"

elif [[ $INSTALL_TYPE == azure ]]; then
  export AZURE_CONTAINER_NAME="build"
  CHECK_RESULTS=(-1 -1)
  run_azure_tests $GITHUB_WORKSPACE/.github/resources/azure/azure_cred.sh 0 & pids[0]=$!
  TEST=github_test_$RANDOM_adls run_azure_tests $GITHUB_WORKSPACE/.github/resources/azure/azure_cred_adls.sh 1 & pids[1]=$!
  wait ${pids[0]}
  CHECK_RESULTS[0]=$?
  wait ${pids[1]}
  CHECK_RESULTS[1]=$?
  if [[ ${CHECK_RESULTS[0]} != 0 || ${CHECK_RESULTS[1]} != 0 ]]; then
    echo "Failure in some Azure tests: ${CHECK_RESULTS[0]}  ${CHECK_RESULTS[1]}"
    exit 1
  else
    exit 0
  fi

elif [[ $INSTALL_TYPE == azurite ]]; then
  setup_azurite
  tiledb_utils_tests "az://test@devstoreaccount1.blob.core.windows.net/$TEST" &&
  $CMAKE_BUILD_DIR/test/test_azure_blob_storage --test-dir "az://test@devstoreaccount1.blob.core.windows.net/$TEST" &&
  $CMAKE_BUILD_DIR/test/test_storage_buffer --test-dir "az://test@devstoreaccount1.blob.core.windows.net/$TEST" &&
  $GITHUB_WORKSPACE/examples/run_examples.sh "az://test@devstoreaccount1.blob.core.windows.net/$TEST"

elif [[ $INSTALL_TYPE == aws ]]; then
  TILEDB_BENCHMARK=1
  tiledb_utils_tests "s3://github-actions-1/$TEST" &&
  $CMAKE_BUILD_DIR/test/test_s3_storage --test-dir s3://github-actions-1/$TEST &&
  $CMAKE_BUILD_DIR/test/test_storage_buffer --test-dir s3://github-actions-1/$TEST &&
  $CMAKE_BUILD_DIR/test/test_sparse_array_benchmark --test-dir s3://github-actions-1/$TEST &&
  $GITHUB_WORKSPACE/examples/run_examples.sh s3://github-actions-1/$TEST

elif [[  $INSTALL_TYPE == minio ]]; then
  source $HOME/aws_env.sh &&
  $CMAKE_BUILD_DIR/test/test_s3_storage --test-dir s3://test/$TEST &&
  $GITHUB_WORKSPACE/examples/run_examples.sh s3://test/$TEST
fi

check_results_from_examples
