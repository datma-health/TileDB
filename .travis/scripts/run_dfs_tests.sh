#!/bin/bash

tiledb_utils_tests() {
  $TILEDB_BUILD_DIR/test/test_tiledb_utils [initialize_workspace] --test-dir $1 &&
  $TILEDB_BUILD_DIR/test/test_tiledb_utils [create_workspace] --test-dir $1 &&
  $TILEDB_BUILD_DIR/test/test_tiledb_utils [array_exists] --test-dir $1 &&
  $TILEDB_BUILD_DIR/test/test_tiledb_utils [get_fragment_names] --test-dir $1 &&
  $TILEDB_BUILD_DIR/test/test_tiledb_utils [file_utils_multi_threads] --test-dir $1 &&
  $TILEDB_BUILD_DIR/test/test_tiledb_utils [file_ops] --test-dir $1 &&
  $TILEDB_BUILD_DIR/test/test_tiledb_utils [move_across_filesystems] --test-dir $1
}

if [[ $INSTALL_TYPE != basic ]]; then
  cd $TILEDB_BUILD_DIR && make examples && cd examples
  if [[ $INSTALL_TYPE == hdfs ]]; then
    echo "JAVA_HOME=$JAVA_HOME"
    java -version
    tiledb_utils_tests "hdfs://localhost:9000/travis_unit_test" &&
    time $TRAVIS_BUILD_DIR/examples/run_examples.sh "hdfs://localhost:9000/travis_test"
  elif [[ $INSTALL_TYPE == gcs ]]; then
    export GOOGLE_APPLICATION_CREDENTIALS=$TRAVIS_BUILD_DIR/.travis/resources/gcs/GCS.json
    echo "Listing $GS_BUCKET"; hdfs dfs -ls gs://$GS_BUCKET/; echo "Listing $GS_BUCKET DONE"
    tiledb_utils_tests "gs://$GS_BUCKET/travis_unit_test" &&
    time $TRAVIS_BUILD_DIR/examples/run_examples.sh "gs://$GS_BUCKET/travis_test"
  elif [[ $INSTALL_TYPE == azure ]]; then
    source $TRAVIS_BUILD_DIR/.travis/resources/azure/azure_cred.sh
    echo $AZURE_STORAGE_ACCOUNT
    echo "wasbs schema utils test" && tiledb_utils_tests "wasbs://$AZURE_CONTAINER_NAME@$AZURE_ACCOUNT_NAME.blob.core.windows.net/travis_unit_test" &&
    echo "az schema utils test" && tiledb_utils_tests "az://$AZURE_CONTAINER_NAME@$AZURE_ACCOUNT_NAME.blob.core.windows.net/travis_azure_blob_test" &&
    echo "az schema storage test" && test_azure_blob_storage --test-dir "az://$AZURE_CONTAINER_NAME@$AZURE_ACCOUNT_NAME.blob.core.windows.net/travis_azure_storage_test" &&
    echo "az schema examples" && time $TRAVIS_BUILD_DIR/examples/run_examples.sh "az://$AZURE_CONTAINER_NAME@$AZURE_ACCOUNT_NAME.blob.core.windows.net/travis_test"
#    echo "wasbs schema examples" && time $TRAVIS_BUILD_DIR/examples/run_examples.sh "wasbs://$AZURE_CONTAINER_NAME@$AZURE_ACCOUNT_NAME.blob.core.windows.net/travis_test"
  fi
  if [[ -f travis_test.log ]]; then
    diff travis_test.log $TRAVIS_BUILD_DIR/examples/expected_results
  else
    echo "travis_test.log from run_examples.sh does not seem to be found"
    exit 1
  fi
fi
