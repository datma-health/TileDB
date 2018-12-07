#!/bin/bash

if [[ $INSTALL_TYPE != basic ]]; then
	cd $TILEDB_BUILD_DIR && make examples && cd examples
	if [[ $INSTALL_TYPE == hdfs ]]; then
		$TRAVIS_BUILD_DIR/examples/run_examples.sh "hdfs://localhost:9000/travis_test"
	elif [[ $INSTALL_TYPE == gcs ]]; then
		GOOGLE_APPLICATION_CREDENTIALS=$TRAVIS_BUILD_DIR/.travis/resources/gcs/GCS.json $TRAVIS_BUILD_DIR/examples/run_examples.sh "gs://$GS_BUCKET/travis_test"
	elif [[ $INSTALL_TYPE == azure ]]; then
		$TRAVIS_BUILD_DIR/examples/run_examples.sh "wasbs://$AZURE_CONTAINER_NAME@$AZURE_ACCOUNT_NAME.blob.core.windows.net/travis_test"
	fi
	if [[ ! $! -eq 0 ]]; then
        echo "Something wrong! run_examples.sh returned status code = $!"
        exit 1
    else
		diff travis_test.log $TRAVIS_BUILD_DIR/examples/expected_results
	fi
fi
