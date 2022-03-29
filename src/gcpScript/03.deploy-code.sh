#!/bin/bash

. 00.variables.sh

~/google-cloud-sdk/bin/gsutil cp ${SCALA_JAR_FILE} gs://${GCS_SRC_BUCKET_NAME}/


