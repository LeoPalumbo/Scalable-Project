#!/bin/bash

. 00.variables.sh

~/google-cloud-sdk/bin/gsutil mb -c ${GCS_BUCKET_CLASS} -l ${GCS_BUCKET_ZONE} gs://${GCS_BUCKET_NAME}
~/google-cloud-sdk/bin/gsutil mb -c ${GCS_SRC_BUCKET_CLASS} -l ${GCS_SRC_BUCKET_ZONE} gs://${GCS_SRC_BUCKET_NAME}
