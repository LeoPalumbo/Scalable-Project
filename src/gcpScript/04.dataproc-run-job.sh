#!/bin/bash

. 00.variables.sh

./90.log-time.sh "RUNNING SPARK JOB '${SCALA_RUNNABLE_CLASS}' OVER '${DATA_PATH}' DATA FILE ON '${DATAPROC_CLUSTER_NAME}' CLUSTER ..."
~/google-cloud-sdk/bin/gcloud dataproc jobs submit spark --cluster ${DATAPROC_CLUSTER_NAME} --region ${DATAPROC_CLUSTER_REGION} \
      --class ${SCALA_RUNNABLE_CLASS} \
      --jars ${SCALA_JAR_FILE_FOR_JOB_SUBMIT} \
      -- $JAR_FILE $PAR_METRICS $PAR_JOINING $METRIC $MAX_SEQUENCES_PER_FILE $ALPHA $BETA $GAMMA
      #--num-workers 5
      #--bucket gs://scala-project-data-bucket/
      #gs://${GCS_BUCKET_NAME}/${DATA_FILE} gs://${GCS_BUCKET_NAME}/output/${RUN_SESSION}/
./90.log-time.sh "SPARK JOB '${SCALA_RUNNABLE_CLASS}' DONE!"
