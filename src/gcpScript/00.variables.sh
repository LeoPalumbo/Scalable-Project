#!/bin/bash

RUN_SESSION=20181122114000

GCP_PROJECT=scala-project

GCS_BUCKET_NAME=${GCP_PROJECT}-data-bucket
GCS_BUCKET_ZONE=europe-west1
GCS_BUCKET_CLASS=regional

GCS_SRC_BUCKET_NAME=${GCP_PROJECT}-src-bucket
GCS_SRC_BUCKET_ZONE=europe-west1
GCS_SRC_BUCKET_CLASS=regional

SCALA_JAR_FILENAME=covid-alignment.jar
SCALA_JAR_FILE=/Users/leonardopiopalumbo/Desktop/Università/Scalable-Project/target/scala-2.12/${SCALA_JAR_FILENAME}
SCALA_JAR_FILE_LOCALPATH=file://$(pwd)
SCALA_RUNNABLE_CLASS=main

#SCALA_JAR_FILE_FOR_JOB_SUBMIT=${SCALA_JAR_FILE_LOCALPATH}/${SCALA_JAR_FILE}
SCALA_JAR_FILE_FOR_JOB_SUBMIT=gs://${GCS_SRC_BUCKET_NAME}/${SCALA_JAR_FILENAME}

DATA_FILE=all-shakespeare-02.txt

DATAPROC_CLUSTER_NAME=scala-project-dataproc-cluster-${RUN_SESSION}
DATAPROC_CLUSTER_REGION=europe-west1
DATAPROC_CLUSTER_ZONE=europe-west1-d

#ARGS
PAR_METRICS=true
PAR_JOINING=false
METRIC=p
MAX_SEQUENCES_PER_FILE=2
#DATA_PATH=https://console.cloud.google.com/storage/browser/${GCS_BUCKET_NAME}/COVID-19_seqLunghe
#DATA_PATH=https://storage.cloud.google.com/scala-project-data-bucket/COVID-19_seqLunghe
DATA_PATH=gs://scala-project-data-bucket/COVID-19_seqLunghe
#VARIANTS
ALPHA=${DATA_PATH}/alpha/1646989737406.sequences.fasta
BETA=${DATA_PATH}/beta/1646989945496.sequences.fasta
DELTA=${DATA_PATH}/delta/1646989040908.sequences.fasta
GAMMA=${DATA_PATH}/gamma/1646990274551.sequences.fasta
GH_490R=${DATA_PATH}/GH_490R/1646990812510.sequences.fasta
LAMBDA=${DATA_PATH}/lambda/1646990517372.sequences.fasta
MU=${DATA_PATH}/mu/1646990621241.sequences.fasta
OMICRON=${DATA_PATH}/omicron/1646988200948.sequences.fasta


