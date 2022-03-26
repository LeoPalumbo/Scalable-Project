#!/bin/bash

JAR_FILE=./target/scala-2.12/HelloWorldSpark-assembly-1.0.jar
MAIN_CLASS=main
PAR_METRICS="$1"
PAR_JOINING="$2"
METRIC="$3"
MAX_SEQUENCES_PER_FILE="$4"

echo "================================================="
echo "PARAMS:"
echo "================================================="
echo "> path jar            -> $JAR_FILE"
echo "> mainclass           -> $MAIN_CLASS"
echo "> parMetrics          -> $PAR_METRICS"
echo "> parJoining          -> $PAR_JOINING"
echo "> metric              -> $METRIC"
echo "> maxSequencesPerFile -> $MAX_SEQUENCES_PER_FILE"
echo "================================================="
echo "STARTING..."
echo "================================================="

spark-submit --class $MAIN_CLASS $JAR_FILE $PAR_METRICS $PAR_JOINING $METRIC $MAX_SEQUENCES_PER_FILE

echo "==========================="
echo "ENDING..."
echo "==========================="