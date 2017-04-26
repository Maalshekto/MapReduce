#!/bin/bash
DATETIME=$(date +%Y%m%d_%H%M%S)
OUTPUT_FOLDER="reservation_output_$DATE"
INPUT_FILE="International_airline_activity_Australia.csv"
OUTPUT_FILE_HDFS="part-r-00000"
OUTPUT_FILE="reservation_$DATE.csv"
yarn jar wordCount-0.0.6-SNAPSHOT.jar formation.bigdata.com.reservation.MyLauncher $INPUT_FILE $OUTPUT_FOLDER
hget $OUTPUT_FOLDER/$OUTPUT_FILE_HDFS $OUTPUT_FILE
