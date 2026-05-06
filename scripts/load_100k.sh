#!/bin/bash

LOG_FILE=/home/parth/bigdata_project/logs/2024q1_push_100k.log
CSV_FILE=/home/parth/bigdata_project/clean/smart_telemetry_100k_noheader.csv

echo "=== Push Started: $(date) ===" >> "$LOG_FILE"
START=$(date +%s.%N)

cd /home/parth/phoenix/bin || exit 1

./psql.py \
  -t SMART_TELEMETRY \
  -h SALT_BUCKET,SERIAL_NUMBER,EVENT_DATE,MODEL,CAPACITY_BYTES,DATACENTER,CLUSTER_ID,FAILURE,SMART_5_RAW,SMART_9_RAW,SMART_187_RAW,SMART_188_RAW,SMART_197_RAW,SMART_198_RAW,SMART_199_RAW \
  localhost \
  "$CSV_FILE" >> "$LOG_FILE" 2>&1

END=$(date +%s.%N)
DURATION=$(echo "$END - $START" | bc)

echo "Duration: $DURATION seconds" >> "$LOG_FILE"
echo "=== Push Completed: $(date) ===" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"
