#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <year>"
    exit 1
fi

YEAR=$1
INPUT_DIR=~/gharchive
OUTPUT_DIR=~/gharchive

for MONTH in $(seq -w 1 12); do
    INPUT_FILE="${INPUT_DIR}/${YEAR}-${MONTH}-*.json.gz"
    OUTPUT_FILE="${OUTPUT_DIR}/gharchive-${YEAR}-${MONTH}.csv"

    if [ -f "$OUTPUT_FILE" ]; then
        if [ ! -s "$OUTPUT_FILE" ]; then
            echo "Deleting empty file ${OUTPUT_FILE}."
            rm "$OUTPUT_FILE"
        else
            echo "Skipping ${OUTPUT_FILE}, already exists and is not empty."
            continue
        fi
    fi

    if [ "$YEAR" -lt 2015 ]; then
        echo "Using SQL for the old JSON schema for ${YEAR}-${MONTH}"
        SQL_TEMPLATE=$(<gharchive_monthly_old_json_version.sql)
    else
        SQL_TEMPLATE=$(<gharchive_monthly.sql)
    fi
    SQL="${SQL_TEMPLATE//\{\{INPUT_FILE\}\}/$INPUT_FILE}"
    SQL="${SQL//\{\{OUTPUT_FILE\}\}/$OUTPUT_FILE}"

    echo "Running for ${YEAR}-${MONTH}"
    echo "$SQL" | duckdb
done
