#!/bin/bash

# to run:
# export ACCESS_TOKEN=myaccesstoken
# bash ssr.sh path/to/dir

if [ -z "$1" ]
  then
    echo "Missing argument: path to directory containing SSR files to ETL"
    exit 1
fi

for filepath in $1/*; do
    S3_BUCKET=none JOB_NAME=SSR FILE_PATH=$filepath python covid19-etl/main.py || exit 1
done
