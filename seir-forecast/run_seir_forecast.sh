#!/usr/bin/env bash

set -euxo pipefail

FILE="seir-forecast.ipynb"
if [ ! -f $FILE ]; then
  echo "$FILE not exist. Exiting..."
  exit 1
fi

echo "Running notebook $FILE..."
jupyter nbconvert --to notebook --inplace --execute "$FILE"

cat simulated_seir.txt

echo "Copying to S3 bucket..."
if [[ -n "$S3_BUCKET" ]]; then
  aws s3 cp "simulated_cases.txt" "$S3_BUCKET/simulated_cases.txt"
  aws s3 cp "observed_cases.txt" "$S3_BUCKET/observed_cases.txt"
fi
