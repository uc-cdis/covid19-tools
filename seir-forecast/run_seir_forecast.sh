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
UPLOAD_FILE="simulated_seir.txt"
if [[ -n "$S3_BUCKET" ]]; then
  cat $UPLOAD_FILE
  aws s3 cp "$FILE" "$S3_BUCKET/$UPLOAD_FILE"
fi
