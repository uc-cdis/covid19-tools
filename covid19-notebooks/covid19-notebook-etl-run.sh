#!/usr/bin/env bash

set -euxo pipefail

if [[ -z "${S3_BUCKET-}" ]]; then
  echo "No S3 bucket provided (use env var S3_BUCKET)"
  exit 1
fi

# run Python notebooks and push outputs to S3


NOTEBOOKS=(
  IL_tab_charts.ipynb
)
for file in "${NOTEBOOKS[@]}"; do
  if [ ! -f $file ]; then
    echo "$file does not exist. Exiting"
    exit 1
  fi
  echo "Running notebook $file..."
  jupyter nbconvert --to notebook --inplace --execute "$file"
done

echo "Copying to S3 bucket..."
for file in IL_tab_charts*.svg; do
  aws s3 cp $file $S3_BUCKET/charts_data/
done
