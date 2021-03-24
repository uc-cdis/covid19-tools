#!/usr/bin/env bash

set -euxo pipefail

# run Python notebooks and push outputs to S3

# ===> commenting out `seir-forecast` - broken and unsused
# FILE="seir-forecast.ipynb"
# if [ ! -f $FILE ]; then
#   echo "$FILE not exist. Exiting..."
#   exit 1
# fi
# echo "Running notebook $FILE..."
# jupyter nbconvert --to notebook --inplace --execute "$FILE"

echo "Running top10 script..."
python3 generate_top10_plots.py

echo "Copying to S3 bucket..."
if [[ -n "$S3_BUCKET" ]]; then
#   aws s3 cp "simulated_cases.txt" "$S3_BUCKET/simulated_cases.txt"
#   aws s3 cp "observed_cases.txt" "$S3_BUCKET/observed_cases.txt"
  aws s3 cp "top10.txt" "$S3_BUCKET/top10.txt"
fi
