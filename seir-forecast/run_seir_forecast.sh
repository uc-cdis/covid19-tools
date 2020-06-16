#!/usr/bin/env bash

set -euxo pipefail

FILE="seir-forecast.ipynb"
if [ ! -f $FILE ]; then
  echo "$FILE not exist. Exiting..."
  exit 1
fi

echo "Running notebook $FILE..."
jupyter nbconvert --to notebook --inplace --execute "$FILE"

echo "Copying to S3 bucket..."
if [[ -n "$S3_BUCKET" ]]; then
  aws s3 cp "simulated_cases.txt" "$S3_BUCKET/simulated_cases.txt"
  aws s3 cp "observed_cases.txt" "$S3_BUCKET/observed_cases.txt"
fi

#### 
## bayes-by-county big sim
####

cd ../covid19-notebooks/bayes-by-county/

echo "Running bayes-by-county..."

# sh run.sh <stan_model> <deaths_cutoff> <nIterations>
# 100 iterations just to see that things run without error
sh run.sh us_base 10 100
# later -> actual simulation -> 16,000 iterations (somewhat arbitrary, but "big enough")
# sh run.sh us_base 10 16000

# copy images to S3 under prefix "/bayes-by-county/"
echo "Copying to S3 bucket..."
if [[ -n "$S3_BUCKET" ]]; then
  aws s3 sync "./modelOutput/figures" "$S3_BUCKET/bayes-by-county/"
fi

### HERE is the directory structure:
#
# Matts-MacBook-Pro:bayes-by-county mattgarvin$ ls -R modelOutput/figures
#  Cook		DuPage		Lake		Rt_All.png
# 
# modelOutput/figures/Cook:
# Rt.png			casesForecast.png	deathsForecast.png
# cases.png
#
# ...
