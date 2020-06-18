#!/usr/bin/env bash

set -euxo pipefail

FILE="seir-forecast.ipynb"
if [ ! -f $FILE ]; then
  echo "$FILE not exist. Exiting..."
  exit 1
fi

echo "Running notebook $FILE..."
jupyter nbconvert --to notebook --inplace --execute "$FILE"

echo "Running top10 script..."
python generate_top10_plots.py

echo "Copying to S3 bucket..."
if [[ -n "$S3_BUCKET" ]]; then
  aws s3 cp "simulated_cases.txt" "$S3_BUCKET/simulated_cases.txt"
  aws s3 cp "observed_cases.txt" "$S3_BUCKET/observed_cases.txt"
  aws s3 cp "top10.txt" "$S3_BUCKET/top10.txt"
fi

#### 
## bayes-by-county big sim
####

cd ../covid19-notebooks/bayes-by-county/

echo "Running bayes-by-county..."
# sh run.sh <stan_model> <deaths_cutoff> <nIterations>
sh run.sh us_base 10 8000

# copy images to S3 under prefix "/bayes-by-county/"
echo "Copying to S3 bucket..."
if [[ -n "$S3_BUCKET" ]]; then
  aws s3 sync "./modelOutput/figures" "$S3_BUCKET/bayes-by-county/"
fi

### HERE is the directory structure:
#
# Matts-MacBook-Pro:figures mattgarvin$ ls -R
# 17031			17097			CountyCodeList.txt
# 17043			17197			Rt_All.png
# 
# ./17031:
# Rt.png			casesForecast.png	deathsForecast.png
# cases.png		deaths.png
# 
# ...