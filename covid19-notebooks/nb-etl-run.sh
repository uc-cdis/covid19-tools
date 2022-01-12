#!/usr/bin/env bash

set -euxo pipefail

if [[ -z "${S3_BUCKET-}" ]]; then
  echo "No S3 bucket provided (use env var S3_BUCKET)"
  exit 1
fi

# run Python notebooks and push outputs to S3

# ===> commenting out `seir-forecast` - broken and unsused
# FILE="seir-forecast.ipynb"
# if [ ! -f $FILE ]; then
#   echo "$FILE not exist. Exiting..."
#   exit 1
# fi
# echo "Running notebook $FILE..."
# jupyter nbconvert --to notebook --inplace --execute "$FILE"

# run R bayes-by-county simulation and push outputs to S3
echo "Running bayes-by-county..."
cd /nb-etl/bayes-by-county/

# sh run.sh <stan_model> <deaths_cutoff> <nIterations>
sh run.sh us_mobility 10 200
echo "Done!"

# copy images to S3 under prefix "bayes-by-county"
# directory structure:
#   bayes-by-county/
#     17031/ (FIPS)
#       cases.png
#       casesForecast.png
#       deaths.png
#       deathsForecast.png
#       Rt.png
#     <more FIPS folders>
#     CountyCodeList.txt
#     Rt_All.png

echo "Copying to S3 bucket..."
if [[ -n "$S3_BUCKET" ]]; then
  # don't copy over the .keep (or any non-image or county list) file
  aws s3 sync "./modelOutput/figures" "$S3_BUCKET/bayes-by-county/" --exclude ".keep"
fi
