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
