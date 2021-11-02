#!/usr/bin/env bash

echo "Running pymc3_generative_model..."
python3 pymc3_generative_model.py;

echo "Copying results to S3 bucket..."
if [[ -n "$S3_BUCKET" ]]; thenm
    aws s3 cp "*.svg" "$S3_BUCKET/generative_bayes_model/"
else
    echo "Copying to s3 failed due to S3_BUCKET env_var not being set"
fi
