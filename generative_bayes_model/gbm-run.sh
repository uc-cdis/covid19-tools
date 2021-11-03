#!/usr/bin/env bash

if [[ -z "${S3_BUCKET-}" ]]; then
    echo "No S3 bucket provided (use env var S3_BUCKET)"
    exit 1
fi

echo "Running pymc3_generative_model..."
python3 pymc3_generative_model.py;

echo "Copying results to S3 bucket..."
if [[ -n "$S3_BUCKET" ]]; then
    aws s3 sync . $S3_BUCKET/generative_bayes_model/ --exclude "*" --include "*.svg"
fi
