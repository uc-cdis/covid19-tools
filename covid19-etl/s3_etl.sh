#!/usr/bin/env bash

set -euxo pipefail

python3 s3_etl_main.py

echo "Copying to S3 bucket..."
if [[ -n "$S3_BUCKET" ]]; then
  aws s3 cp "jhu_geojson.json" "$S3_BUCKET/map_data/jhu_geojson.json"
fi
