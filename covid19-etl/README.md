# COVID-19 Pandemic Response Commons ETL framework

## Structure

All the ETLs are written in Python. Each ETL is 1 file in the `covid19-tools/covid19-etl/etl/` folder. This file contain a class inheriting from the [base ETL class](./etl/base.py).

The ETL modules are automatically imported by [this code](./etl/__init__.py). The [main function](./main.py) assumes all the ETLs inherit from the base class: it calls the `files_to_submissions` function and then the `submit_metadata` function. The existence of these functions is enforced in the [tests](./tests/).

All the ETLs are initialized with a `base_url`, an `access_token` and an `s3_bucket`, even if each individual ETL doesn't necessarily need all of them.

## Running an ETL locally

In the root of the covid19-tools repo:
```
pip install -r covid19-etl-requirements.txt
export ACCESS_TOKEN=<access token>
JOB_NAME=<name of the ETL to run> S3_BUCKET=<bucket> python covid19-etl/main.py
```
- `JOB_NAME` is required
- `ACCESS_TOKEN` is required. If the ETL you are running does not need an access token, use a fake value
- `S3_BUCKET` is optional, but ETLs that upload files to S3 need it

## Adding a new ETL

1. Create a file in the `covid19-tools/covid19-etl/etl/` folder. The file name should be `<ETL identifier (lowercase)>.py`.
2. In this file, create an ETL child class as follows (replace the class name):
```
from etl import base

class <ETL identifier (uppercase)>(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        self.base_url = base_url
        self.access_token = access_token
        self.s3_bucket = s3_bucket

    def files_to_submissions(self):
        # ETL code that reads from the data source
        # and generates the data to submit/upload

    def submit_metadata(self):
        # submit/upload the transformed data
```
3. Write `files_to_submissions` and `submit_metadata` functions.
    - Example of streaming a CSV file from a URL row by row: [here](https://github.com/uc-cdis/covid19-tools/blob/2d8bed0243fad7c5adb382913e0252b68304aae5/covid19-etl/etl/jhu_to_s3.py#L339-L343) and [here](https://github.com/uc-cdis/covid19-tools/blob/2d8bed0243fad7c5adb382913e0252b68304aae5/covid19-etl/etl/jhu_to_s3.py#L365-L366)
    - Example of file upload to an S3 bucket: [here](https://github.com/uc-cdis/covid19-tools/blob/2d8bed0243fad7c5adb382913e0252b68304aae5/covid19-etl/etl/jhu_to_s3.py#L810)
4. In the root of the `covid19-tools` repo, you can run the following to make sure the format is correct:
```
pip install -r covid19-etl-requirements.txt
pip install -r test-requirements.txt
pip install pytest~=3.6
pytest -vv covid19-etl/tests
```
