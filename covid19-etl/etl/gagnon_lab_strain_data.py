from datetime import datetime
import os
from contextlib import closing
import csv
import requests
import boto3
import json

from etl import base
from utils.metadata_helper import MetadataHelper


def getDiffDaysSinceDataEpoch(newDate):
    diff = newDate - datetime(2020, 1, 22)
    return diff.days


class GAGNON_LAB_STRAIN_DATA(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        self.base_url = base_url
        self.access_token = access_token
        self.s3_bucket = s3_bucket
        self.nested_dict = {}
        self.s3_client = boto3.client("s3")
        self.url = "https://raw.githubusercontent.com/gagnonlab/ncov-data/master/gagnon_data.csv"
        self.expected_headers = [
            "countyFIPS",
            "date",
            "19A",
            "19B",
            "20A",
            "20A.EU2",
            "20B",
            "20C",
            "20D",
            "20E (EU1)",
            "20G",
            "20H/501Y.V2",
            "20I/501Y.V1",
            "20J/501Y.V3",
        ]
        self.column_headers = {
            "countyFIPS": 0,
            "date": 1,
            "19A": 2,
            "19B": 3,
            "20A": 4,
            "20A.EU2": 5,
            "20B": 6,
            "20C": 7,
            "20D": 8,
            "20E (EU1)": 9,
            "20G": 10,
            "20H/501Y.V2": 11,
            "20I/501Y.V1": 12,
            "20J/501Y.V3": 13,
        }

    def files_to_submissions(self):
        self.parse_mobility_file()

    def submit_metadata(self):
        self.publish_to_s3()

    def parse_mobility_file(self):
        # token = os.environ.get('GIT_AUTH_TOKEN')
        # if token is None:
        #     print("GIT_AUTH_TOKEN missing from env vars, No Strain data collected.")
        #     return None
        # headers = {'Authorization': 'token %s' % token}
        print("Getting data from {}".format(self.url))
        with closing(requests.get(self.url, stream=True)) as r:
            f = (line.decode("utf-8") for line in r.iter_lines())
            reader = csv.reader(f, delimiter=",", quotechar='"')
            headers = next(reader)

            assert (
                headers == self.expected_headers
            ), "CSV headers have changed (expected {}, got {}). We may need to update the ETL code".format(
                self.expected_headers, headers
            )

            print("Headers were checked and now parsing data")

            for row in reader:
                self.parse_row(row)

    def parse_row(self, row):

        if not row:  # ignore empty rows
            return

        if row[self.column_headers["countyFIPS"]] not in self.nested_dict:
            self.nested_dict[row[self.column_headers["countyFIPS"]]] = []

        self.nested_dict[row[self.column_headers["countyFIPS"]]].append(
            {
                "DE": getDiffDaysSinceDataEpoch(
                    datetime.strptime(row[self.column_headers["date"]], "%Y-%m-%d")
                ),
                "19A": row[self.column_headers["19A"]],
                "19B": row[self.column_headers["19B"]],
                "20A": row[self.column_headers["20A"]],
                "20A.EU2": row[self.column_headers["20A.EU2"]],
                "20B": row[self.column_headers["20B"]],
                "20C": row[self.column_headers["20C"]],
                "20D": row[self.column_headers["20D"]],
                "20E (EU1)": row[self.column_headers["20E (EU1)"]],
                "20G": row[self.column_headers["20G"]],
                "20H/501Y.V2": row[self.column_headers["20H/501Y.V2"]],
                "20I/501Y.V1": row[self.column_headers["20I/501Y.V1"]],
                "20J/501Y.V3": row[self.column_headers["20J/501Y.V3"]],
            }
        )

    def publish_to_s3(self):
        print("Uploading strain data json to S3...")
        self.s3_client.put_object(
            Body=str(json.dumps(self.nested_dict)),
            Bucket=self.s3_bucket,
            Key="gagnon_lab_strain_data.json",
        )
        print("Done!")
