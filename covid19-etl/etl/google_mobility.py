from datetime import datetime
import os
from contextlib import closing
import csv
import requests
import boto3
import json

from etl import base
from utils.metadata_helper import MetadataHelper

"""
getting data in the below format for the mobility json

{
    <county_code> : [
        {
        "date": date,
        "days_elapsed": int,
        "retail_and_recreation_percent_change_from_baseline": int,
        "grocery_and_pharmacy_percent_change_from_baseline": int,
        "parks_percent_change_from_baseline": int,
        "transit_stations_percent_change_from_baseline": int,
        "workplaces_percent_change_from_baseline": int,
        "residential_percent_change_from_baseline": int
        },
        ...
    ]
}
"""


def getDiffDaysSinceDataEpoch(newDate):
    diff = newDate - datetime(2020, 1, 22)
    return diff.days


class GOOGLE_MOBILITY(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        self.base_url = base_url
        self.access_token = access_token
        self.s3_bucket = s3_bucket
        self.nested_dict = {}
        self.s3_client = boto3.client("s3")
        self.url = "https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"
        self.expected_headers = [
            "country_region_code",
            "country_region",
            "sub_region_1",
            "sub_region_2",
            "metro_area",
            "iso_3166_2_code",
            "census_fips_code",
            "place_id",
            "date",
            "retail_and_recreation_percent_change_from_baseline",
            "grocery_and_pharmacy_percent_change_from_baseline",
            "parks_percent_change_from_baseline",
            "transit_stations_percent_change_from_baseline",
            "workplaces_percent_change_from_baseline",
            "residential_percent_change_from_baseline",
        ]
        self.column_headers = {
            "census_fips_code": 6,
            "place_id": 7,
            "date": 8,
            "retail_and_recreation_percent_change_from_baseline": 9,
            "grocery_and_pharmacy_percent_change_from_baseline": 10,
            "parks_percent_change_from_baseline": 11,
            "transit_stations_percent_change_from_baseline": 12,
            "workplaces_percent_change_from_baseline": 13,
            "residential_percent_change_from_baseline": 14,
        }

    def files_to_submissions(self):
        self.parse_mobility_file()

    def submit_metadata(self):
        self.publish_to_s3()

    def parse_mobility_file(self):
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

        if row[0] == "US" and row[2] == "Illinois":
            return

        if row[self.column_headers["census_fips_code"]] not in self.nested_dict:
            self.nested_dict[self.column_headers["census_fips_code"]] = []

        self.nested_dict[self.column_headers["census_fips_code"]].append(
            {
                "place_id": row[self.column_headers["place_id"]],
                "daysElapsed": getDiffDaysSinceDataEpoch(
                    datetime.strptime(row[self.column_headers["date"]], "%Y-%m-%d")
                ),
                "retail_and_recreation": row[
                    self.column_headers[
                        "retail_and_recreation_percent_change_from_baseline"
                    ]
                ],
                "grocery_and_pharmacy": row[
                    self.column_headers[
                        "grocery_and_pharmacy_percent_change_from_baseline"
                    ]
                ],
                "parks": row[self.column_headers["parks_percent_change_from_baseline"]],
                "transit_stations": row[
                    self.column_headers["transit_stations_percent_change_from_baseline"]
                ],
                "workplaces": row[
                    self.column_headers["workplaces_percent_change_from_baseline"]
                ],
                "residential": row[
                    self.column_headers["residential_percent_change_from_baseline"]
                ],
            }
        )

        self.json_file = json.dumps(self.nested_dict)

    def publish_to_s3(self):
        print("Uploading mobility data json to S3...")
        self.s3_client.put_object(
            Body=str(json.dumps(self.nested_dict)),
            Bucket=self.s3_bucket,
            Key="google_mobility_data.json",
        )
        print("Done!")
