import csv
import re
from contextlib import closing

import requests

from etl import base
from helper.metadata_helper import MetadataHelper


def format_location_submitter_id(country, province, county=None):
    """summary_location_<country>_<province>_<county>"""
    submitter_id = "summary_location_{}".format(country)
    if province:
        submitter_id += "_{}".format(province)
    if county:
        submitter_id += "_{}".format(county)

    submitter_id = submitter_id.lower().replace(", ", "_")
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)
    return submitter_id.strip("-")


def format_summary_clinical_submitter_id(location_submitter_id, date):
    return "{}_{}".format(
        location_submitter_id.replace("summary_location_", "summary_clinical_"), date
    )


class STOPLIGHT(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.summary_clinicals = []
        self.summary_locations = []
        self.program_name = "open"
        self.project_code = "stoplight"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

    def files_to_submissions(self):
        """
        Reads json files and converts the data to Sheepdog records
        """
        url = "https://covidstoplight.org/api/v0/location/US"
        self.parse_file(url)

    def parse_file(self, url):
        """
        Converts a json file to data we can submit via Sheepdog. Stores the
        records to submit in `self.location_data` and `self.time_series_data`.
        Ignores any records that are already in Sheepdog (relies on unique
        `submitter_id` to check)

        Args:
            url (str): URL at which the file is available
        """
        print("Getting data from {}".format(url))
        with closing(requests.get(url, stream=True)) as r:
            data = r.json()
            timestamp_created = data["data"]["generated"]
            country = data["country_code"]
            summary_location_list = []
            try:
                for zipcode, feelings in data["data"]["submissions"].items():
                    node = {
                        "zipcode": zipcode,
                        "feelings": feelings,
                        "timestamp_created": timestamp_created,
                        "country": country,
                    }
                    summary_location, summary_clinical = self.parse_node(node)
                    summary_location_submitter_id = summary_location["submitter_id"]
                    if summary_location_submitter_id not in summary_location_list:
                        self.summary_locations.append(summary_location)
                        summary_location_list.append(summary_location_submitter_id)
                    self.summary_clinicals.append(summary_clinical)
            except ValueError as e:
                print(f"ERROR: value error. Detail {e}")

    def parse_node(self, node):
        """
        Converts an element of an JSON file to data we can submit via Sheepdog

        Args:
            node (dict): node data

        Returns:
            (dict, dict) tuple:
                - location data, in a format ready to be submitted to Sheepdog
                - { "date1": <value>, "date2": <value> } from the row data
        """
        zipcode = node["zipcode"]
        feelings = node["feelings"]
        timestamp_created = node["timestamp_created"]
        country = node["country"]
        summary_location_submitter_id = format_location_submitter_id(country, zipcode)
        summary_location = {
            "country_region": country,
            "submitter_id": summary_location_submitter_id,
            "projects": [{"code": self.project_code}],
            "zipcode": zipcode,
        }

        summary_clinical_submitter_id = format_summary_clinical_submitter_id(
            summary_location_submitter_id, timestamp_created
        )

        summary_clinical = {
            "timestamp_created": timestamp_created,
            "submitter_id": summary_clinical_submitter_id,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        map_fields = {
            1: "feeling_healthy_count",
            2: "feeling_not_so_good_count",
            3: "feeling_sick_count",
        }

        for element in feelings:
            summary_clinical[map_fields[element["feeling"]]] = element["count"]

        return summary_location, summary_clinical

    def submit_metadata(self):
        """
        Converts the data in `self.time_series_data` to Sheepdog records.
        `self.location_data already contains Sheepdog records. Batch submits
        all records in `self.location_data` and `self.time_series_data`
        """

        print("Submitting summary_location data")
        for loc in self.summary_locations:
            loc_record = {"type": "summary_location"}
            loc_record.update(loc)
            self.metadata_helper.add_record_to_submit(loc_record)
        self.metadata_helper.batch_submit_records()

        print("Submitting summary_clinical data")
        for rep in self.summary_clinicals:
            rep_record = {"type": "summary_clinical"}
            rep_record.update(rep)
            self.metadata_helper.add_record_to_submit(rep_record)
        self.metadata_helper.batch_submit_records()
