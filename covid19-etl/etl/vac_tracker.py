import csv
import re
from contextlib import closing

import requests

from etl import base
from helper.metadata_helper import MetadataHelper


# map from gen3 fields
MAP_FIELDS = {
    "id": ("submitter_id", str),
    "name": ("title", str),
    "type": ("focus", str),
    "technology": ("technology", str),
    "technologyDetails": ("technology_details", str),
    "organizations": ("sponsor", list),
    "developmentStage": ("development_stage", str),
    "description": ("description", str),
    "customClinicalPhase": ("phase", str),
    "clinicalTrials": ("clinical_trials", list),
    "fdaApproved": ("status", str),
    "completedClinicalTrials": ("completed_clinical_trials", list),
    "inprogressClinicalTrials": ("inprogress_clinical_trials", list),
    "countries": ("countries", list),
}


def format_location_submitter_id(country):
    """summary_location_<country>"""
    submitter_id = "summary_location_{}".format(country)
    submitter_id = submitter_id.lower().replace(", ", "_")
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)
    return submitter_id.strip("-")


def format_summary_clinical_submitter_id(location_submitter_id, date):
    return "{}_{}".format(
        location_submitter_id.replace("summary_location_", "summary_clinical_"), date
    )


class VacTracker(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.summary_locations = []
        self.summary_clinicals = []

        self.program_name = "open"
        self.project_code = "vac_tracker"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.expected_csv_headers = [
            "life_expectancy",
        ]

        self.header_to_column = {
            k: self.expected_csv_headers.index(k) for k in self.expected_csv_headers
        }

    def files_to_submissions(self):
        """
        Reads CSV files and converts the data to Sheepdog records
        """
        url = "https://biorender.com/page-data/covid-vaccine-tracker/page-data.json"
        self.parse_file(url)

    def parse_file(self, url):
        """
        Converts a CSV file to data we can submit via Sheepdog. Stores the
        records to submit in `self.location_data` and `self.time_series_data`.
        Ignores any records that are already in Sheepdog (relies on unique
        `submitter_id` to check)

        Args:
            url (str): URL at which the CSV file is available
        """
        print("Getting data from {}".format(url))
        with closing(requests.get(url, stream=True)) as r:
            data = r.json()
            import pdb

            pdb.set_trace()
            for treatment in data["result"]["pageContext"]["treatments"]:
                node = treatment["node"]
                self.parse_node(node)
                print(node)

                # self.summary_locations.append(summary_location)
                # self.summary_clinicals.append(summary_clinical)

    def parse_node(self, node):
        """
        Converts a row of a CSV file to data we can submit via Sheepdog

        Args:
            row (list(str)): row of data

        Returns:
            (dict, dict) tuple:
                - location data, in a format ready to be submitted to Sheepdog
                - { "date1": <value>, "date2": <value> } from the row data
        """

        date = row[self.header_to_column["date"]]

        country = row[self.header_to_column["location"]]

        summary_location_submitter_id = format_location_submitter_id(country)

        summary_location = {
            "country_region": country,
            "submitter_id": summary_location_submitter_id,
            "projects": [{"code": self.project_code}],
        }

        summary_clinical_submitter_id = format_summary_clinical_submitter_id(
            summary_location_submitter_id, date
        )
        summary_clinical = {
            "date": date,
            "submitter_id": summary_clinical_submitter_id,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        for k, (v, dtype) in map_csv_fields.items():
            value = row[self.header_to_column[v]]
            if value and value.lower() != "nan":
                try:
                    if dtype == int:
                        summary_clinical[k] = int(float(value.replace(",", "")))
                    elif dtype == float:
                        summary_clinical[k] = float(value.replace(",", ""))
                except Exception:
                    pass

        return summary_location, summary_clinical

    def submit_metadata(self):
        """
        Converts the data in `self.time_series_data` to Sheepdog records.
        `self.location_data already contains Sheepdog records. Batch submits
        all records in `self.location_data` and `self.time_series_data`
        """

        # Commented
        # Only required for one time submission of summary_location
        print("Submitting summary_location data")
        for loc in self.summary_locations:
            loc_record = {"type": "summary_location"}
            loc_record.update(loc)
            self.metadata_helper.add_record_to_submit(loc_record)
        self.metadata_helper.batch_submit_records()
        print("Submitting summary_clinical data")
        for sc in self.summary_clinicals:
            sc_record = {"type": "summary_clinical"}
            sc_record.update(sc)
            self.metadata_helper.add_record_to_submit(sc_record)
        self.metadata_helper.batch_submit_records()
