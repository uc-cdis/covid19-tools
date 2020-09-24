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
        self.project_code = "ncbi-covid-19"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

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
            try:
                for treatment in data["result"]["pageContext"]["treatments"]:
                    node = treatment["node"]
                    self.clinical_trials.append(self.parse_node(node))

            except ValueError as e:
                print(f"ERROR: value error. Detail {e}")

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
        clinical_trial = {
            "projects": {"code": self.project_code},
            "type": "clinical_trials",
        }
        for key, value in node.items():
            if key not in MAP_FIELDS:
                continue
            gen3_field = MAP_FIELDS.get(key)[0]
            gen3_field_type = MAP_FIELDS.get(key)[1]
            if type(value) != gen3_field_type:
                print(
                    f"ERROR: The type of {key} does not match with the one in Gen3. Skip it"
                )
                continue
            clinical_trial[gen3_field] = value

        return clinical_trial

    def submit_metadata(self):
        """
        Converts the data in `self.time_series_data` to Sheepdog records.
        `self.location_data already contains Sheepdog records. Batch submits
        all records in `self.location_data` and `self.time_series_data`
        """

        print("Submitting clinical_trial data")
        for clinical_trial in self.clinical_trials:
            self.metadata_helper.add_record_to_submit(clinical_trial)
        self.metadata_helper.batch_submit_records()
