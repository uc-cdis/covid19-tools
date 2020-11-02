import csv
import re
from contextlib import closing

import requests

from etl import base
from utils.metadata_helper import MetadataHelper


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
    "clinicalTrials": ("nct_number", list),
    "fdaApproved": ("fda_regulated_drug_product", str),
    "completedClinicalTrials": ("completed_clinical_trials", list),
    "inprogressClinicalTrials": ("inprogress_clinical_trials", list),
    "countries": ("location", list),
}


class VAC_TRACKER(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.clinical_trials = []
        self.program_name = "open"
        self.project_code = "VacTracker"
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
        url = "https://biorender.com/page-data/covid-vaccine-tracker/page-data.json"
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
            try:
                for treatment in data["result"]["pageContext"]["treatments"]:
                    node = treatment["node"]
                    clinical_trial = self.parse_node(node)
                    self.clinical_trials.append(clinical_trial)
            except ValueError as e:
                print(f"ERROR: value error. Detail {e}")

    def parse_node(self, node):
        """
        Converts an element of an JSON file to data we can submit via Sheepdog

        Args:
            node (dict): node data

        Returns:
            dict:
                - clinical trial data, in a format ready to be submitted to Sheepdog
        """
        clinical_trial = {
            "projects": [{"code": self.project_code}],
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
            if key == "fdaApproved":
                if "FDA-approved" in value:
                    value = "Yes"
                elif value == "":
                    value = "Unknown"
                elif value in ["N/A", "N//A", "N/A*"]:
                    value = "NA"
                elif value not in ["Yes", "No", "Unknown", "NA", None]:
                    value = "Unknown"
            if key == "customClinicalPhase":
                if value.lower() == "phase na":
                    value = "Phase N/A"
                elif value.lower() in ["preclinical", "pre-clinical"]:
                    value = "Preclinical Phase"
                elif value not in [
                    "Preclinical Phase",
                    "Phase I",
                    "Phase I/II",
                    "Phase II",
                    "Phase I/II/III",
                    "Phase III",
                    "Phase III/IV",
                    "Phase IV",
                    "Phase I/III/IV",
                    "Phase I/IV",
                    "Phase II/IV",
                    "Phase II/III/IV",
                    "Phase I/II/III/IV",
                    "Phase II/III",
                    "Phase N/A",
                    None,
                ]:
                    value = None
            if key == "technology":
                value = value.replace("*", "")
                if "to repurpose" in value.lower():
                    value = "Repurposed"
                if value not in [
                    "Antibodies",
                    "Antivirals",
                    "Cell-based therapies",
                    "Device",
                    "DNA-based",
                    "Inactivated virus",
                    "Modified APC",
                    "Non-replicating viral vector",
                    "Protein subunit",
                    "RNA-based treatments",
                    "RNA-based vaccine",
                    "Repurposed",
                    "Virus Like Particle",
                    "Other",
                    None,
                ]:
                    value = "Other"
            if key == "developmentStage":
                if value.lower() in ["preclinical", "pre-clinical"]:
                    value = "Preclinical Phase"
                elif value not in ["Preclinical Phase", "Clinical", "Withdrawn", None]:
                    value = "Other"

            if gen3_field_type == list:
                value = [str(v) for v in value]
            clinical_trial[gen3_field] = value
        return clinical_trial

    def submit_metadata(self):
        """
        Converts the data in `self.time_series_data` to Sheepdog records.
        `self.location_data already contains Sheepdog records. Batch submits
        all records in `self.clinical_trials`
        """

        print("Submitting clinical_trial data")
        for clinical_trial in self.clinical_trials:
            self.metadata_helper.add_record_to_submit(clinical_trial)
        self.metadata_helper.batch_submit_records()
