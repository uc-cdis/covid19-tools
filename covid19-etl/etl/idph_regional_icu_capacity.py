import datetime
import os
import re
from contextlib import closing

from etl import base
from utils.format_helper import (
    derived_submitter_id,
    format_submitter_id,
)
from utils.metadata_helper import MetadataHelper

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


def strip_prefix(region):
    # removes any digits, spaces and dashes from begining of string
    # used to cleanup the hospital region
    return re.sub(r"^[\d\s-]+", "", region)


class IDPH_REGIONAL_ICU_CAPACITY(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "IDPH-Regional_ICU_Capacity"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.country = "US"
        self.state = "IL"
        self.etlJobDate = datetime.date.today().strftime("%Y-%m-%d")
        self.summary_locations = []
        self.summary_clinicals = []

    def files_to_submissions(self):
        """
        Reads JSON file and convert the data to Sheepdog records
        """

        today = datetime.date.today()
        latest_submitted_date = self.metadata_helper.get_latest_submitted_date_idph()
        if latest_submitted_date == today:
            print("Nothing to submit: today and latest submitted date are the same.")
            return
        today_str = today.strftime("%Y%m%d")

        print(f"Getting data for date: {today_str}")
        url = "https://idph.illinois.gov/DPHPublicInformation/api/COVIDExport/GetHospitalizationResultsRegion"
        self.parse_file(latest_submitted_date, url)

    def parse_file(self, latest_submitted_date, url):
        """
        Converts a JSON files to data we can submit via Sheepdog. Stores the
        records to submit in `self.summary_locations` and `self.summary_clinicals`.

        Args:
            latest_submitted_date (date): the date of latest available "summary_clinical" for project
            url (str): URL at which the JSON file is available
        """
        print("Getting data from {}".format(url))
        with closing(self.get(url, stream=True)) as r:
            data = r.json()
            date = self.etlJobDate

            summary_locations_in_guppy = self.get_existing_summary_locations()

            for region in data:
                (summary_location, summary_clinical) = self.parse_region(date, region)
                self.summary_clinicals.append(summary_clinical)
                if summary_location["submitter_id"] not in summary_locations_in_guppy:
                    self.summary_locations.append(summary_location)

    def parse_region(self, date, hospital_region):
        """
        From county-level data, generate the data we can submit via Sheepdog
        """
        region = hospital_region["region"]
        region_description = hospital_region["region_description"]

        summary_location_submitter_id = format_submitter_id(
            "summary_location",
            {
                "region": region,
            },
        )

        summary_location = {
            "country_region": self.country,
            "submitter_id": summary_location_submitter_id,
            "projects": [{"code": self.project_code}],
            "province_state": self.state,
            "state_hospital_region": region,
            "state_region_description": strip_prefix(region_description),
        }

        summary_clinical_submitter_id = derived_submitter_id(
            summary_location_submitter_id,
            "summary_location",
            "summary_clinical",
            {"date": date},
        )

        summary_clinical = {
            "submitter_id": summary_clinical_submitter_id,
            "date": date,
            "lastUpdateEt": self.etlJobDate,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
            "region_icu_avail": hospital_region["ICUAvail"],
            "region_icu_capacity": hospital_region["ICUCapacity"],
            "region_vents_available": hospital_region["VentsAvailable"],
            "region_vents_capacity": hospital_region["VentsCapacity"],
        }

        return summary_location, summary_clinical

    def get_existing_summary_locations(self):
        print("Getting current summary_location records from Guppy...")
        query_string = """query ($filter: JSON) {
            location (
                filter: $filter,
                first: 10000,
                accessibility: accessible
            ) {
                submitter_id
            }
        }"""

        project_id = f"{self.program_name}-{self.project_code}"
        variables = {"filter": {"=": {"project_id": project_id}}}
        query_res = self.metadata_helper.query_guppy(query_string, variables)

        if "data" not in query_res or "location" not in query_res["data"]:
            raise Exception(
                f"Did not receive any data from Guppy. Query result for the query - {query_string} with variables - {variables} is \n\t {query_res}"
            )

        location_list = query_res["data"]["location"]
        return [location["submitter_id"] for location in location_list]

    def submit_metadata(self):
        print("Submitting data...")
        print("Submitting summary_location data")
        for sl in self.summary_locations:
            sl_record = {"type": "summary_location"}
            sl_record.update(sl)
            self.metadata_helper.add_record_to_submit(sl_record)
        self.metadata_helper.batch_submit_records()

        print("Submitting summary_clinical data")
        for sc in self.summary_clinicals:
            sc_record = {"type": "summary_clinical"}
            sc_record.update(sc)
            self.metadata_helper.add_record_to_submit(sc_record)
        self.metadata_helper.batch_submit_records()
