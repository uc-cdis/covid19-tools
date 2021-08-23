import datetime
import os
from contextlib import closing

from etl import base
from etl.idph import IDPH
from utils.format_helper import (
    derived_submitter_id,
    format_submitter_id,
    idph_get_date,
    remove_time_from_date_time,
)
from utils.metadata_helper import MetadataHelper

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


class IDPH_FACILITY(IDPH):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "IDPH-Facility"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.country = "US"
        self.state = "IL"

        self.summary_locations = {}
        self.summary_clinicals = {}

    def files_to_submissions(self):
        """
        Reads JSON file and convert the data to Sheepdog records
        """

        latest_submitted_date = self.metadata_helper.get_latest_submitted_date_idph()
        today = datetime.date.today()
        if latest_submitted_date == today:
            print("Nothing to submit: today and latest submitted date are the same.")
            return
        today_str = today.strftime("%Y%m%d")
        begin_date = (latest_submitted_date + datetime.timedelta(days=1)).strftime(
            "%m/%d/%Y"
        )
        end_date = today.strftime("%m/%d/%Y")
        print(f"Getting data for date: {today_str}")
        url = f"https://idph.illinois.gov/DPHPublicInformation/api/COVIDExport/GetLTCFacilityHistorical?beginDate={begin_date}&endDate={end_date}"
        self.parse_file(latest_submitted_date, url)

    def parse_file(self, url):
        """
        Converts a JSON files to data we can submit via Sheepdog. Stores the
        records to submit in `self.summary_locations` and `self.summary_clinicals`.

        Args:
            url (str): URL at which the JSON file is available
        """
        print("Getting data from {}".format(url))
        with closing(self.get(url, stream=True)) as r:
            data = r.json()

            for facility in data:
                date = remove_time_from_date_time(facility["ReportDate"])
                (summary_location, summary_clinical) = self.parse_facility(
                    date, facility
                )
                summary_location_submitter_id = summary_location["submitter_id"]
                summary_clinical_submitter_id = summary_clinical["submitter_id"]

                self.summary_locations[summary_location_submitter_id] = summary_location

                if summary_clinical_submitter_id in self.summary_clinicals:
                    existed = self.summary_clinicals[summary_clinical_submitter_id]
                    summary_clinical["confirmed"] = max(
                        summary_clinical["confirmed"], existed["confirmed"]
                    )
                    summary_clinical["deaths"] = max(
                        summary_clinical["deaths"], existed["deaths"]
                    )

                self.summary_clinicals[summary_clinical_submitter_id] = summary_clinical

    def parse_facility(self, date, facility):
        """
        From county-level data, generate the data we can submit via Sheepdog
        """
        county = facility["County"]
        facility_name = facility["FacilityName"]
        confirmed_cases = facility["confirmed_cases"]
        deaths = facility["deaths"]
        status = facility.get("status", None)
        etlJobDate = (datetime.date.today().strftime("%Y-%m-%d"),)
        summary_location_submitter_id = format_submitter_id(
            "summary_location",
            {
                "country": self.country,
                "state": self.state,
                "facility_name": facility_name,
                "reporting_org_status": status,
            },
        )

        summary_location = {
            "country_region": self.country,
            "submitter_id": summary_location_submitter_id,
            "projects": [{"code": self.project_code}],
            "province_state": self.state,
            "county": county,
            "reporting_org": facility_name,
            "reporting_org_status": status,
        }

        summary_clinical_submitter_id = derived_submitter_id(
            summary_location_submitter_id,
            "summary_location",
            "summary_clinical",
            {"date": date},
        )

        summary_clinical = {
            "confirmed": confirmed_cases,
            "deaths": deaths,
            "submitter_id": summary_clinical_submitter_id,
            "lastUpdateEt": date,
            "date": etlJobDate,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        return summary_location, summary_clinical

    def submit_metadata(self):
        print("Submitting data...")
        print("Submitting summary_location data")
        for sl in self.summary_locations.values():
            sl_record = {"type": "summary_location"}
            sl_record.update(sl)
            self.metadata_helper.add_record_to_submit(sl_record)
        self.metadata_helper.batch_submit_records()

        print("Submitting summary_clinical data")
        for sc in self.summary_clinicals.values():
            sc_record = {"type": "summary_clinical"}
            sc_record.update(sc)
            self.metadata_helper.add_record_to_submit(sc_record)
        self.metadata_helper.batch_submit_records()
