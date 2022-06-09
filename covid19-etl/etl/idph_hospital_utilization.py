import datetime
import os
import re
from contextlib import closing

from etl import base
from utils.format_helper import (
    derived_submitter_id,
    format_submitter_id,
    remove_time_from_date_time,
    idph_last_reported_date,
    get_date_from_str,
)
from utils.metadata_helper import MetadataHelper

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


def strip_prefix(region):
    # removes any digits, spaces and dashes from begining of string
    # used to cleanup the hospital region
    return re.sub(r"^[\d\s-]+", "", region)


class IDPH_HOSPITAL_UTILIZATION(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "IDPH-Hospital_Utilization"
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
        latest_submitted_date = self.metadata_helper.get_latest_submitted_date()
        print(f"Latest submitted date from guppy is {str(latest_submitted_date)}")
        if latest_submitted_date == today:
            print("Nothing to submit: today and latest submitted date are the same.")
            return
        today_str = today.strftime("%Y%m%d")

        print(f"Getting data for date: {today_str}")
        url = "https://idph.illinois.gov/DPHPublicInformation/api/COVIDExport/GetHospitalUtilizationResults"
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
            last_reported_date = idph_last_reported_date(data)

            if (
                latest_submitted_date
                and last_reported_date == latest_submitted_date.strftime("%Y-%m-%d")
            ):
                print(
                    "Nothing to submit: latest submitted date and the last reported date from data are the same."
                )
                return

            summary_location_submitter_id = format_submitter_id(
                "summary_location",
                {
                    "project": self.project_code,
                },
            )
            summary_location = {
                "country_region": self.country,
                "submitter_id": summary_location_submitter_id,
                "projects": [{"code": self.project_code}],
                "province_state": self.state,
            }
            self.summary_locations.append(summary_location)
            first_util_date = None
            for utilization in data:
                # There is a known bug in IDPH API where the data records are repeated
                # Since the dates are always sorted, we break the loop
                # as soon as a repetition is noticed
                if not first_util_date:
                    first_util_date = data[0]["ReportDate"]
                elif first_util_date == utilization["ReportDate"]:
                    break

                if (
                    latest_submitted_date is None
                    or get_date_from_str(utilization["ReportDate"])
                    > latest_submitted_date
                ):
                    summary_clinical = self.parse_historical(
                        summary_location_submitter_id, utilization
                    )
                    self.summary_clinicals.append(summary_clinical)

    def parse_historical(self, summary_location_submitter_id, utilization):
        utilization_mapping = {
            "ReportDate": "date",
            "TotalBeds": "state_total_beds",
            "TotalOpenBeds": "total_open_beds",
            "TotalInUseBedsNonCOVID": "total_in_use_beds_non_covid",
            "TotalInUseBedsCOVID": "total_in_use_beds_covid",
            "ICUBeds": "icu_beds",
            "ICUOpenBeds": "icu_open_beds",
            "ICUInUseBedsNonCOVID": "icu_in_use_beds_non_covid",
            "ICUInUseBedsCOVID": "icu_in_use_beds_covid",
            "VentilatorCapacity": "ventilator_capacity",
            "VentilatorAvailable": "ventilator_available",
            "VentilatorInUseNonCOVID": "ventilator_in_use_non_covid",
            "VentilatorInUseCOVID": "ventilator_in_use_covid",
        }
        utilization["ReportDate"] = remove_time_from_date_time(
            utilization["ReportDate"]
        )

        summary_clinical_submitter_id = derived_submitter_id(
            summary_location_submitter_id,
            "summary_location",
            "summary_clinical",
            {"date": utilization["ReportDate"]},
        )

        summary_clinical = {
            "submitter_id": summary_clinical_submitter_id,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
            "lastUpdateEt": self.etlJobDate,
        }

        for k, v in utilization.items():
            summary_clinical[utilization_mapping[k]] = v

        return summary_clinical

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
