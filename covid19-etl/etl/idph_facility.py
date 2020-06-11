from contextlib import closing
import datetime
import os
import re
import requests

from etl import base
from helper.metadata_helper import MetadataHelper


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


def format_summary_location_submitter_id(
    country, state=None, county=None, facility_name=None
):
    submitter_id = "summary_location_facility_{}".format(country)
    if state:
        submitter_id += "_{}".format(state)
    if county:
        submitter_id += "_{}".format(county)
    if facility_name:
        submitter_id += "_{}".format(facility_name)

    submitter_id = submitter_id.lower().replace(", ", "_")
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)
    return submitter_id.strip("-")


def format_summary_clinical_submitter_id(location_submitter_id, date):
    return "{}_{}".format(
        location_submitter_id.replace("summary_location_", "summary_clinical_"), date
    )


class IDPH_FACILITY(base.BaseETL):
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

        self.summary_locations = []
        self.summary_clinicals = []

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

        print(f"Getting data for date: {today_str}")
        url = "https://dph.illinois.gov/sitefiles/COVIDLTC.json"
        self.parse_file(latest_submitted_date, url)

    def parse_file(self, latest_submitted_date, url):
        """
        Converts a JSON files to data we can submit via Sheepdog. Stores the
        records to submit in `self.summary_locations` and `self.summary_clinicals`.
        :param latest_submitted_date: the date of latest available "summary_clinical" for project
        :param url: URL of JSON for parsing
        :return: fast-return if no new data is available
        """
        print("Getting data from {}".format(url))
        with closing(requests.get(url, stream=True)) as r:
            data = r.json()
            date = self.get_date(data)

            if latest_submitted_date and date == latest_submitted_date.strftime(
                "%Y-%m-%d"
            ):
                print(
                    "Nothing to submit: latest submitted date and date from data are the same."
                )
                return

            summary_location_submitter_id = format_summary_location_submitter_id(
                self.country, self.state
            )
            summary_location = {
                "country_region": self.country,
                "submitter_id": summary_location_submitter_id,
                "projects": [{"code": self.project_code}],
                "province_state": self.state,
            }

            summary_clinical_submitter_id = format_summary_clinical_submitter_id(
                summary_location_submitter_id, date
            )
            summary_clinical = {
                "confirmed": data["LTC_Reported_Cases"]["confirmed_cases"],
                "deaths": data["LTC_Reported_Cases"]["deaths"],
                "submitter_id": summary_clinical_submitter_id,
                "lastUpdateEt": date,
                "date": date,
                "summary_locations": [{"submitter_id": summary_location_submitter_id}],
            }
            self.summary_locations.append(summary_location)
            self.summary_clinicals.append(summary_clinical)

            for facility in data["FacilityValues"]:
                (summary_location, summary_clinical,) = self.parse_facility(
                    date, facility
                )

                self.summary_locations.append(summary_location)
                self.summary_clinicals.append(summary_clinical)

    def parse_facility(self, date, facility):
        """
        From county-level data, generate the data we can submit via Sheepdog
        """
        county = facility["County"]
        facility_name = facility["FacilityName"]
        confirmed_cases = facility["confirmed_cases"]
        deaths = facility["deaths"]
        status = facility["status"]

        summary_location_submitter_id = format_summary_location_submitter_id(
            self.country, self.state, county=county, facility_name=facility_name
        )

        summary_location = {
            "country_region": self.country,
            "submitter_id": summary_location_submitter_id,
            "projects": [{"code": self.project_code}],
            "province_state": self.state,
            "county": county,
            "reporting_org": facility_name,
        }

        summary_clinical_submitter_id = format_summary_clinical_submitter_id(
            summary_location_submitter_id, date
        )
        summary_clinical = {
            "confirmed": confirmed_cases,
            "deaths": deaths,
            "submitter_id": summary_clinical_submitter_id,
            "lastUpdateEt": date,
            "date": date,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        return summary_location, summary_clinical

    def get_date(self, county_json):
        """
        Converts JSON with "year", "month" and "day" to formatted date string.
        """
        date_json = county_json["LastUpdateDate"]
        date = datetime.date(**date_json)
        return date.strftime("%Y-%m-%d")

    def submit_metadata(self):
        print("Submitting data")
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
