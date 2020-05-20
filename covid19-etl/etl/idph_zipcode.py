import datetime
import re
from contextlib import closing

import requests

from etl import base
from helper.metadata_helper import MetadataHelper


def format_summary_location_submitter_id(
    country, state=None, county=None, zipcode=None
):
    submitter_id = "summary_location_{}".format(country)
    if state:
        submitter_id += "_{}".format(state)
    if county:
        submitter_id += "_{}".format(county)
    if zipcode:
        submitter_id += "_{}".format(zipcode)

    submitter_id = submitter_id.lower().replace(", ", "_")
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)
    return submitter_id.strip("-")


def format_summary_report_submitter_id(location_submitter_id, date):
    return "{}_{}".format(
        location_submitter_id.replace("summary_location_", "summary_report_"), date
    )


def format_summary_demographic_submitter_id(location_submitter_id, date):
    return "{}_{}".format(
        location_submitter_id.replace("summary_location_", "summary_demographic_"), date
    )


def format_summary_demographic_submitter_id(location_submitter_id, date):
    return "{}_{}".format(
        location_submitter_id.replace("summary_location_", "summary_demographic_"), date
    )


class IDPH_ZIPCODE(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token)

        self.program_name = "open"
        self.project_code = "IDPH-zipcode"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.county_dict = {}
        self.il_counties()

        self.summary_locations = []
        self.summary_reports = []
        self.summary_demographics = []

    def il_counties(self):
        with open("etl/IL_counties_central_coords_lat_long.tsv") as f:
            counties = f.readlines()
            counties = counties[1:]
            counties = map(lambda l: l.strip().split("\t"), counties)

        for county, lat, lon in counties:
            self.county_dict[county] = {"lat": lat, "lon": lon}

    def files_to_submissions(self):
        """
        Reads JSON file and convert the data to Sheepdog records
        """

        latest_submitted_date = self.metadata_helper.get_latest_submitted_data_idph()
        today = datetime.date.today()
        if latest_submitted_date == today:
            print("Nothing to submit: today and latest submitted date are the same.")
            return

        today_str = today.strftime("%Y%m%d")
        print(f"Getting data for date: {today_str}")
        state = "IL"
        url = "http://dph.illinois.gov/sitefiles/COVIDZip.json?nocache=1"
        self.parse_file(latest_submitted_date, state, url)

    def parse_file(self, latest_submitted_date, state, url):
        """
        Converts a JSON files to data we can submit via Sheepdog. Stores the
        records to submit in `self.summary_locations` and `self.summary_reports`.

        `self.summary_locations` is only needed once.

        Args:
            state (str): the state
            url (str): URL at which the JSON file is available
        """
        print("Getting data from {}".format(url))
        with closing(requests.get(url, stream=True)) as r:
            data = r.json()
            date = self.get_date(data)

            if date == latest_submitted_date.strftime("%Y-%m-%d"):
                print(
                    "Nothing to submit: today and latest submitted date are the same."
                )
                return

            for zipcode_values in data["zip_values"]:
                (
                    summary_location,
                    summary_report,
                    summary_demographic,
                ) = self.parse_zipcode(date, state, zipcode_values)

                self.summary_locations.append(summary_location)
                self.summary_reports.append(summary_report)
                self.summary_demographics.append(summary_demographic)

    def parse_zipcode(self, date, state, zipcode_values):
        """
        From county-level data, generate the data we can submit via Sheepdog
        """
        country = "US"
        zipcode = zipcode_values["zip"]

        summary_location_submitter_id = format_summary_location_submitter_id(
            country, state, zipcode=zipcode
        )

        summary_location = {
            "country_region": country,
            "submitter_id": summary_location_submitter_id,
            "projects": [{"code": self.project_code}],
            "province_state": state,
            "zipcode": zipcode,
        }

        summary_report_submitter_id = format_summary_report_submitter_id(
            summary_location_submitter_id, date
        )
        summary_report = {
            "confirmed": zipcode_values["confirmed_cases"],
            "submitter_id": summary_report_submitter_id,
            "date": date,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        summary_demographic_submitter_id = format_summary_demographic_submitter_id(
            summary_location_submitter_id, date
        )
        summary_demographic = {
            "submitter_id": summary_demographic_submitter_id,
            "date": date,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        gender_mapping = {
            "Male": "gender_male",
            "Female": "gender_female",
            "Unknown/Left Blank": "gender_unknown_left_blank",
        }

        race_mapping = {
            "White": "race_white",
            "Hispanic": "race_hispanic",
            "Black": "race_black",
            "Left Blank": "race_left_blank",
            "Other": "race_other",
            "Asian": "race_asian",
            "NH/PI*": "race_nh_pi",
            "AI/AN**": "race_ai_an",
        }

        age_mapping = {
            "Unknown": "",
            "<20": "age_group_less_20",
            "20-29": "age_group_20_29",
            "30-39": "age_group_30_39",
            "40-49": "age_group_40_49",
            "50-59": "age_group_50_59",
            "60-69": "age_group_60_69",
            "70-79": "age_group_70_79",
            "80+": "age_group_greater_80",
        }

        fields_mapping = {
            "age": ("age_group", age_mapping),
            "gender": ("description", gender_mapping),
            "race": ("description", race_mapping),
        }

        demographic = zipcode_values["demographics"]

        for k, v in fields_mapping.items():
            field, mapping = v
            demographic_group = demographic[k]
            for item in demographic_group:
                dst_field = mapping[item[field]]
                if dst_field:
                    summary_demographic[mapping[item[field]]] = item["count"]

        return summary_location, summary_report, summary_demographic

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

        print("Submitting summary_report data")
        for rep in self.summary_reports:
            rep_record = {"type": "summary_report"}
            rep_record.update(rep)
            self.metadata_helper.add_record_to_submit(rep_record)
        self.metadata_helper.batch_submit_records()
