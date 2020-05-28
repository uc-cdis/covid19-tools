import datetime
import re
from contextlib import closing

import requests

from etl import base
from helper.metadata_helper import MetadataHelper


def format_summary_location_submitter_id(country, state, county):
    submitter_id = "summary_location_{}".format(country)
    if state:
        submitter_id += "_{}".format(state)
    if county:
        submitter_id += "_{}".format(county)

    submitter_id = submitter_id.lower().replace(", ", "_")
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)
    return submitter_id.strip("-")


def format_summary_report_submitter_id(location_submitter_id, date):
    return "{}_{}".format(
        location_submitter_id.replace("summary_location_", "summary_report_"), date
    )


class IDPH(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "IDPH"
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

    def il_counties(self):
        with open("etl/data/IL_counties_central_coords_lat_long.tsv") as f:
            counties = f.readlines()
            counties = counties[1:]
            counties = map(lambda l: l.strip().split("\t"), counties)

        for county, lat, lon in counties:
            self.county_dict[county] = {"lat": lat, "lon": lon}

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
        state = "IL"

        # they changed the URL on April 1, 2020
        if today > datetime.date(2020, 3, 31):
            url = "http://www.dph.illinois.gov/sitefiles/COVIDTestResults.json"
        else:
            url = f"https://www.dph.illinois.gov/sites/default/files/COVID19/COVID19CountyResults{today_str}.json"
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

            if latest_submitted_date and date == latest_submitted_date.strftime(
                "%Y-%m-%d"
            ):
                print(
                    "Nothing to submit: today and latest submitted date are the same."
                )
                return

            for county in data["characteristics_by_county"]["values"]:
                summary_location, summary_report = self.parse_county(
                    date, state, county
                )

                self.summary_locations.append(summary_location)
                self.summary_reports.append(summary_report)

            for illinois_data in data["state_testing_results"]["values"]:
                illinois_historic_data = self.parse_historical_data(illinois_data)
                self.summary_reports.append(illinois_historic_data)

    def parse_historical_data(self, illinois_data):
        country = "US"
        state = "IL"
        county = "Illinois"

        date = datetime.datetime.strptime(
            illinois_data["testDate"], "%m/%d/%Y"
        ).strftime("%Y-%m-%d")

        summary_location_submitter_id = format_summary_location_submitter_id(
            country, state, county
        )

        summary_report_submitter_id = format_summary_report_submitter_id(
            summary_location_submitter_id, date
        )
        summary_report = {
            "confirmed": illinois_data["confirmed_cases"],
            "submitter_id": summary_report_submitter_id,
            "testing": illinois_data["total_tested"],
            "date": date,
            "deaths": illinois_data["deaths"],
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        return summary_report

    def parse_county(self, date, state, county_json):
        """
        From county-level data, generate the data we can submit via Sheepdog
        """
        country = "US"
        county = county_json["County"]

        summary_location_submitter_id = format_summary_location_submitter_id(
            country, state, county
        )

        summary_location = {
            "country_region": country,
            "submitter_id": summary_location_submitter_id,
            "projects": [{"code": self.project_code}],
            "province_state": state,
        }

        # the IDPH data use Illinois in "County" field for aggregated data
        # in Gen3 it would equal to location with "province_state" equal to "IL" and no "County" field
        if county != "Illinois":
            summary_location["county"] = county

        if county in self.county_dict:
            summary_location["latitude"] = self.county_dict[county]["lat"]
            summary_location["longitude"] = self.county_dict[county]["lon"]
        else:
            if county_json["lat"] != 0:
                summary_location["latitude"] = str(county_json["lat"])
            if county_json["lon"] != 0:
                summary_location["longitude"] = str(county_json["lon"])

        summary_report_submitter_id = format_summary_report_submitter_id(
            summary_location_submitter_id, date
        )
        summary_report = {
            "confirmed": county_json["confirmed_cases"],
            "submitter_id": summary_report_submitter_id,
            "testing": county_json["total_tested"],
            "date": date,
            "deaths": county_json["deaths"],
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        if "negative" in county_json:
            summary_report["negative"] = county_json["negative"]

        return summary_location, summary_report

    def get_date(self, county_json):
        """
        Converts JSON with "year", "month" and "day" to formatted date string.
        """
        date_json = county_json["LastUpdateDate"]
        date = datetime.date(**date_json)
        return date.strftime("%Y-%m-%d")

    def submit_metadata(self):
        """
        Submits the data in `self.summary_locations` and `self.summary_reports` to Sheepdog.
        """

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
