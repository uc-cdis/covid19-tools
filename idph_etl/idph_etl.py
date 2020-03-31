import csv
import datetime
import enum
import json
import os
import re
from collections import defaultdict
from contextlib import closing
from math import ceil

import requests

BASE_URL = "https://covid19.datacommons.io"
PROGRAM_NAME = "open"
PROJECT_CODE = "IDPH"

# Note: if we end up having too much data, Sheepdog submissions may
# time out. We'll have to use a smaller batch size and hope that's enough
SUBMIT_BATCH_SIZE = 100


# with open("IL_counties_central_coords_lat_long.tsv") as f:
#     counties = f.readlines()
#     counties = counties[1:]
#     counties = map(lambda l: l.strip().split("\t"), counties)

# county_dict = {}
# for county, lat, lon in counties:
#     print(county)
#     county_dict[county] = {'lat': lat, 'lon': lon}

def main():
    token = os.environ.get("ACCESS_TOKEN")
    if not token:
        raise Exception(
            "Need ACCESS_TOKEN environment variable (token for user with read and write access to {}-{})".format(
                PROGRAM_NAME, PROJECT_CODE
            )
        )

    etl = IllinoisDPHETL(token)
    etl.files_to_submissions()
    etl.submit_metadata()


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
    return "{}_{}".format(location_submitter_id.replace("summary_location_", "summary_report_"), date)


class IllinoisDPHETL:
    def __init__(self, access_token):
        self.summary_locations = []
        self.summary_reports = []
        self.metadata_helper = MetadataHelper(access_token=access_token)

    def files_to_submissions(self):
        """
        Reads JSON file and convert the data to Sheepdog records
        """

        latest_submitted_date = self.metadata_helper.get_latest_submitted_data()
        today = datetime.date.today()
        if latest_submitted_date == today:
            print("Nothing to submit: today and latest submitted date are the same.")
            return

        today_str = today.strftime("%Y%m%d")
        print(f"Getting data for date: {today_str}")
        state = "IL"
        url = f"https://www.dph.illinois.gov/sites/default/files/COVID19/COVID19CountyResults{today_str}.json"
        self.parse_file(state, url)

    def parse_file(self, state, url):
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

            for county in data["characteristics_by_county"]["values"]:
                summary_location, summary_report = self.parse_county(
                    date, state, county)

                # drop the Illinois summary data
                if summary_location["county"] == "Illinois":
                    continue

                self.summary_locations.append(summary_location)
                self.summary_reports.append(summary_report)

    def parse_county(self, date, state, county_json):
        """
        From county-level data, generate the data we can submit via Sheepdog
        """
        country = "US"
        county = county_json["County"]

        summary_location_submitter_id = format_summary_location_submitter_id(
            country, state, county)

        summary_location = {
            "country_region": country,
            "county": county,
            "submitter_id": summary_location_submitter_id,
            "projects": [{"code": PROJECT_CODE}],
            "province_state": state,
        }

        # if county in county_dict:
        #     summary_location["latitude"] = county_dict[county]["lat"]
        #     summary_location["longitude"] = county_dict[county]["lon"]
        # else:
        #     if county_json["lat"] != 0:
        #         summary_location["latitude"] = str(county_json["lat"])
        #     if county_json["lon"] != 0:
        #         summary_location["longitude"] = str(county_json["lon"])

        summary_report_submitter_id = format_summary_report_submitter_id(
            summary_location_submitter_id, date
        )
        summary_report = {
            "confirmed": county_json["confirmed_cases"],
            "submitter_id": summary_report_submitter_id,
            "testing": county_json["total_tested"],
            "negative": county_json["negative"],
            "date": date,
            "deaths": county_json["deaths"],
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        return summary_location, summary_report

    def get_date(self, county_json):
        """
        Converts JSON with "year", "month" and "day" to formatted date string.
        """
        date_json = county_json['LastUpdateDate']
        date = datetime.date(**date_json)
        return date.strftime("%Y-%m-%d")

    def submit_metadata(self):
        """
        Submits the data in `self.summary_locations` and `self.summary_reports` to Sheepdog.
        """

        print("Submitting data")

        # Commented
        # Only required for one time submission of summary_location
        # print("Submitting summary_location data")
        # for loc in self.summary_locations:
        #     loc_record = {"type": "summary_location"}
        #     loc_record.update(loc)
        #     self.metadata_helper.add_record_to_submit(loc_record)
        # self.metadata_helper.batch_submit_records()

        print("Submitting summary_report data")
        for rep in self.summary_reports:
            rep_record = {"type": "summary_report"}
            rep_record.update(rep)
            self.metadata_helper.add_record_to_submit(rep_record)
        self.metadata_helper.batch_submit_records()


class MetadataHelper:
    def __init__(self, access_token):
        self.base_url = BASE_URL
        self.headers = {"Authorization": "bearer " + access_token}
        self.project_id = "{}-{}".format(PROGRAM_NAME, PROJECT_CODE)
        self.records_to_submit = []

    def get_latest_submitted_data(self):
        """
        Queries Peregrine for the existing `summary_report` data.

        { summary_report (first: 1, project_id: <...>) { date } }

        Returns the latest submitted date as Python "datetime.date"
        """
        print("Getting latest date from Peregrine...")
        query_string = (
            '{ summary_report (first: 1, order_by_desc: "date", project_id: "'
            + self.project_id
            + '") { submitter_id date } }'
        )
        response = requests.post(
            "{}/api/v0/submission/graphql".format(self.base_url),
            json={"query": query_string, "variables": None},
            headers=self.headers,
        )
        assert (
            response.status_code == 200
        ), "Unable to query Peregrine for existing data: {}\n{}".format(
            response.status_code, response.text
        )
        try:
            query_res = json.loads(response.text)
        except:
            print("Peregrine did not return JSON")
            raise

        report = query_res["data"]["summary_report"][0]
        latest_submitted_date = datetime.datetime.strptime(
            report["date"], "%Y-%m-%d")
        return latest_submitted_date.date()

    def add_record_to_submit(self, record):
        self.records_to_submit.append(record)

    def batch_submit_records(self):
        """
        Submits Sheepdog records in batch
        """
        if not self.records_to_submit:
            print("  Nothing new to submit")
            return

        n_batches = ceil(len(self.records_to_submit) / SUBMIT_BATCH_SIZE)
        for i in range(n_batches):
            records = self.records_to_submit[
                i * SUBMIT_BATCH_SIZE: (i + 1) * SUBMIT_BATCH_SIZE
            ]
            print(
                "  Submitting {} records: {}".format(
                    len(records), [r["submitter_id"] for r in records]
                )
            )
            # self.records_to_submit = []  # TODO remove
            # return  # TODO remove

            response = requests.put(
                "{}/api/v0/submission/{}/{}".format(
                    self.base_url, PROGRAM_NAME, PROJECT_CODE
                ),
                headers=self.headers,
                data=json.dumps(records),
            )
            assert (
                response.status_code == 200
            ), "Unable to submit to Sheepdog: {}\n{}".format(
                response.status_code, response.text
            )

        self.records_to_submit = []


if __name__ == "__main__":
    main()
