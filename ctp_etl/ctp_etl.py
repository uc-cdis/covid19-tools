from collections import defaultdict
from contextlib import closing
import csv
from datetime import datetime
import enum
import json
import os
from math import ceil
import re
import requests


BASE_URL = "https://covid19.datacommons.io"
PROGRAM_NAME = "open"
PROJECT_CODE = "CTP"

# Note: if we end up having too much data, Sheepdog submissions may
# time out. We'll have to use a smaller batch size and hope that's enough
SUBMIT_BATCH_SIZE = 100


def get_token():
    with open("credentials.json", "r") as f:
        creds = json.load(f)
    token_url = BASE_URL + "/user/credentials/api/access_token"
    res = requests.post(token_url, json=creds).json()
    if not "access_token" in res:
        print(res)
    return res["access_token"]


def main():
    # token = get_token()  # TODO remove
    token = os.environ.get("ACCESS_TOKEN")
    if not token:
        raise Exception(
            "Need ACCESS_TOKEN environment variable (token for user with read and write access to {}-{})".format(
                PROGRAM_NAME, PROJECT_CODE
            )
        )

    etl = CovidTrackingProjectETL(token)
    etl.files_to_submissions()
    etl.submit_metadata()


def format_location_submitter_id(country, province, county=None):
    """summary_location_<country>_<province>_<county>"""
    submitter_id = "summary_location_{}".format(country)
    if province:
        submitter_id += "_{}".format(province)
    if county:
        submitter_id += "_{}".format(county)

    submitter_id = submitter_id.lower().replace(", ", "_")
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)
    return submitter_id.strip("-")


def format_summary_report_submitter_id(location_submitter_id, date):
    return "{}_{}".format(location_submitter_id.replace("summary_location_", "summary_report_"), date)


def get_unified_date_format(date):
    month, day, year = date.split("/")

    # format all the dates the same way
    if len(year) == 2:
        year = "20{}".format(year)
    if len(month) == 1:
        month = "0{}".format(month)
    if len(day) == 1:
        day = "0{}".format(day)

    return "-".join((year, month, day))


def format_report_submitter_id(location_submitter_id, date):
    """summary_report_<country>_<province>_<county>_<date>"""
    sub_id = location_submitter_id.replace(
        "summary_location", "summary_report")
    return "{}_{}".format(sub_id, date)


def format_time_series_date(date):
    return datetime.strptime(date, "%Y-%m-%d").isoformat("T")


class CovidTrackingProjectETL:
    def __init__(self, access_token):
        self.summary_locations = []
        self.summary_reports = []
        self.metadata_helper = MetadataHelper(access_token=access_token)
        self.expected_csv_headers = [
            "date",
            "state",
            "positive",
            "negative",
            "pending",
            "hospitalized",
            "death",
            "total",
            "hash",
            "dateChecked",
            "totalTestResults",
            "fips",
            "deathIncrease",
            "hospitalizedIncrease",
            "negativeIncrease",
            "positiveIncrease",
            "totalTestResultsIncrease"
        ]
        self.header_to_column = {k: self.expected_csv_headers.index(
            k) for k in self.expected_csv_headers}
        # self.existing_data = self.metadata_helper.get_existing_data()

    def files_to_submissions(self):
        """
        Reads CSV files and converts the data to Sheepdog records
        """
        # self.metadata_helper.delete_tmp()  # TODO remove
        # return
        url = "https://raw.githubusercontent.com/COVID19Tracking/covid-tracking-data/master/data/states_daily_4pm_et.csv"
        self.parse_file(url)

    def parse_file(self, url):
        """
        Converts a CSV file to data we can submit via Sheepdog. Stores the
        records to submit in `self.location_data` and `self.time_series_data`.
        Ignores any records that are already in Sheepdog (relies on unique
        `submitter_id` to check)

        Args:
            data_type (str): type of the data in this file - one
                of ["confirmed", "deaths", "recovered"]
            url (str): URL at which the CSV file is available
        """
        print("Getting data from {}".format(url))
        with closing(requests.get(url, stream=True)) as r:
            f = (line.decode("utf-8") for line in r.iter_lines())
            reader = csv.reader(f, delimiter=",", quotechar='"')

            headers = next(reader)

            if headers[0] == "404: Not Found":
                print("  Unable to get file contents, received {}.".format(headers))
                return

            expected_h = self.expected_csv_headers
            obtained_h = headers[: len(expected_h)]
            assert (
                obtained_h == expected_h
            ), "CSV headers have changed (expected {}, got {}). We may need to update the ETL code".format(
                expected_h, obtained_h
            )

            for row in reader:
                summary_location, summary_report = self.parse_row(headers, row)

                self.summary_locations.append(summary_location)
                self.summary_reports.append(summary_report)

    def parse_row(self, headers, row):
        """
        Converts a row of a CSV file to data we can submit via Sheepdog

        Args:
            headers (list(str)): CSV file headers (first row of the file)
            row (list(str)): row of data

        Returns:
            (dict, dict) tuple:
                - location data, in a format ready to be submitted to Sheepdog
                - { "date1": <value>, "date2": <value> } from the row data
        """

        date = row[self.header_to_column["date"]]
        date = datetime.strptime(date, "%Y%m%d").date()
        date = date.strftime("%Y-%m-%d")

        country = "US"
        state = row[self.header_to_column["state"]]
        summary_location_submitter_id = format_location_submitter_id(
            country, state)

        summary_location = {
            "country_region": country,
            "submitter_id": summary_location_submitter_id,
            "projects": [{"code": PROJECT_CODE}],
            "province_state": state,
        }

        summary_report_submitter_id = format_summary_report_submitter_id(
            summary_location_submitter_id, date
        )
        summary_report = {
            "date": date,
            "submitter_id": summary_report_submitter_id,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        map_csv_fields = {
            "hospitalized": "hospitalized",
            "confirmed": "positive",
            "testing": "totalTestResultsIncrease",
            "hospitalizedIncrease": "hospitalizedIncrease",
            "totalTestResults": "totalTestResults",
            "pending": "pending",
            "positiveIncrease": "positiveIncrease",
            "negativeIncrease": "negativeIncrease",
            "deaths": "death",
            "deathIncrease": "deathIncrease",
            "negative": "negative",
        }

        for k, v in map_csv_fields.items():
            if row[self.header_to_column[v]]:
                summary_report[k] = int(row[self.header_to_column[v]])

        return summary_location, summary_report

    def submit_metadata(self):
        """
        Converts the data in `self.time_series_data` to Sheepdog records.
        `self.location_data already contains Sheepdog records. Batch submits
        all records in `self.location_data` and `self.time_series_data`
        """

        # Commented
        # Only required for one time submission of summary_location
        print("Submitting summary_location data")
        # for loc in self.summary_locations:
        #     loc_record = {"type": "summary_location"}
        #     loc_record.update(loc)
        #     self.metadata_helper.add_record_to_submit(loc_record)
        # self.metadata_helper.batch_submit_records()

        # print("Submitting summary_report data")
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

    def get_existing_data(self):
        """
        Queries Peregrine for the existing `location` and `time_series` data. Returns a dict in format { "location1": [ "date1", "date2" ] }

        Note: if we end up having too much data, the query may timeout. We
        could simplify this by assuming that any `time_series` date that
        already exists for one location also already exists for all other
        locations (historically not true), and use the following query to
        retrieve the dates we already have data for:
        { location (first: 1, project_id: <...>) { time_seriess (first: 0) { date } } }
        Or use the `first` and `offset` Peregrine parameters
        We could also query Guppy instead (assuming the Guppy ETL ran since
        last time this ETL ran), or get the existing data directly from the DB.
        """
        print("Getting existing data from Peregrine...")
        print("  summary_location data...")
        query_string = (
            '{ summary_location (first: 0, project_id: "'
            + self.project_id
            + '") { submitter_id } }'
        )
        response = requests.post(
            "{}/api/v0/submission/graphql".format(self.base_url),
            json={"query": query_string, "variables": None},
            headers=self.headers,
        )
        assert (
            response.status_code == 200
        ), "Unable to query Peregrine for existing 'summary_location' data: {}\n{}".format(
            response.status_code, response.text
        )
        try:
            query_res = json.loads(response.text)
        except:
            print("Peregrine did not return JSON")
            raise
        json_res = {
            location["submitter_id"]: []
            for location in query_res["data"]["summary_location"]
        }

        print("  summary_report data...")
        query_string = (
            '{ summary_report (first: 0, project_id: "'
            + self.project_id
            + '") { submitter_id } }'
        )
        response = requests.post(
            "{}/api/v0/submission/graphql".format(self.base_url),
            json={"query": query_string, "variables": None},
            headers=self.headers,
        )
        assert (
            response.status_code == 200
        ), "Unable to query Peregrine for existing 'summary_report' data: {}\n{}".format(
            response.status_code, response.text
        )
        try:
            query_res = json.loads(response.text)
        except:
            print("Peregrine did not return JSON")
            raise

        for report in query_res["data"]["summary_report"]:
            report_id = report["submitter_id"]
            location_id = report_id.replace(
                "summary_report", "summary_location")
            location_id = "_".join(location_id.split("_")[
                                   :-1])  # remove the date
            json_res[location_id].append(report_id)

        return json_res

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
            # return

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

    # TODO remove
    def delete_tmp(self):

        return

        for i in range(3):
            print(i)
            query_string = (
                '{ summary_location (first: 200, project_id: "'
                + self.project_id
                + '") { submitter_id, id } }'
            )
            # query_string = (
            #     '{ summary_report (first: 200, project_id: "'
            #     + self.project_id
            #     + '") { submitter_id, id } }'
            # )
            # query_string = (
            #     '{ summary_report (first: 200, project_id: "'
            #     + self.project_id
            #     + '"with_path_to: { type: "summary_location", country_region: "US" }) { submitter_id, id } }'
            # )
            response = requests.post(
                "{}/api/v0/submission/graphql".format(self.base_url),
                json={"query": query_string, "variables": None},
                headers=self.headers,
            )
            ids = [
                loc["id"]
                for loc in json.loads(response.text)["data"]["summary_location"]
            ]
            # ids = [loc["id"] for loc in json.loads(response.text)["data"]["summary_report"]]
            url = (
                self.base_url
                + "/api/v0/submission/{}/{}".format(PROGRAM_NAME, PROJECT_CODE)
                + "/entities/"
                + ",".join(ids)
            )
            resp = requests.delete(url, headers=self.headers)
            assert resp.status_code == 200, resp.status_code


if __name__ == "__main__":
    main()
