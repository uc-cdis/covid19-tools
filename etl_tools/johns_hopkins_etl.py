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
PROJECT_CODE = "JHU"

# Note: if we end up having too much data, Sheepdog submissions may
# time out. We'll have to use a smaller batch size and hope that's enough
SUBMIT_BATCH_SIZE = 100


# TODO remove
# def get_token():
#     with open("credentials.json", "r") as f:
#         creds = json.load(f)
#     token_url = BASE_URL + "/user/credentials/api/access_token"
#     token = requests.post(token_url, json=creds).json()["access_token"]
#     return token


def main():
    # token = get_token()
    token = os.environ.get("ACCESS_TOKEN")
    if not token:
        raise Exception(
            "Need ACCESS_TOKEN environment variable (token for user with read and write access to {}-{})".format(
                PROGRAM_NAME, PROJECT_CODE
            )
        )

    etl = JonhsHopkinsETL(token)
    etl.files_to_submissions()
    etl.submit_metadata()


def format_location_submitter_id(country, province):
    submitter_id = "location_{}".format(country)
    if province:
        submitter_id += "_{}".format(province)

    submitter_id = submitter_id.lower().replace(", ", "_")
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)
    return submitter_id.strip("-")


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


def format_time_series_submitter_id(location_submitter_id, date):
    return "{}_timeseries_{}".format(location_submitter_id, date)


def format_time_series_date(date):
    return datetime.strptime(date, "%Y-%m-%d").isoformat("T")


class JonhsHopkinsETL:
    def __init__(self, access_token):
        self.location_data = {}
        self.time_series_data = defaultdict(lambda: defaultdict(dict))
        self.metadata_helper = MetadataHelper(access_token=access_token)
        self.expected_csv_headers = [
            "Province/State",
            "Country/Region",
            "Lat",
            "Long",
            "1/22/20",
        ]
        self.existing_data = self.metadata_helper.get_existing_data()

    def files_to_submissions(self):
        """
        Reads CSV files and converts the data to Sheepdog records
        """
        # self.metadata_helper.delete_tmp()  # TODO remove
        # return
        urls = {
            # global
            "confirmed": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv",
            "deaths": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv",
            "recovered": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv",
            "testing": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_testing_global.csv",
            # by state for US
            # TODO: enable these when the files are available and test
            # "confirmed": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv",
            # "deaths": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv",
            # "testing": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_testing_US.csv",
        }
        for data_type, url in urls.items():
            self.parse_file(data_type, url)

    def parse_file(self, data_type, url):
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

            assert (
                headers[:5] == self.expected_csv_headers
            ), "CSV headers have changed (expected {}, got {}). We may need to update the ETL code".format(
                headers[:5], self.expected_csv_headers
            )

            for row in reader:
                location, date_to_value = self.parse_row(headers, row)
                if not location:
                    # We are using US data by state instead of global
                    continue

                location_submitter_id = location["submitter_id"]
                if (
                    location_submitter_id not in self.location_data
                    # do not re-submit location data that already exist
                    and location_submitter_id not in self.existing_data
                ):
                    self.location_data[location_submitter_id] = location

                for date, value in date_to_value.items():
                    date_submitter_id = format_time_series_submitter_id(
                        location_submitter_id, date
                    )
                    # do not re-submit time_series data that already exist
                    if date_submitter_id not in self.existing_data.get(
                        location_submitter_id, []
                    ):
                        self.time_series_data[location_submitter_id][date][
                            data_type
                        ] = value

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
        province = row[0]
        country = row[1]
        submitter_id = format_location_submitter_id(country, province)

        # TODO: enable this AND DELETE THE GLOBAL US DATA when the
        # state-level data files are available
        # if country == "US" and province == "":
        #     # We are using US data by state instead of global
        #     return None, None

        location = {
            "country_region": country,
            "latitude": row[2],
            "longitude": row[3],
            "submitter_id": submitter_id,
            "projects": [{"code": PROJECT_CODE}],
        }
        if province:
            location["province_state"] = province

        date_to_value = {}
        for i in range(4, len(headers)):
            date = headers[i]
            date = get_unified_date_format(date)

            if row[i] == "":  # ignore empty values
                continue
            try:
                val = int(row[i])
            except:
                print(
                    'Unable to convert {} to int for "{}", "{}" at {}'.format(
                        row[i], province, country, date
                    )
                )
                raise
            date_to_value[date] = val

        return location, date_to_value

    def submit_metadata(self):
        """
        Converts the data in `self.time_series_data` to Sheepdog records.
        `self.location_data already contains Sheepdog records. Batch submits
        all records in `self.location_data` and `self.time_series_data`
        """

        print("Submitting location data")
        for location in self.location_data.values():
            record = {"type": "location"}
            record.update(location)
            self.metadata_helper.add_record_to_submit(record)
        self.metadata_helper.batch_submit_records()

        print("Submitting time_series data")
        for location_submitter_id, time_series in self.time_series_data.items():
            for date, data in time_series.items():
                submitter_id = format_time_series_submitter_id(
                    location_submitter_id, date
                )
                record = {
                    "type": "time_series",
                    "submitter_id": submitter_id,
                    "locations": [{"submitter_id": location_submitter_id}],
                    "date": format_time_series_date(date),
                }
                for data_type, value in data.items():
                    record[data_type] = value
                self.metadata_helper.add_record_to_submit(record)
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
        We could also get the existing data directly from the DB instead of
        querying Peregrine.
        """
        print("Getting existing data from Peregrine...")
        query_string = (
            '{ location (first: 0, project_id: "'
            + self.project_id
            + '") { submitter_id, time_seriess (first: 0) { submitter_id } } }'
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

        json_res = {}
        for location in query_res["data"]["location"]:
            json_res[location["submitter_id"]] = [
                time_series["submitter_id"]
                for time_series in location.get("time_seriess")
            ]
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
                i * SUBMIT_BATCH_SIZE : (i + 1) * SUBMIT_BATCH_SIZE
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

    # TODO remove
    def delete_tmp(self):
        for i in range(3):
            print(i)
            query_string = (
                '{ location (first: 200, project_id: "'
                + self.project_id
                + '") { submitter_id, id } }'
            )
            # query_string = (
            #     '{ time_series (first: 200, project_id: "'
            #     + self.project_id
            #     + '") { submitter_id, id } }'
            # )
            response = requests.post(
                "{}/api/v0/submission/graphql".format(self.base_url),
                json={"query": query_string, "variables": None},
                headers=self.headers,
            )
            ids = [loc["id"] for loc in json.loads(response.text)["data"]["location"]]
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
