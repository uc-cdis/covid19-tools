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

    etl = JonhsHopkinsETL(token)
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
    sub_id = location_submitter_id.replace("summary_location", "summary_report")
    return "{}_{}".format(sub_id, date)


def format_time_series_date(date):
    return datetime.strptime(date, "%Y-%m-%d").isoformat("T")


class JonhsHopkinsETL:
    def __init__(self, access_token):
        self.location_data = {}
        self.time_series_data = defaultdict(lambda: defaultdict(dict))
        self.metadata_helper = MetadataHelper(access_token=access_token)
        self.expected_csv_headers = {
            "global": ["Province/State", "Country/Region", "Lat", "Long", "1/22/20"],
            "US_counties": {
                "confirmed": [
                    "UID",
                    "iso2",
                    "iso3",
                    "code3",
                    "FIPS",
                    "Admin2",
                    "Province_State",
                    "Country_Region",
                    "Lat",
                    "Long_",
                    "Combined_Key",
                    "1/22/20",
                ],
                "deaths": [
                    "UID",
                    "iso2",
                    "iso3",
                    "code3",
                    "FIPS",
                    "Admin2",
                    "Province_State",
                    "Country_Region",
                    "Lat",
                    "Long_",
                    "Combined_Key",
                    "Population",  # TODO use this
                    "1/22/20",
                ],
            },
        }
        self.header_to_column = {
            "global": {
                "province": 0,
                "country": 1,
                "latitude": 2,
                "longitude": 3,
                "dates_start": 4,
            },
            "US_counties": {
                "confirmed": {
                    "iso2": 1,
                    "iso3": 2,
                    "code3": 3,
                    "FIPS": 4,
                    "county": 5,
                    "province": 6,
                    "country": 7,
                    "latitude": 8,
                    "longitude": 9,
                    "dates_start": 11,
                },
                "deaths": {
                    "iso2": 1,
                    "iso3": 2,
                    "code3": 3,
                    "FIPS": 4,
                    "county": 5,
                    "province": 6,
                    "country": 7,
                    "latitude": 8,
                    "longitude": 9,
                    "dates_start": 12,
                },
            },
        }
        self.existing_data = self.metadata_helper.get_existing_data()

    def files_to_submissions(self):
        """
        Reads CSV files and converts the data to Sheepdog records
        """
        # self.metadata_helper.delete_tmp()  # TODO remove
        # return
        urls = {
            "global": {
                "confirmed": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv",
                "deaths": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv",
                "recovered": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv",
                "testing": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_testing_global.csv",
            },
            "US_counties": {
                "confirmed": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv",
                "deaths": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv",
            },
        }

        for file_type in ["global", "US_counties"]:
            for data_type, url in urls[file_type].items():
                self.parse_file(file_type, data_type, url)

    def parse_file(self, file_type, data_type, url):
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

            expected_h = self.expected_csv_headers[file_type]
            if isinstance(expected_h, dict):
                expected_h = expected_h[data_type]
            obtained_h = headers[: len(expected_h)]
            assert (
                obtained_h == expected_h
            ), "CSV headers have changed (expected {}, got {}). We may need to update the ETL code".format(
                expected_h, obtained_h
            )

            for row in reader:
                location, date_to_value = self.parse_row(
                    file_type, data_type, headers, row
                )
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
                    date_submitter_id = format_report_submitter_id(
                        location_submitter_id, date
                    )
                    # do not re-submit time_series data that already exist
                    if date_submitter_id not in self.existing_data.get(
                        location_submitter_id, []
                    ):
                        self.time_series_data[location_submitter_id][date][
                            data_type
                        ] = value

    def parse_row(self, file_type, data_type, headers, row):
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
        header_to_column = self.header_to_column[file_type]
        if "country" not in header_to_column:
            header_to_column = header_to_column[data_type]

        country = row[header_to_column["country"]]
        province = row[header_to_column["province"]]
        latitude = row[header_to_column["latitude"]]
        longitude = row[header_to_column["longitude"]]

        if country == "US" and province == "":
            # We are using US data by state instead of global
            return None, None

        if int(float(latitude)) == 0 and int(float(longitude)) == 0:
            # Data with "Out of <state>" or "Unassigned" county value have
            # unknown coordinates of (0,0). We don't submit them for now
            return None, None

        submitter_id = format_location_submitter_id(country, province)
        location = {
            "country_region": country,
            "latitude": latitude,
            "longitude": longitude,
            "projects": [{"code": PROJECT_CODE}],
        }
        if province:
            location["province_state"] = province
        if file_type == "US_counties":
            county = row[header_to_column["county"]]
            iso2 = row[header_to_column["iso2"]]
            iso3 = row[header_to_column["iso3"]]
            code3 = row[header_to_column["code3"]]
            fips = row[header_to_column["FIPS"]]
            if county:
                location["county"] = county
                submitter_id = format_location_submitter_id(country, province, county)
            if iso2:
                location["iso2"] = iso2
            if iso3:
                location["iso3"] = iso3
            if code3:
                location["code3"] = int(code3)
            if fips:
                location["FIPS"] = int(float(fips))
        location["submitter_id"] = submitter_id

        date_to_value = {}
        dates_start = header_to_column["dates_start"]
        for i in range(dates_start, len(headers)):
            date = headers[i]
            date = get_unified_date_format(date)

            if row[i] == "":  # ignore empty values
                continue
            try:
                val = int(row[i])
            except ValueError:
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

        print("Submitting summary_location data")
        for location in self.location_data.values():
            record = {"type": "summary_location"}
            record.update(location)
            self.metadata_helper.add_record_to_submit(record)
        self.metadata_helper.batch_submit_records()

        print("Submitting summary_report data")
        for location_submitter_id, time_series in self.time_series_data.items():
            for date, data in time_series.items():
                submitter_id = format_report_submitter_id(location_submitter_id, date)
                record = {
                    "type": "summary_report",
                    "submitter_id": submitter_id,
                    "summary_locations": [{"submitter_id": location_submitter_id}],
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
            location_id = report_id.replace("summary_report", "summary_location")
            location_id = "_".join(location_id.split("_")[:-1])  # remove the date
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
                i * SUBMIT_BATCH_SIZE : (i + 1) * SUBMIT_BATCH_SIZE
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
                '{ summary_location (first: 0, project_id: "'
                + self.project_id
                + '") { id, summary_reports (first: 0) { id } } }'
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
