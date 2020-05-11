import csv
import re
from contextlib import closing
from datetime import datetime

import requests


from etl import base
from helper.metadata_helper import MetadataHelper

# Note: if we end up having too much data, Sheepdog submissions may
# time out. We'll have to use a smaller batch size and hope that's enough
SUBMIT_BATCH_SIZE = 100


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
    return "{}_{}".format(
        location_submitter_id.replace("summary_location_", "summary_report_"), date
    )


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


class CTP(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.summary_locations = []
        self.summary_reports = []

        self.program_name = "open"
        self.project_code = "CTP"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.expected_csv_headers = [
            "date",
            "state",
            "positive",
            "negative",
            "pending",
            "hospitalizedCurrently",
            "hospitalizedCumulative",
            "inIcuCurrently",
            "inIcuCumulative",
            "onVentilatorCurrently",
            "onVentilatorCumulative",
            "recovered",
            "hash",
            "dateChecked",
            "death",
            "hospitalized",
            "total",
            "totalTestResults",
            "posNeg",
            "fips",
            "deathIncrease",
            "hospitalizedIncrease",
            "negativeIncrease",
            "positiveIncrease",
            "totalTestResultsIncrease",
        ]

        self.header_to_column = {
            k: self.expected_csv_headers.index(k) for k in self.expected_csv_headers
        }

    def files_to_submissions(self):
        """
        Reads CSV files and converts the data to Sheepdog records
        """
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
        summary_location_submitter_id = format_location_submitter_id(country, state)

        summary_location = {
            "country_region": country,
            "submitter_id": summary_location_submitter_id,
            "projects": [{"code": self.project_code}],
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
            "confirmed": "positive",
            "testing": "totalTestResultsIncrease",
            "deaths": "death",
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
