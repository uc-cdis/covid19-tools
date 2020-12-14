import csv
import re
from contextlib import closing

import requests
import datetime

from etl import base
from utils.metadata_helper import MetadataHelper


def format_location_submitter_id(country):
    """summary_location_<country>"""
    submitter_id = "summary_location_{}".format(country)
    submitter_id = submitter_id.lower().replace(", ", "_")
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)
    return submitter_id.strip("-")


def format_summary_clinical_submitter_id(location_submitter_id, date):
    return "{}_{}".format(
        location_submitter_id.replace("summary_location_", "summary_clinical_"), date
    )


def format_summary_summary_socio_demographic(location_submitter_id, date):
    return "{}_{}".format(
        location_submitter_id.replace(
            "summary_location_", "summary_socio_demographic_"
        ),
        date,
    )


class OWID2(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.summary_locations = []
        self.summary_clinicals = []
        self.summary_socio_demographics = []

        self.program_name = "open"
        self.project_code = "OWID"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.expected_csv_headers = [
            "iso_code",
            "continent",
            "location",
            "date",
            "total_cases",
            "new_cases",
            "new_cases_smoothed",
            "new_deaths",
            "new_deaths_smoothed",
            "total_cases_per_million",
            "new_cases_per_million",
            "new_cases_smoothed_per_million",
            "total_deaths_per_million",
            "new_deaths_per_million",
            "new_deaths_smoothed_per_million",
            "new_tests",
            "total_tests",
            "total_tests_per_thousand",
            "new_tests_per_thousand",
            "new_tests_smoothed",
            "new_tests_smoothed_per_thousand",
            "tests_per_case",
            "positive_rate",
            "tests_units",
            "stringency_index",
            "population",
            "population_density",
            "median_age",
            "aged_65_older",
            "aged_70_older",
            "gdp_per_capita",
            "extreme_poverty",
            "cardiovasc_death_rate",
            "diabetes_prevalence",
            "female_smokers",
            "male_smokers",
            "handwashing_facilities",
            "hospital_beds_per_thousand",
            "life_expectancy",
        ]

        self.header_to_column = {
            k: self.expected_csv_headers.index(k) for k in self.expected_csv_headers
        }

    def files_to_submissions(self):
        """
        Reads CSV files and converts the data to Sheepdog records
        """
        url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
        self.parse_file(url)

    def insert_row_value(self, row_value):
        summary_location_list = []
        (
            summary_location,
            summary_clinical,
            summary_socio_demographic,
        ) = row_value
        summary_location_submitter_id = summary_location["submitter_id"]
        if summary_location_submitter_id not in summary_location_list:
            self.summary_locations.append(summary_location)
            summary_location_list.append(summary_location_submitter_id)

        self.summary_clinicals.append(summary_clinical)
        self.summary_socio_demographics.append(summary_socio_demographic)

    def parse_file(self, url):
        """
        Converts a CSV file to data we can submit via Sheepdog. Stores the
        records to submit in `self.location_data` and `self.time_series_data`.
        Ignores any records that are already in Sheepdog (relies on unique
        `submitter_id` to check)

        Args:
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
            assert (
                set(expected_h).issubset(headers) == True
            ), "CSV headers have changed (expected {}, got {}). We may need to update the ETL code".format(
                expected_h, headers
            )

            pre_row = None

            for row in reader:
                res = self.parse_row(pre_row, row)
                if res is not None:
                    self.insert_row_value(res)
                pre_row = row
            if pre_row is not None:
                res = self.parse_row(pre_row, None)
                if res is not None:
                    self.insert_row_value(res)

    def create_clinical(self, row, date, summary_location_submitter_id):
        summary_clinical_submitter_id = format_summary_clinical_submitter_id(
            summary_location_submitter_id, date
        )

        summary_clinical = {
            "date": date,
            "submitter_id": summary_clinical_submitter_id,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        map_csv_fields = {
            # "iso_code": "iso_code",
            # "continent": "continent",
            # "location": "location",
            # "date": "date",
            "confirmed": ("total_cases", int),
            "new_cases": ("new_cases", int),
            "new_cases_smoothed": ("new_cases_smoothed", float),
            # "total_deaths": ("total_deaths", int),
            "new_deaths": ("new_deaths", int),
            "new_deaths_smoothed": ("new_deaths_smoothed", float),
            "total_cases_per_million": ("total_cases_per_million", float),
            "new_cases_per_million": ("new_cases_per_million", float),
            "new_cases_smoothed_per_million": ("new_cases_smoothed_per_million", float),
            "total_deaths_per_million": ("total_deaths_per_million", float),
            "new_deaths_per_million": ("new_deaths_per_million", float),
            "new_deaths_smoothed_per_million": (
                "new_deaths_smoothed_per_million",
                float,
            ),
            "new_tests": ("new_tests", int),
            "testing": ("total_tests", int),
            "total_tests_per_thousand": ("total_tests_per_thousand", float),
            "new_tests_per_thousand": ("new_tests_per_thousand", float),
            "new_tests_smoothed": ("new_tests_smoothed", float),
            "new_tests_smoothed_per_thousand": (
                "new_tests_smoothed_per_thousand",
                float,
            ),
            "tests_per_case": ("tests_per_case", float),
            "positive_rate": ("positive_rate", float),
            "tests_units": ("tests_units", str),
            "cardiovasc_death_rate": ("cardiovasc_death_rate", float),
            "diabetes_prevalence": ("diabetes_prevalence", float)
            # "hospital_beds_per_thousand": ("hospital_beds_per_thousand", float)
            # "human_development_index": ("human_development_index", float),
        }

        for k, (v, dtype) in map_csv_fields.items():
            value = row[self.header_to_column[v]]
            if value and value.lower() != "nan":
                try:
                    if dtype == int:
                        summary_clinical[k] = int(float(value.replace(",", "")))
                    elif dtype == float:
                        summary_clinical[k] = float(value.replace(",", ""))
                except Exception:
                    pass

        return summary_clinical

    def create_summary_socio_demographic(
        self, row, date, summary_location_submitter_id
    ):
        summary_socio_demographic_submitter_id = (
            format_summary_summary_socio_demographic(
                summary_location_submitter_id, date
            )
        )

        summary_socio_demographic = {
            "submitter_id": summary_socio_demographic_submitter_id,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        map_csv_socio_fields = {
            "stringency_index": ("stringency_index", float),
            "population": ("population", int),
            "population_density": ("population_density", float),
            "median_age": ("median_age", float),
            "aged_65_older": ("aged_65_older", float),
            "aged_70_older": ("aged_70_older", float),
            "gdp_per_capita": ("gdp_per_capita", float),
            "extreme_poverty": ("extreme_poverty", float),
            "female_smokers": ("female_smokers", float),
            "male_smokers": ("male_smokers", float),
            "handwashing_facilities": ("handwashing_facilities", float),
            "life_expectancy": ("life_expectancy", float),
        }

        for k, (v, dtype) in map_csv_socio_fields.items():
            value = row[self.header_to_column[v]]
            if value and value.lower() != "nan":
                try:
                    if dtype == int:
                        summary_socio_demographic[k] = int(
                            float(value.replace(",", ""))
                        )
                    elif dtype == float:
                        summary_socio_demographic[k] = float(value.replace(",", ""))
                except Exception:
                    pass

        return summary_socio_demographic

    def parse_row(self, pre_row, row):
        """
        Converts a row of a CSV file to data we can submit via Sheepdog

        Args:
            row (list(str)): row of data

        Returns:
            (dict, dict) tuple:
                - location data, in a format ready to be submitted to Sheepdog
                - { "date1": <value>, "date2": <value> } from the row data
        """
        if pre_row is None:
            return None

        pre_date = pre_row[self.header_to_column["date"]]
        pre_country = pre_row[self.header_to_column["location"]]
        pre_iso_code = pre_row[self.header_to_column["iso_code"]]

        if row is not None:
            iso_code = row[self.header_to_column["iso_code"]]

        if row is not None and pre_iso_code == iso_code:
            return None

        summary_location_submitter_id = format_location_submitter_id(pre_country)
        summary_location = {
            "country_region": pre_country,
            "submitter_id": summary_location_submitter_id,
            "projects": [{"code": self.project_code}],
        }

        return (
            summary_location,
            self.create_clinical(pre_row, pre_date, summary_location_submitter_id),
            self.create_summary_socio_demographic(
                pre_row, pre_date, summary_location_submitter_id
            ),
        )

    def submit_metadata(self):
        """
        Converts the data in `self.time_series_data` to Sheepdog records.
        `self.location_data already contains Sheepdog records. Batch submits
        all records in `self.location_data` and `self.time_series_data`
        """

        # Commented
        # Only required for one time submission of summary_location
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
        print("Submitting summary_socio_demographic data")
        for sc in self.summary_socio_demographics:
            sc_record = {"type": "summary_socio_demographic"}
            sc_record.update(sc)
            self.metadata_helper.add_record_to_submit(sc_record)
        self.metadata_helper.batch_submit_records()
