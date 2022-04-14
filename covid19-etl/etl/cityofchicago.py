#  This ETL is for city of chicago dataset for COVID-19 Daily Cases, Deaths, and Hospitalizations (CDH)
#  Reference: https://data.cityofchicago.org/Health-Human-Services/COVID-19-Daily-Cases-Deaths-and-Hospitalizations/naz8-j4nc

import boto3
import csv
from datetime import datetime
import time
import json
import requests
import pandas as pd
from contextlib import closing
from utils.metadata_helper import MetadataHelper
from utils.format_helper import (
    derived_submitter_id,
    format_submitter_id,
)
from etl import base
import sys

CITYOFCHICAGO_CDH_URL = "https://data.cityofchicago.org/resource/naz8-j4nc.csv"


class CITYOFCHICAGO(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        self.base_url = base_url
        self.access_token = access_token
        self.s3_bucket = s3_bucket

        self.program_name = "open"
        self.project_code = "cityofchicago"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=self.access_token,
        )

        self.city = "chicago"
        self.county = "Cook"
        self.country = "US"
        self.state = "IL"
        self.latest_submitted_date = None

        self.summary_locations = {}  # { <submitter_id>: <record> }
        self.summary_clinicals = {}
        self.summary_group_demographics = {}

        self.summary_location_submitter_id = ""

        self.headers = []

        self.expected_csv_headers = [
            "lab_report_date",
            "cases_total",
            "deaths_total",
            "hospitalizations_total",
            "cases_age_0_17",
            "cases_age_18_29",
            "cases_age_30_39",
            "cases_age_40_49",
            "cases_age_50_59",
            "cases_age_60_69",
            "cases_age_70_79",
            "cases_age_80_",
            "cases_age_unknown",
            "cases_female",
            "cases_male",
            "cases_unknown_gender",
            "cases_latinx",
            "cases_asian_non_latinx",
            "cases_black_non_latinx",
            "cases_white_non_latinx",
            "cases_other_non_latinx",
            "cases_unknown_race_eth",
            "deaths_0_17_yrs",
            "deaths_18_29_yrs",
            "deaths_30_39_yrs",
            "deaths_40_49_yrs",
            "deaths_50_59_yrs",
            "deaths_60_69_yrs",
            "deaths_70_79_yrs",
            "deaths_80_yrs",
            "deaths_unknown_age",
            "deaths_female",
            "deaths_male",
            "deaths_unknown_gender",
            "deaths_latinx",
            "deaths_asian_non_latinx",
            "deaths_black_non_latinx",
            "deaths_white_non_latinx",
            "deaths_other_non_latinx",
            "deaths_unknown_race_eth",
            "hospitalizations_age_0_17",
            "hospitalizations_age_18_29",
            "hospitalizations_age_30_39",
            "hospitalizations_age_40_49",
            "hospitalizations_age_50_59",
            "hospitalizations_age_60_69",
            "hospitalizations_age_70_79",
            "hospitalizations_age_80_",
            "hospitalizations_age_unknown",
            "hospitalizations_female",
            "hospitalizations_male",
            "hospitalizations_unknown_gender",
            "hospitalizations_latinx",
            "hospitalizations_asian_non_latinx",
            "hospitalizations_black_non_latinx",
            "hospitalizations_white_non_latinx",
            "hospitalizations_other_race_non_latinx",
            "hospitalizations_unknown_race_ethnicity",
        ]

    def get_unified_date_format(self, datetime):
        """
        Method to parse and check the date from datasource used in this ETL
        """
        date = datetime
        if "T" in datetime:
            date = datetime.split("T")[0]
        year, month, day = date.split("-")
        # format all the dates the same way
        if len(year) == 2:
            year = "20{}".format(year)
        if len(month) == 1:
            month = "0{}".format(month)
        if len(day) == 1:
            day = "0{}".format(day)
        return "-".join((year, month, day))

    def convert_str_to_int(self, string):
        # Method to parse numbers from string to int and 0 if string it empty

        return int(string or 0)

    def get_summary_location(self):
        # This dataset would only require one `summary_location` which is chicago, so the entry is made in `summary_location` only if it doesn't already exsist

        current_summary_location = self.metadata_helper.get_existing_summary_locations()
        if len(current_summary_location) > 0:
            return
        else:
            self.summary_locations[self.summary_location_submitter_id] = {
                "country_region": self.country,
                "county": self.county,
                "province_state": self.state,
                "projects": [{"code": self.project_code}],
            }

    def add_summary_clinical(
        self, cases_total, deaths_total, hospitalizations_total, today
    ):

        # To add `summary_clinical` data for each date in dataset with total of cases, deaths and hospitalization records

        summary_clinical_submitter_id = derived_submitter_id(
            self.summary_location_submitter_id,
            "summary_location",
            "summary_clinical",
            {"date": today.strftime("%Y-%m-%d")},
        )
        summary_clinical = {
            "submitter_id": summary_clinical_submitter_id,
            "count": cases_total,
            "deaths": deaths_total,
            "hospitaliIzedCumulative": hospitalizations_total,
            "date": today.strftime("%Y-%m-%dT%H:%M:%S"),
            "summary_locations": [{"submitter_id": self.summary_location_submitter_id}],
        }
        self.summary_clinicals[summary_clinical_submitter_id] = summary_clinical
        return summary_clinical_submitter_id

    def add_to_summary_group_demographics(
        self, header_value, record_value, summary_clinical_submitter_id
    ):

        # To add `summary_group_demographics` data for each date in dataset with total of cases, deaths and hospitalization records

        # `summary_group_demographics` have summary of covid data for either AgeGroup, Race, Gender
        age_group_dict = {"AgeGroup": "None", "Race": "None", "Gender": "None"}

        # This dict variable have valueset for for `summary_group_demographics`
        summary_group_demographics_value_dict = {
            "age_group": "",
            "race": "",
            "gender": "",
            "ethnicity": "",
            "count": 0,
            "deaths": 0,
            "hospitalizations": 0,
            "summary_clinicals": [{"submitter_id": summary_clinical_submitter_id}],
        }

        # age group mapping for value from original dataset to value in Gen3 data dictionary
        age_group = {
            "age_0_17": "less than 17",
            "age_18_29": "18 to 29",
            "age_30_39": "30 to 39",
            "age_40_49": "40 to 49",
            "age_50_59": "50 to 59",
            "age_60_69": "60 to 69",
            "age_70_79": "70 to 79",
            "age_80_": "greater than 80",
            "age_unknown": "unknown",
        }

        # Race mapping for submitter id value from original dataset to value in Gen3 data dictionary
        race_submitter_id = {
            "latinx": "Hispanic or Latino",
            "asian_non_latinx": "Asian",
            "black_non_latinx": "Black or African-American",
            "white_non_latinx": "White",
            "other_race_non_latinx": "Other race",
            "unknown_race_ethnicity": "Unknown",
        }

        # race mapping for value from original dataset to value in Gen3 data dictionary
        race = {
            "latinx": "Hispanic",
            "asian_non_latinx": "Asian",
            "black_non_latinx": "Black",
            "white_non_latinx": "White",
            "other_race_non_latinx": "Other",
            "unknown_race_ethnicity": "Unknown",
        }

        # Gender for value from original dataset to value in Gen3 data dictionary
        gender = {
            "female": "Female",
            "male": "Male",
            "unknown_gender": "Unknown",
        }

        record_type_mapping = {
            "cases": "count",
            "deaths": "deaths",
            "hospitalizations": "hospitalizations",
        }

        record_type, submitter_value = header_value.split("_", maxsplit=1)
        if submitter_value in age_group:
            if "80" in submitter_value:
                age_group_dict["AgeGroup"] = "80+"
            else:
                age_group_dict["AgeGroup"] = age_group[submitter_value].replace(
                    " to ", "_"
                )
            summary_group_demographics_value_dict["age_group"] = age_group[
                submitter_value
            ]

        elif submitter_value in race:
            age_group_dict["Race"] = race_submitter_id[submitter_value]
            summary_group_demographics_value_dict["race"] = race[submitter_value]
            if submitter_value == "latinx":
                summary_group_demographics_value_dict["ethnicity"] = "Hispanic"
            else:
                summary_group_demographics_value_dict["ethnicity"] = "Nonhispanic"

        elif submitter_value in gender:
            age_group_dict["Gender"] = gender[submitter_value]
            if submitter_value == "unknown_gender":
                summary_group_demographics_value_dict[
                    "gender"
                ] = "Unknown or Left Blank"
            else:
                summary_group_demographics_value_dict["gender"] = gender[
                    submitter_value
                ]

        else:
            return

        summary_group_demographics_submitter_id = derived_submitter_id(
            summary_clinical_submitter_id,
            "summary_clinical",
            "summary_group_demographic",
            age_group_dict,
        )

        if (
            summary_group_demographics_submitter_id
            not in self.summary_group_demographics
        ):
            self.summary_group_demographics[
                summary_group_demographics_submitter_id
            ] = summary_group_demographics_value_dict
        self.summary_group_demographics[summary_group_demographics_submitter_id][
            record_type_mapping[record_type]
        ] = record_value

    def parse_cityofchicago_file(self, start_date, end_date):
        # parse original file into value to be passed in sheepdog
        city_of_chicago_url = (
            CITYOFCHICAGO_CDH_URL
            + "?$where=lab_report_date between '"
            + start_date
            + "' and '"
            + end_date
            + "'"
        )

        with closing(requests.get(city_of_chicago_url, stream=True)) as r:
            f = (line.decode("utf-8") for line in r.iter_lines())
            reader = csv.reader(f, delimiter=",", quotechar='"')
            self.headers = next(reader)

            if self.headers[0] == "404: Not Found":
                print("Unable to get file contents, received {}.".format(self.headers))
                return

            obtained_h = self.headers[: len(self.expected_csv_headers)]
            assert (
                obtained_h == self.expected_csv_headers
            ), "CSV headers have changed (expected {}, got {}). We may need to update the ETL code".format(
                self.expected_csv_headers, obtained_h
            )

            for row in reader:
                self.parse_row(row)

    def parse_row(self, row):

        """
        according to row mapping in the dataset, in each row
        column 0 would be lab report date
        column 1,2 and 3 would be total number of cases, deaths and hospitalization per day which would be used in summary_clinical
        column 3 to rest would be for summary_group_demographics for Age group, race , gender and ethincity
        Here we are ignoring records which doesn't have any lab report dates
        """
        if row[0] != "":
            row_dict = {}
            file_latest_date = self.get_unified_date_format(row[0])
            row_dict[self.headers[0]] = file_latest_date
            summary_clinical_submitter_id = self.add_summary_clinical(
                self.convert_str_to_int(row[1]),
                self.convert_str_to_int(row[2]),
                self.convert_str_to_int(row[3]),
                datetime.strptime(file_latest_date, "%Y-%m-%d"),
            )
            for i in range(3, len(self.headers)):
                row_dict[self.headers[i]] = self.convert_str_to_int(row[i])
                self.add_to_summary_group_demographics(
                    self.headers[i],
                    row_dict[self.headers[i]],
                    summary_clinical_submitter_id,
                )

    def files_to_submissions(self):
        # ETL code that reads from the data source
        # and generates the data to submit
        start = time.time()
        latest_submitted_date = self.metadata_helper.get_latest_submitted_date_idph()

        # The following condition is for the first entry in dataset, which is from date `03-01-2020`
        if latest_submitted_date == None:
            latest_submitted_date = datetime.strptime("2020-03-01", "%Y-%m-%d")

        today = datetime.today()
        today = today.replace(hour=0, minute=0, second=0, microsecond=0)

        if latest_submitted_date == today:
            print("Nothing to submit: today and latest submitted date are the same.")
            return

        print(
            f"Latest submitted date: {latest_submitted_date}. Getting data until date: {today}"
        )

        self.summary_location_submitter_id = format_submitter_id(
            "summary_location",
            {"country": self.country, "state": self.state, "city": self.city},
        )

        self.get_summary_location()
        self.parse_cityofchicago_file(
            latest_submitted_date.strftime("%Y-%m-%dT%H:%M:%S"),
            today.strftime("%Y-%m-%dT%H:%M:%S"),
        )
        print("Done in {} secs".format(int(time.time() - start)))

    def submit_metadata(self):
        # Submits the data in `self.summary_locations`, `self.summary_clinicals` and `self.summary_group_demographic` to Sheepdog.
        print("Submitting data...")
        if len(self.summary_locations) > 0:
            print("Submitting summary_location data")
            for sl in self.summary_locations.values():
                sl_record = {"type": "summary_location"}
                sl_record.update(sl)
                self.metadata_helper.add_record_to_submit(sl_record)
            self.metadata_helper.batch_submit_records()

        print("Submitting summary_clinical data")
        for sc in self.summary_clinicals.values():
            sc_record = {"type": "summary_clinical"}
            sc_record.update(sc)
            self.metadata_helper.add_record_to_submit(sc_record)
        self.metadata_helper.batch_submit_records()

        print("Submitting summary_group_demographic data")
        for sgd in self.summary_group_demographics.values():
            sgd_record = {"type": "summary_group_demographics"}
            sgd_record.update(sgd)
            self.metadata_helper.add_record_to_submit(sgd_record)
        self.metadata_helper.batch_submit_records()
