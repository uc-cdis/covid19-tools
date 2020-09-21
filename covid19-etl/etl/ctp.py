import csv
import re
from contextlib import closing
from datetime import datetime

import requests

from etl import base
from helper.metadata_helper import MetadataHelper


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


def format_summary_clinical_submitter_id(location_submitter_id, date):
    return "{}_{}".format(
        location_submitter_id.replace("summary_location_", "summary_clinical_"), date
    )


class CTP(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.summary_locations = []
        self.summary_clinicals = []

        self.program_name = "open"
        self.project_code = "CTP"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.expected_file_headers = [
            "date",
            "state",
            "positive",
            "negative",
            "pending",
            "totalTestResults",
            "hospitalizedCurrently",
            "hospitalizedCumulative",
            "inIcuCurrently",
            "inIcuCumulative",
            "onVentilatorCurrently",
            "onVentilatorCumulative",
            "recovered",
            "dataQualityGrade",
            "lastUpdateEt",
            "dateModified",
            "checkTimeEt",
            "death",
            "hospitalized",
            "dateChecked",
            "totalTestsViral",
            "positiveTestsViral",
            "negativeTestsViral",
            "positiveCasesViral",
            "deathConfirmed",
            "deathProbable",
            "totalTestEncountersViral",
            "totalTestsPeopleViral",
            "totalTestsAntibody",
            "positiveTestsAntibody",
            "negativeTestsAntibody",
            "totalTestsPeopleAntibody",
            "positiveTestsPeopleAntibody",
            "negativeTestsPeopleAntibody",
            "totalTestsPeopleAntigen",
            "positiveTestsPeopleAntigen",
            "totalTestsAntigen",
            "positiveTestsAntigen",
            "fips",
            "positiveIncrease",
            "negativeIncrease",
            "total",
            "totalTestResultsSource",
            "totalTestResultsIncrease",
            "posNeg",
            "deathIncrease",
            "hospitalizedIncrease",
            "hash",
            "commercialScore",
            "negativeRegularScore",
            "negativeScore",
            "positiveScore",
            "score",
            "grade",
        ]

        self.expected_race_headers = [
            "Date",
            "State",
            "Cases_Total",
            "Cases_White",
            "Cases_Black",
            "Cases_LatinX",
            "Cases_Asian",
            "Cases_AIAN",
            "Cases_NHPI",
            "Cases_Multiracial",
            "Cases_Other",
            "Cases_Unknown",
            "Cases_Ethnicity_Hispanic",
            "Cases_Ethnicity_NonHispanic",
            "Cases_Ethnicity_Unknown",
            "Deaths_Total",
            "Deaths_White",
            "Deaths_Black",
            "Deaths_LatinX",
            "Deaths_Asian",
            "Deaths_AIAN",
            "Deaths_NHPI",
            "Deaths_Multiracial",
            "Deaths_Other",
            "Deaths_Unknown",
            "Deaths_Ethnicity_Hispanic",
            "Deaths_Ethnicity_NonHispanic",
            "Deaths_Ethnicity_Unknown",
        ]

        self.expected_csv_headers = (
            self.expected_file_headers + self.expected_race_headers[3:]
        )
        self.header_to_column = {
            k: self.expected_csv_headers.index(k) for k in self.expected_csv_headers
        }
        print("The length of the expected headers: ", len(self.header_to_column))

    def files_to_submissions(self):
        """
        Reads CSV files and converts the data to Sheepdog records
        """
        url = "https://api.covidtracking.com/v1/states/daily.csv"
        self.parse_file(url)

    def extract_races(self):
        """
        Extract race information. Store the data to a dictionary for
        fast lookup during merging process.

        """
        url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vR_xmYt4ACPDZCDJcY12kCiMiH0ODyx3E1ZvgOHB8ae1tRcjXbs_yWBOA4j4uoCEADVfC1PS2jYO68B/pub?gid=43720681&single=true&output=csv"
        races = {}
        with closing(requests.get(url, stream=True)) as r:
            f = (line.decode("utf-8") for line in r.iter_lines())
            reader = csv.reader(f, delimiter=",", quotechar='"')
            headers = next(reader)
            if headers[0] == "404: Not Found":
                print("  Unable to get file contents, received {}.".format(headers))
                return

            expected_h = self.expected_race_headers
            obtained_h = headers[: len(expected_h)]
            assert (
                obtained_h == expected_h
            ), "CSV headers have changed (expected {}, got {}). We may need to update the ETL code".format(
                expected_h, obtained_h
            )
            for row in reader:
                races[(row[0], row[1], row[2])] = row[3:]
        return races

    def parse_file(self, url):
        """
        Converts a CSV file to data we can submit via Sheepdog. Stores the
        records to submit in `self.location_data` and `self.time_series_data`.
        Ignores any records that are already in Sheepdog (relies on unique
        `submitter_id` to check)

        Args:
            url (str): URL at which the CSV file is available
        """
        races = self.extract_races()
        print("Getting data from {}".format(url))
        with closing(requests.get(url, stream=True)) as r:
            f = (line.decode("utf-8") for line in r.iter_lines())
            reader = csv.reader(f, delimiter=",", quotechar='"')

            headers = next(reader)

            if headers[0] == "404: Not Found":
                print("  Unable to get file contents, received {}.".format(headers))
                return

            expected_h = self.expected_file_headers
            obtained_h = headers[: len(expected_h)]
            assert (
                obtained_h == expected_h
            ), "CSV headers have changed (expected {}, got {}). We may need to update the ETL code".format(
                expected_h, obtained_h
            )

            summary_location_list = []

            for row in reader:
                if (row[0], row[1], row[2]) in races:
                    [row.append(k) for k in races[(row[0], row[1], row[2])]]
                else:
                    [row.append("") for _ in range(len(self.expected_race_headers) - 3)]

                summary_location, summary_clinical = self.parse_row(row)

                summary_location_submitter_id = summary_location["submitter_id"]
                if summary_location_submitter_id not in summary_location_list:
                    self.summary_locations.append(summary_location)
                    summary_location_list.append(summary_location_submitter_id)

                self.summary_clinicals.append(summary_clinical)

    def parse_row(self, row):
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

        fips = row[self.header_to_column["fips"]]
        if fips:
            summary_location["FIPS"] = int(fips)

        summary_clinical_submitter_id = format_summary_clinical_submitter_id(
            summary_location_submitter_id, date
        )
        summary_clinical = {
            "date": date,
            "submitter_id": summary_clinical_submitter_id,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        map_csv_fields = {
            "confirmed": "positive",
            "negative": "negative",
            "pending": "pending",
            "hospitalizedCurrently": "hospitalizedCurrently",
            "hospitalizedCumulative": "hospitalizedCumulative",
            "inIcuCurrently": "inIcuCurrently",
            "inIcuCumulative": "inIcuCumulative",
            "onVentilatorCurrently": "onVentilatorCurrently",
            # "": "onVentilatorCumulative",
            "recovered": "recovered",
            # "": "dataQualityGrade",
            # "": "lastUpdateEt",
            # "": "dateModified",
            # "": "checkTimeEt",
            # "": "death",
            # "": "hospitalized",
            # "": "dateChecked",
            "totalTestsViral": "totalTestsViral",
            "positiveTestsViral": "positiveTestsViral",
            "negativeTestsViral": "negativeTestsViral",
            "positiveCasesViral": "positiveCasesViral",
            # "deaths": "deathConfirmed",
            # "": "deathProbable",
            # "": "totalTestEncountersViral",
            # "": "totalTestsPeopleViral",
            # "": "totalTestsAntibody",
            # "": "positiveTestsAntibody",
            # "": "negativeTestsAntibody",
            # "": "totalTestsPeopleAntibody",
            # "": "positiveTestsPeopleAntibody",
            # "": "negativeTestsPeopleAntibody",
            # "": "totalTestsPeopleAntigen",
            # "": "positiveTestsPeopleAntigen",
            # "": "totalTestsAntigen",
            # "": "positiveTestsAntigen",
            # "": "fips",
            "positiveIncrease": "positiveIncrease",
            "negativeIncrease": "negativeIncrease",
            # "": "total",
            # "": "totalTestResults",
            "totalTestResultsIncrease": "totalTestResultsIncrease",
            # "": "posNeg",
            "deathIncrease": "deathIncrease",
            "hospitalizedIncrease": "hospitalizedIncrease",
            # "": "hash",
            # "": "commercialScore",
            # "": "negativeRegularScore",
            # "": "negativeScore",
            # "": "positiveScore",
            "race_white_count": "Cases_White",
            "race_black_count": "Cases_Black",
            "race_hispanic_count": "Cases_LatinX",
            "race_asian_count": "Cases_Asian",
            "race_ai_an_count": "Cases_AIAN",
            "race_nh_pi_count": "Cases_NHPI",
            "race_multiracial_count": "Cases_Multiracial",
            "race_other_count": "Cases_Other",
            "race_left_blank_count": "Cases_Unknown",
            "ethnicity_hispanic_count": "Cases_Ethnicity_Hispanic",
            "ethnicity_nonhispanic_count": "Cases_Ethnicity_NonHispanic",
            "ethnicity_unknown_count": "Cases_Ethnicity_Unknown",
            "deaths": "Deaths_Total",
            "race_white_deaths": "Deaths_White",
            "race_black_deaths": "Deaths_Black",
            "race_hispanic_deaths": "Deaths_LatinX",
            "race_asian_deaths": "Deaths_Asian",
            "race_ai_an_deaths": "Deaths_AIAN",
            "race_nh_pi_deaths": "Deaths_NHPI",
            "race_multiracial_deaths": "Deaths_Multiracial",
            "race_other_deaths": "Deaths_Other",
            "race_left_blank_deaths": "Deaths_Unknown",
            "ethnicity_hispanic_deaths": "Deaths_Ethnicity_Hispanic",
            "ethnicity_nonhispanic_deaths": "Deaths_Ethnicity_NonHispanic",
            "ethnicity_unknown_deaths": "Deaths_Ethnicity_Unknown",
        }

        for k, v in map_csv_fields.items():
            value = row[self.header_to_column[v]]
            if value and value.lower() != "nan":
                summary_clinical[k] = int(value.replace(",", ""))

        dataQualityGrade = row[self.header_to_column["dataQualityGrade"]]
        if dataQualityGrade:
            summary_clinical["dataQualityGrade"] = dataQualityGrade

        lastUpdateEt = row[self.header_to_column["lastUpdateEt"]]
        if lastUpdateEt:
            summary_clinical["lastUpdateEt"] = lastUpdateEt

        return summary_location, summary_clinical

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

        # print("Submitting summary_clinical data")
        for sc in self.summary_clinicals:
            sc_record = {"type": "summary_clinical"}
            sc_record.update(sc)
            self.metadata_helper.add_record_to_submit(sc_record)
        self.metadata_helper.batch_submit_records()
