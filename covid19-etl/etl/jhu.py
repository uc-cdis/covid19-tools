from collections import defaultdict
from contextlib import closing
import csv
from datetime import datetime
import re
import requests

from etl import base
from utils.metadata_helper import MetadataHelper


# we are only keeping in Sheepdog the latest data (last date in the
# original data source) to avoid memory issues. when that's fixed,
# we can go back to storing all the JHU data.
LAST_DATE_ONLY = True


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


def format_summary_clinical_submitter_id(location_submitter_id, date):
    """summary_clinical_<country>_<province>_<county>_<date>"""
    sub_id = location_submitter_id.replace("summary_location", "summary_clinical")
    return "{}_{}".format(sub_id, date)


def time_series_date_to_string(date):
    return datetime.strptime(date, "%Y-%m-%d").isoformat("T")


class JHU(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.location_data = {}
        self.time_series_data = defaultdict(lambda: defaultdict(dict))
        self.program_name = "open"
        self.project_code = "JHU"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )
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
        self.existing_summary_locations = []
        self.last_date = ""

    def files_to_submissions(self):
        """
        Reads CSV files and converts the data to Sheepdog records
        """
        urls = {
            "global": {
                "confirmed": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv",
                "deaths": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv",
                "recovered": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv",
                # "testing": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_testing_global.csv",
            },
            "US_counties": {
                "confirmed": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv",
                "deaths": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv",
            },
        }

        (
            self.existing_summary_locations,
            self.last_date,
        ) = self.metadata_helper.get_existing_data_jhu()

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
            file_type (str): type of this file - one
                of ["global", "US_counties"]
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

            first_date_i = [i for i, h in enumerate(headers) if h.endswith("/20")][0]
            last_date = headers[-1]
            print(
                "  First date: {}; last date: {}".format(
                    headers[first_date_i], last_date
                )
            )

            for row in reader:
                if not row:  # ignore empty rows
                    continue
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
                    and location_submitter_id not in self.existing_summary_locations
                ):
                    self.location_data[location_submitter_id] = location

                for date, value in date_to_value.items():
                    # do not re-submit summary_clinical data that
                    # already exist. Assume anything older than the last
                    # submitted date has already been submitted
                    if (
                        time_series_date_to_string(date)
                        > time_series_date_to_string(self.last_date)
                        or LAST_DATE_ONLY
                    ):
                        self.time_series_data[location_submitter_id][date][
                            data_type
                        ] = value

    def parse_row(self, file_type, data_type, headers, row):
        """
        Converts a row of a CSV file to data we can submit via Sheepdog

        Args:
            file_type (str): type of this file - one
                of ["global", "US_counties"]
            data_type (str): type of the data in this file - one
                of ["confirmed", "deaths", "recovered"]
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
        latitude = row[header_to_column["latitude"]] or "0"
        longitude = row[header_to_column["longitude"]] or "0"

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
            "projects": [{"code": self.project_code}],
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
        dates_indices = range(dates_start, len(headers))
        if LAST_DATE_ONLY:
            dates_indices = [len(headers) - 1]
        for i in dates_indices:
            date = headers[i]
            date = get_unified_date_format(date)

            if row[i] == "":  # ignore empty values
                continue
            try:
                val = int(float(row[i]))
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
        if LAST_DATE_ONLY:
            # delete the old data from the Sheepdog DB
            print("Deleting old summary_clinical data")
            self.metadata_helper.delete_nodes(["summary_clinical"])

        print("Submitting summary_location data")
        for location in self.location_data.values():
            record = {"type": "summary_location"}
            record.update(location)
            self.metadata_helper.add_record_to_submit(record)
        self.metadata_helper.batch_submit_records()

        print("Submitting summary_clinical data")
        for location_submitter_id, time_series in self.time_series_data.items():
            for date, data in time_series.items():
                submitter_id = format_summary_clinical_submitter_id(
                    location_submitter_id, date
                )
                record = {
                    "type": "summary_clinical",
                    "submitter_id": submitter_id,
                    "summary_locations": [{"submitter_id": location_submitter_id}],
                    "date": date,
                }
                for data_type, value in data.items():
                    record[data_type] = value
                self.metadata_helper.add_record_to_submit(record)
        self.metadata_helper.batch_submit_records()
