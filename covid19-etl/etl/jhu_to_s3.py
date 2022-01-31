"""
This ETL generates the Illinois data used by the frontend and uploads this
data to S3. It replaced the "JHU_to_S3_global" ETL which generated data for
all the world countries, all the US states and all the US counties.
"""


import boto3
from contextlib import closing
import csv
from datetime import datetime
import json
import os
import pathlib
import time

from etl import base


"""
    We generate JSON files from the raw JHU CSV data for Illinois. One file
    with all the dates, sorted by county, used to display the map data by date
    for IL (1), plus one file per county, with the same data, used to display
    the time series plots (2). All these data files are pushed to S3 and
    accessed by the frontend.

    (1) Choropleth IL map JSON by date:
    "C" for Confirmed and "D" for Deaths (to reduce file size)
    {
        "il_county_list": {
            <US county FIPS>: {
                "county": <county name>,
                "by_date": {
                    "<date>": {
                        "C": <confirmed>
                        "D": <deaths>
                    },
                    ...
                },
                ...
            },
            ...
        },

        # Date data was last updated
        "last_updated": "2020-04-20",

        # Total number of cases at the latest date
        "totals": {
            "C": <total confirmed>,
            "D": <total deaths>
        }
    }

    (2) Times series data:
    In TIME_SERIES_DATA_FOLDER, the "county" folder contains one file per US
    IL county (<county FIPS>.json). Each JSON file is in the following format:
    {
        <date (YYYY-MM-DD)>: {
            C: 0,
            confirmed: 0,
            "D": 0,
            "deaths": 0,
        },
        ...
    }
"""


MAP_DATA_FOLDER = "map_data"
TIME_SERIES_DATA_FOLDER = "time_series"
IL_JSON_BY_DATE_FILENAME = "jhu_il_json_by_time_latest.json"
MINIMUM_COUNT = 5

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


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


def replace_small_counts_simple(data):
    # remove values smaller than the threshold
    count_replacement = f"<{MINIMUM_COUNT}"
    if data < MINIMUM_COUNT:
        data = count_replacement
    return data


class JHU_TO_S3(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.s3_client = boto3.client("s3")
        self.county_by_date = {}
        self.totals = {
            "C": 0,
            "D": 0,
        }
        self.latest_date = None

    def files_to_submissions(self):
        """
        Read CSV files and converts them to the formats described on top
        of this file.
        """
        urls = {
            "confirmed": {
                "url": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv",
                "expected_headers": [
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
                "header_to_column": {
                    "FIPS": 4,
                    "county": 5,
                    "province": 6,
                    "country": 7,
                    "dates_start": 11,
                },
            },
            "deaths": {
                "url": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv",
                "expected_headers": [
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
                    "Population",
                    "1/22/20",
                ],
                "header_to_column": {
                    "FIPS": 4,
                    "county": 5,
                    "province": 6,
                    "country": 7,
                    "dates_start": 12,
                },
            },
        }

        # create data folders
        data_folders = [
            os.path.join(CURRENT_DIR, MAP_DATA_FOLDER),
            os.path.join(CURRENT_DIR, TIME_SERIES_DATA_FOLDER),
            os.path.join(CURRENT_DIR, TIME_SERIES_DATA_FOLDER, "county"),
        ]
        for path in data_folders:
            pathlib.Path(path).mkdir(exist_ok=True)

        # generate data files
        for data_type, url_data in urls.items():
            self.parse_file(
                data_type,
                url_data["url"],
                url_data["expected_headers"],
                url_data["header_to_column"],
            )

        print("Latest date: {}".format(self.latest_date))

        # write map_data files
        for data_type in self.totals:
            self.totals[data_type] = replace_small_counts_simple(self.totals[data_type])
        with open(
            os.path.join(CURRENT_DIR, MAP_DATA_FOLDER, IL_JSON_BY_DATE_FILENAME), "w"
        ) as f:
            # create smaller file size by eliminating white space
            f.write(
                json.dumps(
                    {
                        "il_county_list": self.county_by_date,
                        "last_updated": self.latest_date,
                        "totals": self.totals,
                    },
                    separators=(",", ":"),
                )
            )

        # write time_series files
        for county_fips, data in self.county_by_date.items():
            with open(
                os.path.join(
                    CURRENT_DIR,
                    TIME_SERIES_DATA_FOLDER,
                    "county",
                    f"{county_fips}.json",
                ),
                "w",
            ) as f:
                f.write(
                    json.dumps(
                        data["by_date"],
                        separators=(",", ":"),
                    )
                )

    def parse_file(self, data_type, url, expected_h, header_to_column):
        """
        Args:
            data_type (str): type of the data in this file - one
                of ["confirmed", "deaths", "recovered"]
            url (str): URL at which the CSV file is available
            expected_h (list): expected CSV headers
            header_to_column (dict): mapping of CSV header to column number
        """
        print("Getting data from {}".format(url))
        with closing(self.get(url, stream=True)) as r:
            f = (line.decode("utf-8") for line in r.iter_lines())

            reader = csv.reader(f, delimiter=",", quotechar='"')

            headers = next(reader)
            if headers[0] == "404: Not Found":
                print("  Unable to get file contents, received {}.".format(headers))
                return

            obtained_h = headers[: len(expected_h)]
            assert (
                obtained_h == expected_h
            ), "CSV headers have changed (expected {}, got {}). We may need to update the ETL code".format(
                expected_h, obtained_h
            )

            file_latest_date = get_unified_date_format(headers[-1])
            if not self.latest_date or datetime.strptime(
                self.latest_date, "%Y-%m-%d"
            ) > datetime.strptime(file_latest_date, "%Y-%m-%d"):
                self.latest_date = file_latest_date

            for row in reader:
                self.parse_row(data_type, headers, row, header_to_column)

    def parse_row(self, data_type, headers, row, header_to_column):
        if not row:  # ignore empty rows
            return
        country = row[header_to_column["country"]]
        province = row[header_to_column["province"]]
        if country != "US" or province != "Illinois":
            return
        county = row[header_to_column["county"]]
        county_fips = row[header_to_column["FIPS"]]
        county_fips = int(float(county_fips))
        if county_fips not in self.county_by_date:
            self.county_by_date[county_fips] = {
                "county": county,
                "by_date": {},
            }
        dates_start = header_to_column["dates_start"]
        for i in range(dates_start, len(headers)):
            date = headers[i]
            date = get_unified_date_format(date)
            if date not in self.county_by_date[county_fips]["by_date"]:
                self.county_by_date[county_fips]["by_date"][date] = {}

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

            # store confirmed and deaths numbers
            val = replace_small_counts_simple(val)
            if data_type == "confirmed":
                self.county_by_date[county_fips]["by_date"][date]["C"] = val
            else:  # deaths
                self.county_by_date[county_fips]["by_date"][date]["D"] = val

        # update totals with the values for the latest date
        latest_val = int(float(row[-1]))
        if data_type == "confirmed":
            self.totals["C"] += latest_val
        else:  # deaths
            self.totals["D"] += latest_val

    def submit_metadata(self):
        print("Uploading to S3...")
        start = time.time()

        for folder in [MAP_DATA_FOLDER, TIME_SERIES_DATA_FOLDER]:
            for abs_path, _, files in os.walk(os.path.join(CURRENT_DIR, folder)):
                i = 0
                for file_name in files:
                    local_path = os.path.join(abs_path, file_name)
                    s3_path = os.path.relpath(local_path, CURRENT_DIR)
                    if folder == TIME_SERIES_DATA_FOLDER:
                        if i % 50 == 0:
                            print(f"  Uploading county data: {i} / {len(files)}")
                        i += 1
                    else:
                        print(f"  Uploading {s3_path}")
                    self.s3_client.upload_file(local_path, self.s3_bucket, s3_path)
                    os.remove(local_path)

        print("  Done in {} secs".format(int(time.time() - start)))
        print("Done!")
