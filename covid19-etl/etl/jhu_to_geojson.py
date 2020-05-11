import boto3
from copy import deepcopy
import csv
import json
import os
import re
import requests
import time
from collections import defaultdict
from contextlib import closing
from datetime import datetime

from etl import base
from utils.country_codes_utils import get_codes_dictionary, get_codes_for_country_name


"""
    First, we use the raw CSV data to generate self.nestedDict.
    It contains the data for all dates.

    Then, we use self.nestedDict to generate a GeoJson file.
    It only contains the data for the latest available date.
    It's used to display the density map.

    Then, we use self.nestedDict to generate a JSON file. TODO
    It only contains the data for the latest available date.
    The data is organized by country, state and county.
    It's used to display the choropleth map.

    Finally, we use self.nestedDict to generate JSON files with
    all the dates, sorted by country or state. TODO
    They are used to display the time series plots.

    All these data files are pushed to S3.

    self.nestedDict:
    {
        <country ISO3>: {
            "country_region": '',
            "latitude": '',
            "longitude": '',
            ... other properties,
            "provinces": {
                <province/state name>: {
                    "province_state": '',
                    "latitude": '',
                    "longitude": '',
                    ... other properties,
                    "counties": {
                        <county FIPS>: {
                            "county": '',
                            "latitude": '',
                            "longitude": '',
                            ... other properties,
                            "time_series": {
                                <date>: {
                                    "confirmed": 0,
                                    "deaths": 0,
                                }
                            },
                        }
                    },
                    "time_series": {
                        <date>: {
                            "confirmed": 0,
                            "deaths": 0,
                        }
                    },
                }
            },
            "time_series": {
                <date>: {
                    "confirmed": 0,
                    "deaths": 0,
                }
            },
        }
    }
"""
# TODO describe the 2 other formats


GEOJSON_FILENAME = "jhu_geojson_latest.json"

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


# TODO: merge it with existing JHU ETL
class JHU_TO_GEOJSON(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.nestedDict = {}
        self.codes_dict = get_codes_dictionary()
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
        self.latest_date = None

    def files_to_submissions(self):
        """
        Reads CSV files and converts them to the formats described on top
        of this file.
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

        start = time.time()

        for file_type in ["global", "US_counties"]:
            for data_type, url in urls[file_type].items():
                self.parse_file_to_nexted_dict(file_type, data_type, url)

        self.nested_dict_to_geojson()
        self.nested_dict_to_data_by_level()

        print("Generated files in {} secs".format(int(time.time() - start)))
        print("Latest date: {}".format(self.latest_date))

    def parse_file_to_nexted_dict(self, file_type, data_type, url):
        """
        Converts a CSV file to self.nestedDict in the format described on
        top of this file.

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

            file_latest_date = get_unified_date_format(headers[-1])
            if not self.latest_date or datetime.strptime(
                self.latest_date, "%Y-%m-%d"
            ) > datetime.strptime(file_latest_date, "%Y-%m-%d"):
                self.latest_date = file_latest_date

            for row in reader:
                self.parse_row(file_type, data_type, headers, row)

    def parse_row(self, file_type, data_type, headers, row):
        """
        Converts a row of a CSV file to the self.nestedDict in the format
        described on top of this file.

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
        latitude = row[header_to_column["latitude"]] or "0"
        longitude = row[header_to_column["longitude"]] or "0"

        if int(float(latitude)) == 0 and int(float(longitude)) == 0:
            # Data with "Out of <state>" or "Unassigned" county value have
            # unknown coordinates of (0,0). We don't submit them for now
            return None, None

        codes = get_codes_for_country_name(self.codes_dict, country)
        iso3 = codes["iso3"]
        if iso3 not in self.nestedDict:
            self.nestedDict[iso3] = {
                "country_region": country,
                "latitude": latitude,
                "longitude": longitude,
                "iso2": codes["iso2"],
                "iso3": iso3,
                "provinces": {},
                "time_series": defaultdict(dict),
            }

        province = row[header_to_column["province"]]
        if province and province not in self.nestedDict[iso3]["provinces"]:
            self.nestedDict[iso3]["provinces"][province] = {
                "province_state": province,
                "latitude": latitude,
                "longitude": longitude,
                "counties": {},
                "time_series": defaultdict(dict),
            }

        fips = None
        if file_type == "US_counties":
            fips = row[header_to_column["FIPS"]]
            county = row[header_to_column["county"]]
            iso2 = row[header_to_column["iso2"]]
            county_iso3 = row[header_to_column["iso3"]]
            code3 = row[header_to_column["code3"]]
            if fips:
                fips = int(float(fips))
                if fips not in self.nestedDict[iso3]["provinces"][province]["counties"]:
                    self.nestedDict[iso3]["provinces"][province]["counties"][fips] = {
                        "fips": fips,
                        # TODO remove "None" if it's ignored later
                        "county": county or None,
                        "latitude": latitude,
                        "longitude": longitude,
                        "iso2": iso2 or None,
                        "iso3": county_iso3 or None,
                        "code3": int(code3) or None,
                        "time_series": defaultdict(dict),
                    }

        # find the "time_series" dict we should add this data to
        tmp_dict = self.nestedDict[iso3]
        if province:
            tmp_dict = tmp_dict["provinces"][province]
            if fips:
                tmp_dict = tmp_dict["counties"][fips]
        time_series = tmp_dict["time_series"]

        dates_start = header_to_column["dates_start"]
        for i in range(dates_start, len(headers)):
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
            time_series[date][data_type] = val

    def nested_dict_to_geojson(self):
        """
        If time_series data is available for a country but we have more
        granular, province-level time_series data for this country, we
        keep both the aggregated country-level data and the non-aggregated
        province-level data. Same for province-level data when county-level
        data is available.
        """
        LATEST_DATE_ONLY = True
        features = []
        for country_data in self.nestedDict.values():

            feat_base = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        country_data["longitude"],
                        country_data["latitude"],
                    ],
                },
                "properties": {
                    "country_region": country_data["country_region"],
                    "iso2": country_data["iso2"],
                    "iso3": country_data["iso3"],
                },
            }

            # country-level time_series data
            for date, ts in country_data["time_series"].items():
                if LATEST_DATE_ONLY and date != self.latest_date:
                    continue
                feat_country = deepcopy(feat_base)
                feat_country["properties"]["date"] = date
                feat_country["properties"].update(ts)
                features.append(feat_country)

            for province_data in country_data.get("provinces", {}).values():
                # province-level time_series data
                # and update coordinates to the province's coordinates
                for date, ts in province_data["time_series"].items():
                    if LATEST_DATE_ONLY and date != self.latest_date:
                        continue
                    feat_prov = deepcopy(feat_base)
                    feat_prov["geometry"]["coordinates"] = [
                        province_data["longitude"],
                        province_data["latitude"],
                    ]
                    feat_prov["properties"]["province_state"] = province_data[
                        "province_state"
                    ]
                    feat_prov["properties"]["date"] = date
                    feat_prov["properties"].update(ts)
                    features.append(feat_prov)

                for county_data in province_data.get("counties", {}).values():
                    # county-level time_series data
                    # and update coordinates to the county's coordinates
                    # we don't overwrite the country's ISO2-3 with the county's
                    for date, ts in county_data["time_series"].items():
                        if LATEST_DATE_ONLY and date != self.latest_date:
                            continue
                        feat_county = deepcopy(feat_prov)
                        feat_county["geometry"]["coordinates"] = [
                            county_data["longitude"],
                            county_data["latitude"],
                        ]
                        feat_prov["properties"]["province_state"] = province_data[
                            "province_state"
                        ]
                        feat_county["properties"]["county"] = county_data["county"]
                        feat_county["properties"]["fips"] = county_data["fips"]
                        feat_county["properties"]["code3"] = county_data["code3"]
                        feat_county["properties"]["date"] = date
                        feat_county["properties"].update(ts)
                        features.append(feat_county)

        geojson = {"type": "FeatureCollection", "features": features}
        with open(os.path.join(CURRENT_DIR, GEOJSON_FILENAME), "w") as f:
            json.dump(geojson, f)

    def nested_dict_to_data_by_level(self):
        # TODO
        pass

    def submit_metadata(self):
        print("Uploading to S3...")
        s3 = boto3.resource("s3")
        s3.Bucket(self.s3_bucket).upload_file(
            os.path.join(CURRENT_DIR, GEOJSON_FILENAME),
            "map_data/{}".format(GEOJSON_FILENAME),
        )
        print("Done!")
