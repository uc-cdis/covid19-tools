import boto3
from contextlib import closing
from copy import deepcopy
from collections import defaultdict
import csv
from datetime import datetime
import json
import os
import pathlib
import re
import requests
import time

from etl import base
from utils.country_codes_utils import get_codes_dictionary, get_codes_for_country_name


"""
    First, we use the raw JHU CSV data to generate self.nested_dict. (1)
    It contains the data for all dates.

    Then, we use self.nested_dict to generate a GeoJson file. (2)
    It only contains the data for the latest available date.
    It's used to display the density map.

    Then, we use self.nested_dict to generate a JSON file. (3)
    It only contains the data for the latest available date.
    The data is organized by country, state and county.
    It's used to display the choropleth map.

    Finally, we use self.nested_dict to generate JSON files with
    all the dates, sorted by country, state or county. (4)
    They are used to display the time series plots.

    All these data files are pushed to S3.

    (1) self.nested_dict:
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
                                    "recovered": <optional>,
                                }
                            },
                        }
                    },
                    "time_series": {
                        <date>: {
                            "confirmed": 0,
                            "deaths": 0,
                            "recovered": <optional>,
                        }
                    },
                }
            },
            "time_series": {
                <date>: {
                    "confirmed": 0,
                    "deaths": 0,
                    "recovered": <optional>,
                }
            },
        }
    }

    (2) Density map GeoJson:
    {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [<longitude>, <latitude>],
                },
                "properties": {
                    "country_region": <country_name>,
                    "iso2": "",
                    "iso3": "",
                    "confirmed": 0,
                    "deaths": 0,
                    "recovered": <optional>,
                    "province_state": <US state name - optional>,
                    "county": <US county name - optional>,
                    "fips": <US county FIPS - optional>,
                    "code3": <for US counties - optional>,
                }
            },
            ...
        ]
    }

    (3) Choropleth map JSON:
    {
        # aggregated data for all countries
        "country": {
            <country ISO3>: {
                "confirmed": 0,
                "deaths": 0,
                "recovered": <optional>,
                "country_region": <country name>,
            },
            ...
        },
        # US only
        "state": {
            <US state name>: {
                "confirmed": 0,
                "deaths": 0,
                "recovered": <optional>,
                "country_region": <country name>,
                "province_state": <state name>,
            },
            ...
        },
        # US only
        "county": {
            <US county FIPS>: {
                "confirmed": 0,
                "deaths": 0,
                "recovered": <optional>,
                "country_region": <country name>,
                "province_state": <state name>,
                "county": <county name>,
            },
            ...
        },
    }

    (4) Times series data:
    In TIME_SERIES_DATA_FOLDER, the "country" folder contains one JSON file
    per country (<country ISO3 code>.json), the "state" folder one file per US
    state (<state name>.json) and the "county" folder one file per US county
    (<county FIPS>.json). Each JSON file is in the following format:
    {
        <date (YYYY-MM-DD)>: {
            confirmed: 0,
            "deaths": 0,
            "recovered": <optional>,
        },
        ...
    }
"""


MAP_DATA_FOLDER = "map_data"
GEOJSON_FILENAME = "jhu_geojson_latest.json"
JSON_BY_LEVEL_FILENAME = "jhu_json_by_level_latest.json"
TIME_SERIES_DATA_FOLDER = "time_series"
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


def replace_small_counts(data, data_level):
    # remove values smaller than the threshold
    count_replacement = f"<{MINIMUM_COUNT}"
    res = data.copy()
    for field in ["confirmed", "deaths", "recovered"]:
        if field in res and res[field] < MINIMUM_COUNT:
            res[field] = count_replacement

    # we don't have any "recovered" data for US and Canada
    # states/counties, and displaying "<5" looks bad: removing
    if res.get("country_region") in ["US", "Canada"] and data_level in [
        "state",
        "county",
    ]:
        del res["recovered"]

    return res


class JHU_TO_S3(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.nested_dict = {}
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
                    "Population",
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
        self.s3_client = boto3.client("s3")

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

        for file_type in ["global", "US_counties"]:
            for data_type, url in urls[file_type].items():
                self.parse_file_to_nested_dict(file_type, data_type, url)

        # create data folders
        data_folders = [
            os.path.join(CURRENT_DIR, MAP_DATA_FOLDER),
            os.path.join(CURRENT_DIR, TIME_SERIES_DATA_FOLDER),
            os.path.join(CURRENT_DIR, TIME_SERIES_DATA_FOLDER, "country"),
            os.path.join(CURRENT_DIR, TIME_SERIES_DATA_FOLDER, "state"),
            os.path.join(CURRENT_DIR, TIME_SERIES_DATA_FOLDER, "county"),
        ]
        for path in data_folders:
            pathlib.Path(path).mkdir(exist_ok=True)

        # generate data files
        self.nested_dict_to_geojson()
        self.nested_dict_to_data_by_level()
        self.nested_dict_to_time_series_by_level()

        print("Latest date: {}".format(self.latest_date))

    def parse_file_to_nested_dict(self, file_type, data_type, url):
        """
        Converts a CSV file to self.nested_dict in the format described on
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
        Converts a row of a CSV file to self.nested_dict in the format
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
        if not row:  # ignore empty rows
            return

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
        if iso3 not in self.nested_dict:
            # add this country if it's not already there
            self.nested_dict[iso3] = {
                "country_region": country,
                "latitude": latitude,
                "longitude": longitude,
                "iso2": codes["iso2"],
                "iso3": iso3,
                "provinces": {},
                "time_series": defaultdict(dict),
            }

        province = row[header_to_column["province"]]
        if not province:
            # the country may have been added using a province's coordinates.
            # when we find the country-level data, update the coordinates
            self.nested_dict[iso3]["latitude"] = latitude
            self.nested_dict[iso3]["longitude"] = longitude
        elif province not in self.nested_dict[iso3]["provinces"]:
            # add this province if it's not already there
            self.nested_dict[iso3]["provinces"][province] = {
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
                if (
                    fips
                    not in self.nested_dict[iso3]["provinces"][province]["counties"]
                ):
                    # add this county if it's not already there
                    self.nested_dict[iso3]["provinces"][province]["counties"][fips] = {
                        "fips": fips,
                        "county": county or None,
                        "latitude": latitude,
                        "longitude": longitude,
                        "iso2": iso2 or None,
                        "iso3": county_iso3 or None,
                        "code3": int(code3) or None,
                        "time_series": defaultdict(dict),
                    }

        # find the "time_series" dict we should add this data to
        tmp_dict = self.nested_dict[iso3]
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
        For example, Denmark has both country-level data and province-level
        data, but the latter does not contain the former, so we keep both.
        The exceptions are US and Canada, for which we do not keep the
        aggregated country-level data because it duplicates the province-level
        data.
        """
        print("Generating {}...".format(GEOJSON_FILENAME))
        LATEST_DATE_ONLY = True
        features = []
        for country_data in self.nested_dict.values():

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
                feat_country["properties"].update(replace_small_counts(ts, "country"))
                del feat_country["properties"]["date"]
                if country_data["country_region"] not in ["US", "Canada"]:
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
                    feat_prov["properties"].update(replace_small_counts(ts, "state"))
                    del feat_prov["properties"]["date"]
                    features.append(feat_prov)

                for county_data in province_data.get("counties", {}).values():
                    # county-level time_series data
                    # and update coordinates to the county's coordinates
                    # we don't overwrite the country's ISO2-3 with the county's
                    for date, ts in county_data["time_series"].items():
                        if LATEST_DATE_ONLY and date != self.latest_date:
                            continue
                        feat_county = deepcopy(feat_base)
                        feat_county["geometry"]["coordinates"] = [
                            county_data["longitude"],
                            county_data["latitude"],
                        ]
                        feat_county["properties"]["province_state"] = province_data[
                            "province_state"
                        ]
                        feat_county["properties"]["county"] = county_data["county"]
                        feat_county["properties"]["fips"] = county_data["fips"]
                        feat_county["properties"]["code3"] = county_data["code3"]
                        feat_county["properties"]["date"] = date
                        feat_county["properties"].update(
                            replace_small_counts(ts, "county")
                        )
                        del feat_county["properties"]["date"]
                        features.append(feat_county)

        geojson = {"type": "FeatureCollection", "features": features}
        with open(
            os.path.join(CURRENT_DIR, MAP_DATA_FOLDER, GEOJSON_FILENAME), "w"
        ) as f:
            json.dump(geojson, f)

    def nested_dict_to_data_by_level(self):
        """
        See `nested_dict_to_geojson` docstring for details on the aggregation.
        """
        print("Generating {}...".format(JSON_BY_LEVEL_FILENAME))
        LATEST_DATE_ONLY = True  # enabling this would break a bunch of things
        js = {
            "country": {},  # aggregated data for all countries
            "state": {},  # US only
            "county": {},  # US only
        }
        for country_data in self.nested_dict.values():
            iso3 = country_data["iso3"]
            if iso3 not in js["country"]:
                # add this country if it's not already there
                js["country"][iso3] = {
                    "confirmed": 0,
                    "deaths": 0,
                    "recovered": 0,
                    "country_region": country_data["country_region"],
                }

            # country-level time_series data
            for date, ts1 in country_data["time_series"].items():
                if LATEST_DATE_ONLY and date != self.latest_date:
                    continue
                if country_data["country_region"] not in ["US", "Canada"]:
                    js["country"][iso3]["confirmed"] += ts1["confirmed"]
                    js["country"][iso3]["deaths"] += ts1["deaths"]
                    js["country"][iso3]["recovered"] += ts1.get("recovered", 0)

            for province_data in country_data.get("provinces", {}).values():
                state_name = province_data["province_state"]
                if (
                    country_data["country_region"] == "US"
                    and state_name not in js["state"]
                ):
                    # add this US state if it's not already there
                    js["state"][state_name] = {
                        "confirmed": 0,
                        "deaths": 0,
                        "recovered": 0,
                        "country_region": country_data["country_region"],
                        "province_state": state_name,
                    }

                # add province-level time_series data for all countries + US states
                for date, ts in province_data["time_series"].items():
                    if LATEST_DATE_ONLY and date != self.latest_date:
                        continue
                    js["country"][iso3]["confirmed"] += ts["confirmed"]
                    js["country"][iso3]["deaths"] += ts["deaths"]
                    js["country"][iso3]["recovered"] += ts.get("recovered", 0)
                    if country_data["country_region"] == "US":
                        js["state"][state_name]["confirmed"] += ts["confirmed"]
                        js["state"][state_name]["deaths"] += ts["deaths"]
                        js["state"][state_name]["recovered"] += ts.get("recovered", 0)

                for county_data in province_data.get("counties", {}).values():
                    county_fips = county_data["fips"]
                    # add county-level time_series data for all countries + US states + US counties
                    for date, ts in county_data["time_series"].items():
                        if LATEST_DATE_ONLY and date != self.latest_date:
                            continue
                        js["country"][iso3]["confirmed"] += ts["confirmed"]
                        js["country"][iso3]["deaths"] += ts["deaths"]
                        js["country"][iso3]["recovered"] += ts.get("recovered", 0)
                        if country_data["country_region"] == "US":
                            js["state"][state_name]["confirmed"] += ts["confirmed"]
                            js["state"][state_name]["deaths"] += ts["deaths"]
                            js["state"][state_name]["recovered"] += ts.get(
                                "recovered", 0
                            )

                            # add this US county. it shouldn't already be there
                            js["county"][county_fips] = {
                                "confirmed": ts["confirmed"],
                                "deaths": ts["deaths"],
                                "recovered": ts.get("recovered", 0),
                                "country_region": country_data["country_region"],
                                "province_state": state_name,
                                "county": county_data["county"],
                            }

            # (countries) if the original count is greater than the
            # aggregated count we calculated, use the original count
            for key in ["confirmed", "deaths", "recovered"]:
                original_count = ts1.get(key, 0)
                aggregated_count = js["country"][iso3][key]
                if original_count > aggregated_count:
                    print(
                        "  Country {}: Using global {} count ({}) rather than smaller aggregated count ({})".format(
                            country_data["country_region"],
                            key,
                            original_count,
                            aggregated_count,
                        )
                    )
                    js["country"][iso3][key] = original_count

        # remove values smaller than the threshold
        for data_level, locations in js.items():
            for location_id, data in locations.items():
                js[data_level][location_id] = replace_small_counts(data, data_level)

        with open(
            os.path.join(CURRENT_DIR, MAP_DATA_FOLDER, JSON_BY_LEVEL_FILENAME), "w"
        ) as f:
            json.dump(js, f)

    def nested_dict_to_time_series_by_level(self):
        print("Generating time series files...")
        tmp = {"country": {}, "state": {}, "county": {}}
        for country_data in self.nested_dict.values():
            iso3 = country_data["iso3"]
            if iso3 not in tmp["country"]:
                # add this country if it's not already there
                tmp["country"][iso3] = defaultdict(
                    lambda: {"confirmed": 0, "deaths": 0, "recovered": 0}
                )

            # country-level time_series data
            for date, ts in country_data["time_series"].items():
                if country_data["country_region"] not in ["US", "Canada"]:
                    tmp["country"][iso3][date]["confirmed"] += ts["confirmed"]
                    tmp["country"][iso3][date]["deaths"] += ts["deaths"]
                    tmp["country"][iso3][date]["recovered"] += ts.get("recovered", 0)

            for province_data in country_data.get("provinces", {}).values():
                state_name = province_data["province_state"]
                if (
                    country_data["country_region"] == "US"
                    and state_name not in tmp["state"]
                ):
                    # add this US state if it's not already there
                    tmp["state"][state_name] = defaultdict(
                        lambda: {"confirmed": 0, "deaths": 0, "recovered": 0}
                    )

                # add province-level time_series data for all countries + US states
                for date, ts in province_data["time_series"].items():
                    tmp["country"][iso3][date]["confirmed"] += ts["confirmed"]
                    tmp["country"][iso3][date]["deaths"] += ts["deaths"]
                    tmp["country"][iso3][date]["recovered"] += ts.get("recovered", 0)
                    if country_data["country_region"] == "US":
                        tmp["state"][state_name][date]["confirmed"] += ts["confirmed"]
                        tmp["state"][state_name][date]["deaths"] += ts["deaths"]
                        tmp["state"][state_name][date]["recovered"] += ts.get(
                            "recovered", 0
                        )

                for county_data in province_data.get("counties", {}).values():
                    county_fips = county_data["fips"]
                    if country_data["country_region"] == "US":
                        # add this US county. it shouldn't already be there
                        assert county_fips not in tmp["county"]
                        tmp["county"][county_fips] = {}

                    # add county-level time_series data for all countries + US states + US counties
                    for date, ts in county_data["time_series"].items():
                        tmp["country"][iso3][date]["confirmed"] += ts["confirmed"]
                        tmp["country"][iso3][date]["deaths"] += ts["deaths"]
                        tmp["country"][iso3][date]["recovered"] += ts.get(
                            "recovered", 0
                        )
                        if country_data["country_region"] == "US":
                            tmp["state"][state_name][date]["confirmed"] += ts[
                                "confirmed"
                            ]
                            tmp["state"][state_name][date]["deaths"] += ts["deaths"]
                            tmp["state"][state_name][date]["recovered"] += ts.get(
                                "recovered", 0
                            )

                            tmp["county"][county_fips][date] = {
                                "confirmed": ts["confirmed"],
                                "deaths": ts["deaths"],
                                "recovered": ts.get("recovered", 0),
                            }

            # (countries) if the original count is greater than the
            # aggregated count we calculated, use the original count
            for date in tmp["country"][iso3]:
                for key in ["confirmed", "deaths", "recovered"]:
                    original_count = country_data["time_series"][date].get(key, 0)
                    aggregated_count = tmp["country"][iso3][date][key]
                    if original_count > aggregated_count:
                        tmp["country"][iso3][date][key] = original_count

        # save as JSON files, and upload to S3
        print("Uploading time series files to S3...")
        start = time.time()

        for data_level in ["country", "state", "county"]:
            print("  Uploading {} files".format(data_level.capitalize()))
            for location_id, data_by_date in tmp[data_level].items():
                # remove values smaller than the threshold
                for date, data in data_by_date.items():
                    data_by_date[date] = replace_small_counts(data, data_level)

                file_name = "{}.json".format(location_id)
                abs_path = os.path.join(
                    CURRENT_DIR, TIME_SERIES_DATA_FOLDER, data_level, file_name
                )

                # write to local file, upload to S3, delete local file
                with open(abs_path, "w") as f:
                    json.dump(data_by_date, f)
                s3_path = os.path.relpath(abs_path, CURRENT_DIR)
                self.s3_client.upload_file(abs_path, self.s3_bucket, s3_path)
                os.remove(abs_path)
        print("  Done in {} secs".format(int(time.time() - start)))

    def submit_metadata(self):
        print("Uploading other files to S3...")
        start = time.time()

        # files in TIME_SERIES_DATA_FOLDER have already been uploaded to S3
        for folder in [MAP_DATA_FOLDER]:
            for abs_path, _, files in os.walk(os.path.join(CURRENT_DIR, folder)):
                for file_name in files:
                    local_path = os.path.join(abs_path, file_name)
                    s3_path = os.path.relpath(local_path, CURRENT_DIR)
                    self.s3_client.upload_file(local_path, self.s3_bucket, s3_path)
                    os.remove(local_path)

        print("  Done in {} secs".format(int(time.time() - start)))
        print("Done!")
