# This only needs to be run once, when new locations are submitted.
# The country data was obtained from https://datahub.io/core/country-codes.

import csv
import json
import os
import requests

from etl import base
from helper.metadata_helper import MetadataHelper


BASE_URL = "https://covid19.datacommons.io"
PROGRAM_NAME = "open"
PROJECT_CODE = "JHU"
COUNTRY_NAME_MAPPING = {
    # <name in summary_location>: <name in CSV data file>
    "Bosnia and Herzegovina": "Bosnia",
    "Burma": "Myanmar",
    "Cabo Verde": "Cape Verde",
    "Congo (Brazzaville)": "Congo - Brazzaville",
    "Congo (Kinshasa)": "Congo - Kinshasa",
    "Cote d'Ivoire": "Côte d’Ivoire",
    "Eswatini": "Swaziland",
    "Holy See": "Vatican City",
    "Korea, South": "South Korea",
    "North Macedonia": "Macedonia",
    "Saint Vincent and the Grenadines": "St. Vincent & Grenadines",
    "Sao Tome and Principe": "São Tomé & Príncipe",
    "United Kingdom": "UK",
}
ISO_CODES_MAPPING = {
    # ISO codes for countries that are not in the CSV file
    "Kosovo": {"iso2": "XK", "iso3": "XKX"},
    "West Bank and Gaza": {"iso2": "PS", "iso3": "PSE"},
}

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


class JHU_COUNTRY_CODES(base.BaseETL):
    def __init__(self, base_url, access_token):
        super().__init__(base_url, access_token)
        self.program_name = "open"
        self.project_code = "JHU"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

    def files_to_submissions(self):
        codes_dict = self.get_codes_dictionary()
        locations = self.get_existing_locations()
        for location in locations:
            codes = self.get_codes_for_country_name(
                codes_dict, location["country_region"]
            )
            record = {k: v for k, v in location.items() if v != None}
            record.update(
                {
                    "type": "summary_location",
                    "projects": [{"code": self.project_code}],
                    "iso2": codes["iso2"],
                    "iso3": codes["iso3"],
                }
            )
            self.metadata_helper.add_record_to_submit(record)

    def submit_metadata(self):
        self.metadata_helper.batch_submit_records()

    def get_codes_dictionary(self):
        with open(os.path.join(CURRENT_DIR, "country_codes.csv")) as f:
            reader = csv.reader(f, delimiter=",", quotechar='"')
            headers = next(reader)
            i_name = headers.index("CLDR display name")
            i_iso2 = headers.index("ISO3166-1-Alpha-2")
            i_iso3 = headers.index("ISO3166-1-Alpha-3")

            res = {
                row[i_name]: {"iso2": row[i_iso2], "iso3": row[i_iso3]}
                for row in reader
            }
        return res

    def get_existing_locations(self):
        print("Getting summary_location data from Peregrine")
        headers = {"Authorization": "bearer " + self.access_token}
        query_string = (
            '{ summary_location (first: 0, project_id: "'
            + PROGRAM_NAME
            + "-"
            + PROJECT_CODE
            + '") { submitter_id, country_region, province_state } }'
        )
        response = requests.post(
            "{}/api/v0/submission/graphql".format(BASE_URL),
            json={"query": query_string, "variables": None},
            headers=headers,
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
        return [location for location in query_res["data"]["summary_location"]]

    def get_codes_for_country_name(self, codes_dict, country_name):
        stripped_name = (
            country_name.strip("*").replace("Saint", "St.").replace(" and ", " & ")
        )
        data = codes_dict.get(stripped_name)
        if data:
            return data

        mapped_name = COUNTRY_NAME_MAPPING.get(country_name)
        data = codes_dict.get(mapped_name)
        if data:
            return data

        data = ISO_CODES_MAPPING.get(country_name)
        if data:
            return data

        raise Exception('Cannot find ISO codes data for "{}"'.format(country_name))
