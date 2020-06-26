# This only needs to be run once, when new locations are submitted.
# The country data was obtained from https://datahub.io/core/country-codes.

import json
import requests

from etl import base
from helper.metadata_helper import MetadataHelper
from utils.country_codes_utils import get_codes_dictionary, get_codes_for_country_name


class JHU_COUNTRY_CODES(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.program_name = "open"
        self.project_code = "JHU"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

    def files_to_submissions(self):
        codes_dict = get_codes_dictionary()
        locations = self.get_existing_locations()
        for location in locations:
            codes = get_codes_for_country_name(codes_dict, location["country_region"])
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

    def get_existing_locations(self):
        print("Getting summary_location data from Peregrine")
        headers = {"Authorization": "bearer " + self.access_token}
        query_string = (
            '{ summary_location (first: 0, project_id: "'
            + self.program_name
            + "-"
            + self.project_code
            + '") { submitter_id, country_region, province_state } }'
        )
        response = requests.post(
            "{}/api/v0/submission/graphql".format(self.base_url),
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
