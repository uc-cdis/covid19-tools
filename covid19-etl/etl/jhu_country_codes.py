# This only needs to be run once, when new locations are submitted.
# The country data was obtained from https://datahub.io/core/country-codes.

from etl import base
from utils.metadata_helper import MetadataHelper
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

            # do not update the record if it already has the codes
            if location["iso2"] == codes["iso2"] and location["iso3"] == codes["iso3"]:
                continue

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
        query_string = (
            '{ summary_location (first: 0, project_id: "'
            + self.program_name
            + "-"
            + self.project_code
            + '") { submitter_id, country_region, iso2, iso3 } }'
        )
        query_res = self.metadata_helper.query_peregrine(query_string)
        return [location for location in query_res["data"]["summary_location"]]
