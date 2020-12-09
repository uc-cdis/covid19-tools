import os
import asyncio
import gzip
import time
import re
import requests
import csv
from contextlib import closing
from datetime import datetime
from functools import partial


from etl import base
from utils.async_file_helper import AsyncFileHelper
from utils.format_helper import format_submitter_id
from utils.metadata_helper import MetadataHelper

# The files need to be handled so that they are compatible
# to gen3 fields
SPECIAL_MAP_FIELDS = {"country_region_code": "iso2"}


def format_location_submitter_id(*argv):
    """summary_location_<country>_<province>_<county>"""
    submitter_id = "summary_location"
    for v in argv:
        submitter_id = submitter_id + f"_{v}"
    submitter_id = submitter_id.lower().replace(", ", "_")
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)
    return submitter_id.strip("-")


class MOBILITY(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "ncbi-covid-19"

        self.file_helper = AsyncFileHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.expected_file_headers = [
            "country_region_code",
            "country_region",
            "sub_region_1",
            "sub_region_2",
            "metro_area",
            "iso_3166_2_code",
            "census_fips_code",
            "date",
            "retail_and_recreation_percent_change_from_baseline",
            "grocery_and_pharmacy_percent_change_from_baseline",
            "parks_percent_change_from_baseline",
            "transit_stations_percent_change_from_baseline",
            "workplaces_percent_change_from_baseline",
            "residential_percent_change_from_baseline",
        ]

        self.submitting_data = {
            "summary_location": [],
            "sample": [],
        }

    def files_to_submissions(self):
        """
        Reads CSV files and converts the data to Sheepdog records
        """
        url = "https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"
        self.parse_file(url)

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

            assert (
                headers[0] != "404: Not Found"
            ), "Unable to get file contents, received {}.".format(headers)

            assert set(self.expected_file_headers).issubset(
                set(headers)
            ), "CSV headers have changed (expected {} is a subset of {}). We may need to update the ETL code".format(
                self.expected_file_headers, headers
            )

            import pdb

            pdb.set_trace()
            for row in reader:

                row_dict = dict(zip(headers, row))
                summary_location = {}
                summary_location_submitter_id = format_location_submitter_id(
                    row_dict["country_region_code"],
                    row_dict["sub_region_1"],
                    row_dict["sub_region_2"],
                )
                summary_location = {
                    "submitter_id": summary_location_submitter_id,
                    "projects": [{"code": self.project_code}],
                }

                for field in [
                    "country_region_code",
                    "country_region",
                    "sub_region_1",
                    "sub_region_2",
                    "metro_area",
                    "iso_3166_2_code",
                    "census_fips_code",
                    "date",
                ]:
                    gen3_field = (
                        SPECIAL_MAP_FIELDS[field]
                        if field in SPECIAL_MAP_FIELDS
                        else field
                    )

                    summary_location[gen3_field] = row_dict[field]

                summary_socio_demographic = {}

                for field in [
                    "retail_and_recreation_percent_change_from_baseline",
                    "grocery_and_pharmacy_percent_change_from_baseline",
                    "parks_percent_change_from_baseline",
                    "transit_stations_percent_change_from_baseline",
                    "workplaces_percent_change_from_baseline",
                    "residential_percent_change_from_baseline",
                ]:
                    summary_socio_demographic[field] = row_dict[field]

                # summary_location_submitter_id = summary_location["submitter_id"]
                # if summary_location_submitter_id not in summary_location_list:
                #     self.summary_locations.append(summary_location)
                #     summary_location_list.append(summary_location_submitter_id)

                # self.summary_clinicals.append(summary_clinical)

    def submit_metadata(self):

        # start = time.strftime("%X")
        # loop = asyncio.get_event_loop()
        # tasks = []

        # for node_name, _ in self.data_file.nodes.items():
        #     if node_name == "virus_sequence_run_taxonomy":
        #         continue
        #     else:
        #         tasks.append(
        #             asyncio.ensure_future(self.files_to_node_submissions(node_name))
        #         )

        # try:
        #     results = loop.run_until_complete(asyncio.gather(*tasks))
        #     loop.run_until_complete(
        #         asyncio.gather(
        #             self.files_to_virus_sequence_run_taxonomy_submission(results[0])
        #         )
        #     )
        #     loop.run_until_complete(asyncio.gather(AsyncFileHelper.close_session()))
        # finally:
        #     loop.close()
        # end = time.strftime("%X")

        # for k, v in self.submitting_data.items():
        #     print(f"Submitting {k} data...")
        #     for node in v:
        #         node_record = {"type": k}
        #         node_record.update(node)
        #         self.metadata_helper.add_record_to_submit(node_record)
        #     self.metadata_helper.batch_submit_records()

        # print(f"Running time: From {start} to {end}")

        self.submitting_data["virus_sequence"].append(virus_sequence)
        self.submitting_data["summary_location"].append(summary_location)
        self.submitting_data["sample"].append(sample)
