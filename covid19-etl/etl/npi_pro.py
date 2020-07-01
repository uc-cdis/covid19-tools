import os
import tempfile

import fiona
import geopandas as gpd
import requests

from etl import base
from helper.format_helper import (
    derived_submitter_id,
    format_submitter_id,
)
from helper.metadata_helper import MetadataHelper

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


class NPI_PRO(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "NPI-PRO"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.country = "US"

        self.summary_locations = []
        self.summary_clinicals = []

    def download_dataset(self, url):
        r = requests.get(url, allow_redirects=True)
        tf = tempfile.NamedTemporaryFile(suffix=".gdb.zip", delete=False)
        with open(tf.name, "wb") as npi_pro_geodatabase:
            npi_pro_geodatabase.write(r.content)

        return tf.name

    def files_to_submissions(self):
        print("Getting geodatabase for NPI-PRO dataset...")
        url = "https://www.arcgis.com/sharing/rest/content/items/7e80baf1773e4fd9b44fe9fb054677db/data"
        tf = self.download_dataset(url)
        self.parse_file(file_path=tf)

    def parse_file(self, file_path):
        # get all the layers from the .gdb file
        layers = fiona.listlayers(file_path)
        # and check that the layer is the one we expect
        assert layers == [
            "NPI202003_TaxonomyGroups"
        ], "no required layer, is it a wrong file? got layers {}".format(layers)
        gdf = gpd.read_file(file_path, layer="NPI202003_TaxonomyGroups")

        print("Until better solution, submit only Illinois data")
        il_only = gdf.loc[gdf["Provider_Business_Practice_ST"] == "IL"]

        for i, row in il_only.iterrows():
            summary_location, summary_clinical = self.parse_row(row)
            self.summary_locations.append(summary_location)
            self.summary_clinicals.append(summary_clinical)

    def parse_row(self, row):
        fields_mapping = {
            "NPI": ("summary_location", "npi"),
            "Provider_First_Line_Business_Pra": (
                "summary_location",
                "provider_first_line_business_pr",
            ),
            "Provider_Second_Line_Business_Pr": (
                "summary_location",
                "provider_second_line_business_pr",
            ),
            "Provider_Business_Practice_City": (
                "summary_location",
                "provider_business_practice_city",
            ),
            "Provider_Business_Practice_ST": [
                ("summary_location", "provider_business_practice_st",),
                ("summary_location", "state",),
            ],
            "TaxonomyCode": ("summary_clinical", "taxonomy_code"),
            "ProviderType": ("summary_clinical", "provider_type"),
            "ProviderSubtype": ("summary_clinical", "provider_subtype"),
            "DetailedSpecialty": ("summary_clinical", "detailed_specialty"),
        }

        npi = row["NPI"]
        state = row["Provider_Business_Practice_ST"]

        summary_location_submitter_id = format_submitter_id(
            "summary_location", {"country": self.country, "state": state, "npi": npi},
        )

        summary_clinical_submitter_id = derived_submitter_id(
            summary_location_submitter_id, "summary_location", "summary_clinical", {}
        )

        result = {
            "summary_location": {},
            "summary_clinical": {},
        }

        for original_field, mappings in fields_mapping.items():
            if isinstance(mappings, list):
                for mapping in mappings:
                    node, node_field = mapping
                    result[node][node_field] = row[original_field]
            else:
                node, node_field = mappings
                result[node][node_field] = row[original_field]

        result["summary_location"].update(
            {
                "submitter_id": summary_location_submitter_id,
                "projects": [{"code": self.project_code}],
            }
        )

        result["summary_clinical"].update(
            {
                "submitter_id": summary_clinical_submitter_id,
                "summary_locations": [{"submitter_id": summary_location_submitter_id}],
            }
        )

        return result["summary_location"], result["summary_clinical"]

    def submit_metadata(self):
        print("Submitting data...")
        print("Submitting summary_location data")
        for sl in self.summary_locations:
            sl_record = {"type": "summary_location"}
            sl_record.update(sl)
            self.metadata_helper.add_record_to_submit(sl_record)
        self.metadata_helper.batch_submit_records()

        print("Submitting summary_clinical data")
        for sc in self.summary_clinicals:
            sc_record = {"type": "summary_clinical"}
            sc_record.update(sc)
            self.metadata_helper.add_record_to_submit(sc_record)
        self.metadata_helper.batch_submit_records()
