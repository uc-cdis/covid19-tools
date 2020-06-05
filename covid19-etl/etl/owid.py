import csv
from contextlib import closing
import datetime
import re

import requests

from etl import base
from helper.metadata_helper import MetadataHelper


def format_location_submitter_id(in_json):
    """summary_location_<country>_<province>_<county>"""
    country = in_json["country_region"]
    submitter_id = "summary_location_owid_{}".format(country)
    if "province_state" in in_json:
        province = in_json["province_state"]
        submitter_id += "_{}".format(province)
    if "county" in in_json:
        county = in_json["county"]
        submitter_id += "_{}".format(county)
    if "FIPS" in in_json:
        fips = in_json["FIPS"]
        submitter_id += "_{}".format(fips)

    submitter_id = submitter_id.lower().replace(", ", "_")
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)
    return submitter_id.strip("-")


def format_summary_clinical_submitter_id(location_submitter_id, test_type, date):
    return "{}_{}_{}".format(
        location_submitter_id.replace("summary_location_", "summary_clinical_"),
        test_type,
        date,
    )


def split_entity(entity):
    splitted = entity.split(" - ")
    assert len(splitted) == 2, "incorrect Entity structure"
    return splitted[0], splitted[1]


class OWID(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.summary_locations = []
        self.summary_clinicals = []

        self.program_name = "open"
        self.project_code = "OWID"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        # structure is
        # (csv field name, (node type, node field name, type of field))
        testing_fields = [
            ("ISO code", ("summary_location", "iso3", str)),
            ("Entity", (None, None, split_entity)),
            ("Date", ("summary_clinical", "date", str)),
            ("Source URL", ("summary_clinical", "source_url", str)),
            ("Source label", ("summary_clinical", "source_label", str)),
            ("Notes", ("summary_clinical", "notes", str)),
            ("Number of observations", ("summary_clinical", "num_observations", int)),
            ("Cumulative total", ("summary_clinical", "cumulative_total", int)),
            ("Cumulative total per thousand", ("summary_clinical", "cumulative_total_per_thousand", int)),
            (
                "Daily change in cumulative total",
                ("summary_clinical", "daily_change_in_cumulative_total", int),
            ),
            ("Daily change in cumulative total per thousand", ("summary_clinical", "daily_change_in_cumulative_total_per_thousand", int)),
            ("7-day smoothed daily change", ("summary_clinical", "seven_day_smoothed_daily_change", int)),
            ("7-day smoothed daily change per thousand", ("summary_clinical", "seven_day_smoothed_daily_change_per_thousand", float)),
            ("General source label", ("summary_clinical", "general_source_label", str)),
            ("General source URL", ("summary_clinical", "general_source_url", str)),
            ("Short description", ("summary_clinical", "short_description", str)),
            ("Detailed description", ("summary_clinical", "detailed_description", str)),
        ]

        self.headers_mapping = {
            field: (k, mapping) for k, (field, mapping) in enumerate(testing_fields)
        }

    def files_to_submissions(self):
        """
        Reads CSV files and converts the data to Sheepdog records
        """
        url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/testing/covid-testing-latest-data-source-details.csv"
        self.parse_file(url)

    def parse_file(self, url):
        print("Getting data from {}".format(url))
        with closing(requests.get(url, stream=True)) as r:
            f = (line.decode("utf-8") for line in r.iter_lines())
            reader = csv.reader(f, delimiter=",", quotechar='"')

            headers = next(reader)

            assert (
                headers[0] != "404: Not Found"
            ), "  Unable to get file contents, received {}.".format(headers)

            expected_h = list(self.headers_mapping.keys())
            obtained_h = headers[: len(expected_h)]
            assert (
                obtained_h == expected_h
            ), "CSV headers have changed (expected {}, got {}). We may need to update the ETL code".format(
                expected_h, obtained_h
            )

            for row in reader:
                summary_location, summary_clinical = self.parse_row(
                    row, self.headers_mapping
                )

                if summary_location not in self.summary_locations:
                    self.summary_locations.append(summary_location)
                self.summary_clinicals.append(summary_clinical)

    def parse_row(self, row, mapping):
        summary_location = {}
        summary_clinical = {}

        for k, (i, (node_type, node_field, type_conv)) in mapping.items():
            if k == "Entity":
                country, test_type = split_entity(row[i])
                summary_location["country_region"] = country
                summary_clinical["test_type"] = test_type
            if node_field:
                value = row[i]
                if value:
                    if node_type == "summary_location":
                        summary_location[node_field] = type_conv(value)
                    if node_type == "summary_clinical":
                        if type_conv == int:
                            summary_clinical[node_field] = type_conv(float(value))
                        else:
                            summary_clinical[node_field] = type_conv(value)

        summary_location_submitter_id = format_location_submitter_id(summary_location)

        summary_location["submitter_id"] = summary_location_submitter_id
        summary_location["projects"] = [{"code": self.project_code}]

        summary_clinical["submitter_id"] = format_summary_clinical_submitter_id(
            summary_location_submitter_id,
            test_type=summary_clinical["test_type"],
            date=datetime.date.today().strftime("%Y-%m-%d"),
        )
        summary_clinical["summary_locations"] = [
            {"submitter_id": summary_location_submitter_id}
        ]

        return summary_location, summary_clinical

    def submit_metadata(self):
        print("Submitting summary_location data")
        for loc in self.summary_locations:
            loc_record = {"type": "summary_location"}
            loc_record.update(loc)
            self.metadata_helper.add_record_to_submit(loc_record)
        self.metadata_helper.batch_submit_records()

        print("Submitting summary_clinical data")
        for rep in self.summary_clinicals:
            rep_record = {"type": "summary_clinical"}
            rep_record.update(rep)
            self.metadata_helper.add_record_to_submit(rep_record)
        self.metadata_helper.batch_submit_records()
