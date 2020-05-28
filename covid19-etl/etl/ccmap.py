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
    submitter_id = "summary_location_{}".format(country)
    if "province_state" in in_json:
        province = in_json["province_state"]
        submitter_id += "_{}".format(province)
    if "county" in in_json:
        county = in_json["county"]
        submitter_id += "_{}".format(county)

    submitter_id = submitter_id.lower().replace(", ", "_")
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)
    return submitter_id.strip("-")


def format_summary_report_submitter_id(location_submitter_id, date):
    return "{}_{}".format(
        location_submitter_id.replace("summary_location_", "summary_report_"), date
    )


class CCMAP(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.summary_locations = []
        self.summary_reports = []

        self.program_name = "open"
        self.project_code = "CCMap"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        county_fields = [
            ("fips_code", ("summary_location", "FIPS")),
            ("State", ("summary_location", "province_state")),
            ("County Name", ("summary_location", "county")),
            ("Staffed All Beds", ("summary_report", "staffed_all_beds")),
            ("Staffed ICU Beds", ("summary_report", "staffed_icu_beds")),
            ("Licensed All Beds", ("summary_report", "licensed_all_beds")),
            ("All Bed Occupancy Rate", ("summary_report", "all_bed_occupancy_rate")),
            ("ICU Bed Occupancy Rate", ("summary_report", "icu_bed_occupancy_rate")),
            ("Population", ("summary_report", "population")),
            ("Population (20+)", ("summary_report", "population_gtr_20")),
            ("Population (65+)", ("summary_report", "population_gtr_65")),
            ("Staffed All Beds [Per 1000 People]", ("summary_report", "staffed_all_beds_per_1000")),
            ("Staffed All Beds [Per 1000 Adults (20+)]", ("summary_report", "staffed_all_beds_per_1000_gtr_20")),
            ("Staffed All Beds [Per 1000 Elderly (65+)]", ("summary_report", "staffed_all_beds_per_1000_gtr_65")),
            ("Staffed ICU Beds [Per 1000 People]", ("summary_report", "staffed_icu_beds_per_1000")),
            ("Staffed ICU Beds [Per 1000 Adults (20+)]", ("summary_report", "staffed_icu_beds_per_1000_gtr_20")),
            ("Staffed ICU Beds [Per 1000 Elderly (65+)]", ("summary_report", "staffed_icu_beds_per_1000_gtr_65")),
            ("Licensed All Beds [Per 1000 People]", ("summary_report", "licensed_all_beds_per_1000")),
            ("Licensed All Beds [Per 1000 Adults (20+)]", ("summary_report", "licensed_all_beds_per_1000_gtr_20")),
            ("Licensed All Beds [Per 1000 Elderly (65+)]", ("summary_report", "licensed_all_beds_per_1000_gtr_65")),
        ]

        state_fields = [
            ("State", ("summary_location", None)),
            ("State Name", ("summary_location", "province_state")),
            ("Staffed All Beds", ("summary_report", "staffed_all_beds")),
            ("Staffed ICU Beds", ("summary_report", "staffed_icu_beds")),
            ("Licensed All Beds", ("summary_report", "licensed_all_beds")),
            ("All Bed Occupancy Rate", ("summary_report", "all_bed_occupancy_rate")),
            ("ICU Bed Occupancy Rate", ("summary_report", "icu_bed_occupancy_rate")),
            ("Population", ("summary_report", "population")),
            ("Population (20+)", ("summary_report", "population_gtr_20")),
            ("Population (65+)", ("summary_report", "population_gtr_65")),
            ("Staffed All Beds [Per 1000 People]", ("summary_report", "staffed_all_beds_per_1000")),
            ("Staffed All Beds [Per 1000 Adults (20+)]", ("summary_report", "staffed_all_beds_per_1000_gtr_20")),
            ("Staffed All Beds [Per 1000 Elderly (65+)]", ("summary_report", "staffed_all_beds_per_1000_gtr_65")),
            ("Staffed ICU Beds [Per 1000 People]", ("summary_report", "staffed_icu_beds_per_1000")),
            ("Staffed ICU Beds [Per 1000 Adults (20+)]", ("summary_report", "staffed_icu_beds_per_1000_gtr_20")),
            ("Staffed ICU Beds [Per 1000 Elderly (65+)]", ("summary_report", "staffed_icu_beds_per_1000_gtr_65")),
            ("Licensed All Beds [Per 1000 People]", ("summary_report", "licensed_all_beds_per_1000")),
            ("Licensed All Beds [Per 1000 Adults (20+)]", ("summary_report", "licensed_all_beds_per_1000_gtr_20")),
            ("Licensed All Beds [Per 1000 Elderly (65+)]", ("summary_report", "licensed_all_beds_per_1000_gtr_65")),
            ("Estimated No. Full-Featured Mechanical Ventilators (2010 study estimate)", ("summary_report", "estimated_full_mech_ventilators")),
            ("Estimated No. Full-Featured Mechanical Ventilators per 100,000 Population (2010 study estimate)", ("summary_report", "estimated_full_mech_ventilators_per_100000")),
            ("Estimated No. Pediatrics-Capable Full-Feature Mechanical Ventilators (2010 study estimate)", ("summary_report", "estimated_full_mech_pediatric_ventilators")),
            ("Estimated No. Full-Feature Mechanical Ventilators, Pediatrics Capable per 100,000 Population <14 y (2010 study estimate)", ("summary_report", "estimated_full_mech_pediatric_ventilators_per_100000")),
        ]

        self.headers_mapping = {
            "county": {field: (k, mapping) for k, (field, mapping) in enumerate(county_fields)},
            "state": {field: (k, mapping) for k, (field, mapping) in enumerate(state_fields)},
        }

    def files_to_submissions(self):
        """
        Reads CSV files and converts the data to Sheepdog records
        """
        urls = {
            "county": "https://raw.githubusercontent.com/covidcaremap/covid19-healthsystemcapacity/master/data/published/us_healthcare_capacity-county-CovidCareMap.csv",
            "state": "https://raw.githubusercontent.com/covidcaremap/covid19-healthsystemcapacity/master/data/published/us_healthcare_capacity-state-CovidCareMap.csv",
        }

        for k, url in urls.items():
            self.parse_file(url, csv_type=k)

    def parse_file(self, url, csv_type):
        print("Getting data from {}".format(url))
        with closing(requests.get(url, stream=True)) as r:
            f = (line.decode("utf-8") for line in r.iter_lines())
            reader = csv.reader(f, delimiter=",", quotechar='"')

            headers = next(reader)

            if headers[0] == "404: Not Found":
                print("  Unable to get file contents, received {}.".format(headers))
                return

            expected_h = list(self.headers_mapping[csv_type].keys())
            obtained_h = headers[: len(expected_h)]
            assert (
                    obtained_h == expected_h
            ), "CSV headers have changed (expected {}, got {}). We may need to update the ETL code".format(
                expected_h, obtained_h
            )

            for row in reader:
                summary_location, summary_report = self.parse_row(row, self.headers_mapping[csv_type])

                print(summary_location)
                print(summary_report)

                self.summary_locations.append(summary_location)
                self.summary_reports.append(summary_report)

                break

    def parse_row(self, row, mapping):
        summary_location = {"country_region": "US"}
        summary_report = {}

        for k, (i, (node_type, node_field)) in mapping.items():
            if node_field:
                if node_type == "summary_location":
                    summary_location[node_field] = row[i]
                if node_type == "summary_report":
                    value = row[i]
                    if value:
                        summary_report[node_field] = float(value)

        summary_location_submitter_id = format_location_submitter_id(summary_location)

        summary_location["submitter_id"] = summary_location_submitter_id
        summary_report["submitter_id"] = format_summary_report_submitter_id(summary_location_submitter_id, date=datetime.date.today().strftime("%Y-%m-%d"))
        summary_report["summary_locations"] = [{"submitter_id": summary_location_submitter_id}]

        return summary_location, summary_report

    def submit_metadata(self):
        """
        Converts the data in `self.time_series_data` to Sheepdog records.
        `self.location_data already contains Sheepdog records. Batch submits
        all records in `self.location_data` and `self.time_series_data`
        """

        # Commented
        # Only required for one time submission of summary_location
        print("Submitting summary_location data")
        # for loc in self.summary_locations:
        #     loc_record = {"type": "summary_location"}
        #     loc_record.update(loc)
        #     self.metadata_helper.add_record_to_submit(loc_record)
        # self.metadata_helper.batch_submit_records()

        # print("Submitting summary_report data")
        for rep in self.summary_reports:
            rep_record = {"type": "summary_report"}
            rep_record.update(rep)
            self.metadata_helper.add_record_to_submit(rep_record)
        self.metadata_helper.batch_submit_records()
