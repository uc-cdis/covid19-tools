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
    submitter_id = "summary_location_ccmap_{}".format(country)
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

        # structure is
        # (csv field name, (node type, node field name, type of field))
        county_fields = [
            ("fips_code", ("summary_location", "FIPS", int)),
            ("State", ("summary_location", "province_state", str)),
            ("County Name", ("summary_location", "county", str)),
            ("Staffed All Beds", ("summary_report", "staffed_all_beds", int)),
            ("Staffed ICU Beds", ("summary_report", "staffed_icu_beds", int)),
            ("Licensed All Beds", ("summary_report", "licensed_all_beds", int)),
            ("All Bed Occupancy Rate", ("summary_report", "all_bed_occupancy_rate", float)),
            ("ICU Bed Occupancy Rate", ("summary_report", "icu_bed_occupancy_rate", float)),
            ("Population", ("summary_report", "population", int)),
            ("Population (20+)", ("summary_report", "population_gtr_20", int)),
            ("Population (65+)", ("summary_report", "population_gtr_65", int)),
            ("Staffed All Beds [Per 1000 People]", ("summary_report", "staffed_all_beds_per_1000", float)),
            ("Staffed All Beds [Per 1000 Adults (20+)]", ("summary_report", "staffed_all_beds_per_1000_gtr_20", float)),
            ("Staffed All Beds [Per 1000 Elderly (65+)]", ("summary_report", "staffed_all_beds_per_1000_gtr_65", float)),
            ("Staffed ICU Beds [Per 1000 People]", ("summary_report", "staffed_icu_beds_per_1000", float)),
            ("Staffed ICU Beds [Per 1000 Adults (20+)]", ("summary_report", "staffed_icu_beds_per_1000_gtr_20", float)),
            ("Staffed ICU Beds [Per 1000 Elderly (65+)]", ("summary_report", "staffed_icu_beds_per_1000_gtr_65", float)),
            ("Licensed All Beds [Per 1000 People]", ("summary_report", "licensed_all_beds_per_1000", float)),
            ("Licensed All Beds [Per 1000 Adults (20+)]", ("summary_report", "licensed_all_beds_per_1000_gtr_20", float)),
            ("Licensed All Beds [Per 1000 Elderly (65+)]", ("summary_report", "licensed_all_beds_per_1000_gtr_65", float)),
        ]

        state_fields = [
            ("State", ("summary_location", None, int)),
            ("State Name", ("summary_location", "province_state", str)),
            ("Staffed All Beds", ("summary_report", "staffed_all_beds", int)),
            ("Staffed ICU Beds", ("summary_report", "staffed_icu_beds", int)),
            ("Licensed All Beds", ("summary_report", "licensed_all_beds", int)),
            ("All Bed Occupancy Rate", ("summary_report", "all_bed_occupancy_rate", float)),
            ("ICU Bed Occupancy Rate", ("summary_report", "icu_bed_occupancy_rate", float)),
            ("Population", ("summary_report", "population", int)),
            ("Population (20+)", ("summary_report", "population_gtr_20", int)),
            ("Population (65+)", ("summary_report", "population_gtr_65", int)),
            ("Staffed All Beds [Per 1000 People]", ("summary_report", "staffed_all_beds_per_1000", float)),
            ("Staffed All Beds [Per 1000 Adults (20+)]", ("summary_report", "staffed_all_beds_per_1000_gtr_20", float)),
            ("Staffed All Beds [Per 1000 Elderly (65+)]", ("summary_report", "staffed_all_beds_per_1000_gtr_65", float)),
            ("Staffed ICU Beds [Per 1000 People]", ("summary_report", "staffed_icu_beds_per_1000", float)),
            ("Staffed ICU Beds [Per 1000 Adults (20+)]", ("summary_report", "staffed_icu_beds_per_1000_gtr_20", float)),
            ("Staffed ICU Beds [Per 1000 Elderly (65+)]", ("summary_report", "staffed_icu_beds_per_1000_gtr_65", float)),
            ("Licensed All Beds [Per 1000 People]", ("summary_report", "licensed_all_beds_per_1000", float)),
            ("Licensed All Beds [Per 1000 Adults (20+)]", ("summary_report", "licensed_all_beds_per_1000_gtr_20", float)),
            ("Licensed All Beds [Per 1000 Elderly (65+)]", ("summary_report", "licensed_all_beds_per_1000_gtr_65", float)),
            ("Estimated No. Full-Featured Mechanical Ventilators (2010 study estimate)", ("summary_report", "estimated_full_mech_ventilators", int)),
            ("Estimated No. Full-Featured Mechanical Ventilators per 100,000 Population (2010 study estimate)", ("summary_report", "estimated_full_mech_ventilators_per_100000", float)),
            ("Estimated No. Pediatrics-Capable Full-Feature Mechanical Ventilators (2010 study estimate)", ("summary_report", "estimated_full_mech_pediatric_ventilators", int)),
            ("Estimated No. Full-Feature Mechanical Ventilators, Pediatrics Capable per 100,000 Population <14 y (2010 study estimate)", ("summary_report", "estimated_full_mech_pediatric_ventilators_per_100000", float)),
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

            assert (
                headers[0] != "404: Not Found"
            ), "  Unable to get file contents, received {}.".format(headers)

            expected_h = list(self.headers_mapping[csv_type].keys())
            obtained_h = headers[: len(expected_h)]
            assert (
                obtained_h == expected_h
            ), "CSV headers have changed (expected {}, got {}). We may need to update the ETL code".format(
                expected_h, obtained_h
            )

            for row in reader:
                summary_location, summary_report = self.parse_row(
                    row, self.headers_mapping[csv_type]
                )

                self.summary_locations.append(summary_location)
                self.summary_reports.append(summary_report)

    def parse_row(self, row, mapping):
        summary_location = {"country_region": "US"}
        summary_report = {}

        for k, (i, (node_type, node_field, type_conv)) in mapping.items():
            if node_field:
                value = row[i]
                if value:
                    if node_type == "summary_location":
                        summary_location[node_field] = type_conv(value)
                    if node_type == "summary_report":
                        if type_conv == int:
                            summary_report[node_field] = type_conv(float(value))
                        else:
                            summary_report[node_field] = type_conv(value)

        summary_location_submitter_id = format_location_submitter_id(summary_location)

        summary_location["submitter_id"] = summary_location_submitter_id
        summary_location["projects"] = [{"code": self.project_code}]

        summary_report["submitter_id"] = format_summary_report_submitter_id(
            summary_location_submitter_id,
            date=datetime.date.today().strftime("%Y-%m-%d"),
        )
        summary_report["summary_locations"] = [
            {"submitter_id": summary_location_submitter_id}
        ]

        return summary_location, summary_report

    def submit_metadata(self):
        print("Submitting summary_location data")
        for loc in self.summary_locations:
            loc_record = {"type": "summary_location"}
            loc_record.update(loc)
            self.metadata_helper.add_record_to_submit(loc_record)
        self.metadata_helper.batch_submit_records()

        print("Submitting summary_report data")
        for rep in self.summary_reports:
            rep_record = {"type": "summary_report"}
            rep_record.update(rep)
            self.metadata_helper.add_record_to_submit(rep_record)
        self.metadata_helper.batch_submit_records()
