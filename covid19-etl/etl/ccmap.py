import csv
from contextlib import closing
import datetime
import re

import requests

from etl import base
from utils.metadata_helper import MetadataHelper


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


def format_summary_clinical_submitter_id(location_submitter_id, date):
    return "{}_{}".format(
        location_submitter_id.replace("summary_location_", "summary_clinical_"), date
    )


def state_to_long(state):
    short_to_long = {
        "AK": "Alaska",
        "AL": "Alabama",
        "AR": "Arkansas",
        "AZ": "Arizona",
        "CA": "California",
        "CO": "Colorado",
        "CT": "Connecticut",
        "DC": "District of Columbia",
        "DE": "Delaware",
        "FL": "Florida",
        "GA": "Georgia",
        "HI": "Hawaii",
        "IA": "Iowa",
        "ID": "Idaho",
        "IL": "Illinois",
        "IN": "Indiana",
        "KS": "Kansas",
        "KY": "Kentucky",
        "LA": "Louisiana",
        "MA": "Massachusetts",
        "MD": "Maryland",
        "ME": "Maine",
        "MI": "Michigan",
        "MN": "Minnesota",
        "MO": "Missouri",
        "MS": "Mississippi",
        "MT": "Montana",
        "NC": "North Carolina",
        "ND": "North Dakota",
        "NE": "Nebraska",
        "NH": "New Hampshire",
        "NJ": "New Jersey",
        "NM": "New Mexico",
        "NV": "Nevada",
        "NY": "New York",
        "OH": "Ohio",
        "OK": "Oklahoma",
        "OR": "Oregon",
        "PA": "Pennsylvania",
        "PR": "Puerto Rico",
        "RI": "Rhode Island",
        "SC": "South Carolina",
        "SD": "South Dakota",
        "TN": "Tennessee",
        "TX": "Texas",
        "UT": "Utah",
        "VA": "Virginia",
        "VT": "Vermont",
        "WA": "Washington",
        "WI": "Wisconsin",
        "WV": "West Virginia",
        "WY": "Wyoming",
    }

    return short_to_long[state]


class CCMAP(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.summary_locations = []
        self.summary_clinicals = []

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
            ("Staffed All Beds", ("summary_clinical", "staffed_all_beds", int)),
            ("Staffed ICU Beds", ("summary_clinical", "staffed_icu_beds", int)),
            ("Licensed All Beds", ("summary_clinical", "licensed_all_beds", int)),
            (
                "All Bed Occupancy Rate",
                ("summary_clinical", "all_bed_occupancy_rate", float),
            ),
            (
                "ICU Bed Occupancy Rate",
                ("summary_clinical", "icu_bed_occupancy_rate", float),
            ),
            ("Population", ("summary_clinical", "population", int)),
            ("Population (20+)", ("summary_clinical", "population_gtr_20", int)),
            ("Population (65+)", ("summary_clinical", "population_gtr_65", int)),
            (
                "Staffed All Beds [Per 1000 People]",
                ("summary_clinical", "staffed_all_beds_per_1000", float),
            ),
            (
                "Staffed All Beds [Per 1000 Adults (20+)]",
                ("summary_clinical", "staffed_all_beds_per_1000_gtr_20", float),
            ),
            (
                "Staffed All Beds [Per 1000 Elderly (65+)]",
                ("summary_clinical", "staffed_all_beds_per_1000_gtr_65", float),
            ),
            (
                "Staffed ICU Beds [Per 1000 People]",
                ("summary_clinical", "staffed_icu_beds_per_1000", float),
            ),
            (
                "Staffed ICU Beds [Per 1000 Adults (20+)]",
                ("summary_clinical", "staffed_icu_beds_per_1000_gtr_20", float),
            ),
            (
                "Staffed ICU Beds [Per 1000 Elderly (65+)]",
                ("summary_clinical", "staffed_icu_beds_per_1000_gtr_65", float),
            ),
            (
                "Licensed All Beds [Per 1000 People]",
                ("summary_clinical", "licensed_all_beds_per_1000", float),
            ),
            (
                "Licensed All Beds [Per 1000 Adults (20+)]",
                ("summary_clinical", "licensed_all_beds_per_1000_gtr_20", float),
            ),
            (
                "Licensed All Beds [Per 1000 Elderly (65+)]",
                ("summary_clinical", "licensed_all_beds_per_1000_gtr_65", float),
            ),
        ]

        state_fields = [
            ("State", ("summary_location", None, int)),
            ("State Name", ("summary_location", "province_state", str)),
            ("Staffed All Beds", ("summary_clinical", "staffed_all_beds", int)),
            ("Staffed ICU Beds", ("summary_clinical", "staffed_icu_beds", int)),
            ("Licensed All Beds", ("summary_clinical", "licensed_all_beds", int)),
            (
                "All Bed Occupancy Rate",
                ("summary_clinical", "all_bed_occupancy_rate", float),
            ),
            (
                "ICU Bed Occupancy Rate",
                ("summary_clinical", "icu_bed_occupancy_rate", float),
            ),
            ("Population", ("summary_clinical", "population", int)),
            ("Population (20+)", ("summary_clinical", "population_gtr_20", int)),
            ("Population (65+)", ("summary_clinical", "population_gtr_65", int)),
            (
                "Staffed All Beds [Per 1000 People]",
                ("summary_clinical", "staffed_all_beds_per_1000", float),
            ),
            (
                "Staffed All Beds [Per 1000 Adults (20+)]",
                ("summary_clinical", "staffed_all_beds_per_1000_gtr_20", float),
            ),
            (
                "Staffed All Beds [Per 1000 Elderly (65+)]",
                ("summary_clinical", "staffed_all_beds_per_1000_gtr_65", float),
            ),
            (
                "Staffed ICU Beds [Per 1000 People]",
                ("summary_clinical", "staffed_icu_beds_per_1000", float),
            ),
            (
                "Staffed ICU Beds [Per 1000 Adults (20+)]",
                ("summary_clinical", "staffed_icu_beds_per_1000_gtr_20", float),
            ),
            (
                "Staffed ICU Beds [Per 1000 Elderly (65+)]",
                ("summary_clinical", "staffed_icu_beds_per_1000_gtr_65", float),
            ),
            (
                "Licensed All Beds [Per 1000 People]",
                ("summary_clinical", "licensed_all_beds_per_1000", float),
            ),
            (
                "Licensed All Beds [Per 1000 Adults (20+)]",
                ("summary_clinical", "licensed_all_beds_per_1000_gtr_20", float),
            ),
            (
                "Licensed All Beds [Per 1000 Elderly (65+)]",
                ("summary_clinical", "licensed_all_beds_per_1000_gtr_65", float),
            ),
            (
                "Estimated No. Full-Featured Mechanical Ventilators (2010 study estimate)",
                ("summary_clinical", "estimated_full_mech_ventilators", int),
            ),
            (
                "Estimated No. Full-Featured Mechanical Ventilators per 100,000 Population (2010 study estimate)",
                (
                    "summary_clinical",
                    "estimated_full_mech_ventilators_per_100000",
                    float,
                ),
            ),
            (
                "Estimated No. Pediatrics-Capable Full-Feature Mechanical Ventilators (2010 study estimate)",
                ("summary_clinical", "estimated_full_mech_pediatric_ventilators", int),
            ),
            (
                "Estimated No. Full-Feature Mechanical Ventilators, Pediatrics Capable per 100,000 Population <14 y (2010 study estimate)",
                (
                    "summary_clinical",
                    "estimated_full_mech_pediatric_ventilators_per_100000",
                    float,
                ),
            ),
        ]

        self.headers_mapping = {
            "county": {
                field: (k, mapping) for k, (field, mapping) in enumerate(county_fields)
            },
            "state": {
                field: (k, mapping) for k, (field, mapping) in enumerate(state_fields)
            },
        }

    def files_to_submissions(self):
        """
        Reads CSV files and converts the data to Sheepdog records
        """
        repo = "covidcaremap/covid19-healthsystemcapacity"
        branch = "master"
        files = {
            "county": "data/published/us_healthcare_capacity-county-CovidCareMap.csv",
            "state": "data/published/us_healthcare_capacity-state-CovidCareMap.csv",
        }

        for k, url in files.items():
            self.parse_file(repo, branch, url, csv_type=k)

    def get_last_update_date_file(self, repo, url):
        """
        Gets latest update time for specific file in the repository

        :param repo: "user/repository" for Github repository
        :param url: path to file
        :return: last update (commit) datetime for the file
        """
        api_url = "https://api.github.com/repos"
        commit_info_url = "{}/{}/{}{}{}".format(
            api_url, repo, "commits?path=", url, "&page=1&per_page=1"
        )

        with closing(requests.get(commit_info_url, stream=True)) as r:
            commit_info = r.json()
            last_update_date = commit_info[0]["commit"]["committer"]["date"]

        return datetime.datetime.strptime(last_update_date, "%Y-%m-%dT%H:%M:%SZ")

    def parse_file(self, repo, branch, file_url, csv_type):
        last_update_date = self.get_last_update_date_file(repo, file_url)

        raw_url = "https://raw.githubusercontent.com"
        url = "{}/{}/{}/{}".format(raw_url, repo, branch, file_url)

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
                summary_location, summary_clinical = self.parse_row(
                    row, self.headers_mapping[csv_type], last_update_date
                )

                self.summary_locations.append(summary_location)
                self.summary_clinicals.append(summary_clinical)

    def parse_row(self, row, mapping, last_update_date):
        summary_location = {"country_region": "US"}
        summary_clinical = {}

        for k, (i, (node_type, node_field, type_conv)) in mapping.items():
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

        state = summary_location["province_state"]
        if len(state) == 2:
            summary_location["province_state"] = state_to_long(state)

        summary_clinical["submitter_id"] = format_summary_clinical_submitter_id(
            summary_location_submitter_id, date=last_update_date.strftime("%Y-%m-%d")
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
        for sc in self.summary_clinicals:
            sc_record = {"type": "summary_clinical"}
            sc_record.update(sc)
            self.metadata_helper.add_record_to_submit(sc_record)
        self.metadata_helper.batch_submit_records()
