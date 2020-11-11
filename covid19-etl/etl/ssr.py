# to run:
# export ACCESS_TOKEN=myaccesstoken
# JOB_NAME=SSR FILE_PATH=path/to/file.csv python covid19-etl/main.py

from collections import defaultdict
import csv
from datetime import datetime
import os
import xlrd

from etl import base
from utils.metadata_helper import MetadataHelper
from utils.format_helper import format_submitter_id, derived_submitter_id


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
SUBMISSION_ORDER = ["summary_location", "statistical_summary_report"]
MINIMUM_COUNT = 5


def format_value(key, value, _type, date_mode=None):
    if key == "report_date":
        if type(value) == float and date_mode is not None:  # rush date format
            # Excel stores dates as floats and they must be
            # converted first to a tuple and then a string.
            date = datetime(*xlrd.xldate_as_tuple(value, date_mode))
            value = date.strftime("%Y-%m-%d")
        elif " " in value:  # uchicago date format
            value = value.split(" ")[0]

    if _type == int and int(value) < MINIMUM_COUNT:
        print(
            f"  Warning: Found value < {MINIMUM_COUNT} ({key}: {value}). This value cannot be submitted, replacing it with null."
        )
        return None

    return _type(value)


class SSR(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.subjects = []
        self.demographics = []

        self.program_name = "controlled"
        self.project_code = "SSR"
        self.country = "US"
        self.state = "IL"

        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        # self.records = { <node ID>: { <submitter_id: { <data> } } }
        self.records = defaultdict(dict)

        # TODO temporary - for now this ETL can only be run manually
        self.file_path = os.environ.get("FILE_PATH")
        if not self.file_path:
            # log instead of exception so that unit tests don't complain
            print("Need FILE_PATH environment variable (SSR file to parse)")

    def files_to_submissions(self):
        """
        Reads input files and converts the data to Sheepdog records
        """
        print("Parsing file: {}".format(self.file_path))
        extension = self.file_path.lower().split(".")[-1]
        if extension == "txt":
            self.parse_txt_input_file()
        elif extension == "xlsx":
            self.parse_xlsx_input_file()
        else:
            raise Exception(
                f"I don't know how to parse extension {extension} for file {self.file_path}"
            )

    def parse_txt_input_file(self):
        with open(self.file_path, newline="") as csvfile:
            reader = csv.reader(csvfile, delimiter="|")
            header = next(reader)
            header = {k: v for v, k in enumerate(header)}

            for row in reader:
                row_data = dict(zip(header, row))
                self.parse_input(row_data=row_data)

    def parse_xlsx_input_file(self):
        # Set up file path, workbook, and sheet.
        wb = xlrd.open_workbook(self.file_path)
        sheet = wb.sheet_by_index(0)

        # Create lists for SSR properties and value from Excel sheet.
        prop_list = sheet.col_values(0)[1:]
        value_list = sheet.col_values(1)[1:]

        col_data = dict(zip(prop_list, value_list))
        self.parse_input(row_data=col_data, date_mode=wb.datemode)

    def parse_input(self, row_data, date_mode=None):
        # (original property, (gen3 node, gen3 property, property type))
        mapping = [
            ("reportingOrg", ("summary_location", "reporting_org", str)),
            ("reportDate", ("statistical_summary_report", "report_date", str)),
            ("num_COVID", ("statistical_summary_report", "num_COVID", int)),
            (
                "num_COVID_deaths",
                ("statistical_summary_report", "num_COVID_deaths", int),
            ),
            ("num_outpatient", ("statistical_summary_report", "num_outpatient", int)),
            ("num_admitted", ("statistical_summary_report", "num_admitted", int)),
            ("num_icu", ("statistical_summary_report", "num_icu", int)),
            ("num_vent", ("statistical_summary_report", "num_vent", int)),
            ("num_resp", ("statistical_summary_report", "num_resp", int)),
            ("num_pneu", ("statistical_summary_report", "num_pneu", int)),
            ("num_diab", ("statistical_summary_report", "num_diab", int)),
            ("num_asth", ("statistical_summary_report", "num_asth", int)),
            ("num_obes", ("statistical_summary_report", "num_obes", int)),
            ("num_card", ("statistical_summary_report", "num_card", int)),
            ("num_chf", ("statistical_summary_report", "num_chf", int)),
        ]

        # row_records = { <node ID>: { <record data> } }
        # (there is only 1 record of each node type per row)
        row_records = defaultdict(dict)

        for orig_prop_name, (node_type, prop_name, _type) in mapping:
            if row_data[orig_prop_name]:
                row_records[node_type][prop_name] = format_value(
                    prop_name, row_data[orig_prop_name], _type, date_mode
                )

        # add missing summary_location props
        summary_location_submitter_id = format_submitter_id(
            "summary_location",
            {"reporting_org": row_records["summary_location"]["reporting_org"]},
        )
        row_records["summary_location"].update(
            {
                "type": "summary_location",
                "submitter_id": summary_location_submitter_id,
                "projects": {"code": self.project_code},
                "country_region": self.country,
                "province_state": self.state,
            }
        )

        # add missing statistical_summary_report props
        ssr_submitter_id = derived_submitter_id(
            summary_location_submitter_id,
            "statistical_summary_report",
            "ssr",
            {"report_date": row_records["statistical_summary_report"]["report_date"]},
        )
        row_records["statistical_summary_report"].update(
            {
                "type": "statistical_summary_report",
                "submitter_id": ssr_submitter_id,
                "summary_locations": {"submitter_id": summary_location_submitter_id},
            }
        )

        for node_type in row_records:
            rec = row_records[node_type]
            self.records[node_type][rec["submitter_id"]] = rec

    def submit_metadata(self):
        # TODO check which summary_locations already exist
        for node_type in SUBMISSION_ORDER:
            recs = self.records[node_type].values()
            self.metadata_helper.add_records_to_submit(recs)
            self.metadata_helper.batch_submit_records()
