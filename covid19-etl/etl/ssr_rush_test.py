# to run:
# export ACCESS_TOKEN=myaccesstoken
# JOB_NAME=SSR FILE_PATH=path/to/file.csv python covid19-etl/main.py

from collections import defaultdict
from datetime import datetime
import os
import xlrd
import datetime

from etl import base
from helper.metadata_helper import MetadataHelper
from helper.format_helper import format_submitter_id, derived_submitter_id

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
SUBMISSION_ORDER = ["summary_location", "statistical_summary_report"]


class SSR_RUSH_TEST(base.BaseETL):
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
        self.parse_input_file()

    def format_value(self, key, value):
        if key == "report_date":
            value = value.split(" ")[0]
        return value

    def parse_input_file(self):
        print("Parsing file: {}".format(self.file_path))

        # (original property, (gen3 node, gen3 property, property type))
        mapping = [
            ("reportDate", ("statistical_summary_report", "report_date", str)),
            ("reportingOrg", ("summary_location", "reporting_org", str)),
            ("num_COVID", ("statistical_summary_report", "num_COVID", int)),
            ("num_COVID_eth0", ("statistical_summary_report", "num_COVID_eth0", int)),
            ("num_COVID_race0", ("statistical_summary_report", "num_COVID_race0", int)),
            ("num_COVID_race1", ("statistical_summary_report", "num_COVID_race1", int)),
            ("num_COVID_race2", ("statistical_summary_report", "num_COVID_race2", int)),
            ("num_COVID_race3", ("statistical_summary_report", "num_COVID_race3", int)),
            ("num_COVID_race4", ("statistical_summary_report", "num_COVID_race4", int)),
            (
                "num_COVID_deaths",
                ("statistical_summary_report", "num_COVID_deaths", int),
            ),
            (
                "num_COVID_deaths_eth0",
                ("statistical_summary_report", "num_COVID_deaths_eth0", int),
            ),
            (
                "num_COVID_deaths_eth1",
                ("statistical_summary_report", "num_COVID_deaths_eth1", int),
            ),
            (
                "num_COVID_deaths_race0",
                ("statistical_summary_report", "num_COVID_deaths_race0", int),
            ),
            (
                "num_COVID_deaths_race1",
                ("statistical_summary_report", "num_COVID_deaths_race1", int),
            ),
            (
                "num_COVID_deaths_race2",
                ("statistical_summary_report", "num_COVID_deaths_race2", int),
            ),
            (
                "num_COVID_deaths_race3",
                ("statistical_summary_report", "num_COVID_deaths_race3", int),
            ),
            (
                "num_COVID_deaths_race4",
                ("statistical_summary_report", "num_COVID_deaths_race4", int),
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

        # Set up file path, workbook, and sheet.
        loc = self.file_path
        wb = xlrd.open_workbook(loc)
        sheet = wb.sheet_by_index(0)

        # Excel stores dates as floats and they must be converted first to a tuple and then a string.
        date_cell = sheet.cell(1, 1)
        fixed_date = datetime.datetime(
            *xlrd.xldate_as_tuple(date_cell.value, wb.datemode)
        )
        new_date = fixed_date.strftime("%Y-%m-%d")

        # Create lists for SSR properties and value from Excel sheet.
        prop_list = []
        value_list = []
        value_list.append(new_date)

        # Loop to get all properties from first column.
        for i in range(1, sheet.nrows):
            row_prop = sheet.cell_value(i, 0)
            prop_list.append(row_prop)

        # Loop to get all values from second column.
        for i in range(2, sheet.nrows):
            row_data = sheet.cell_value(i, 1)
            value_list.append(row_data)

        col_records = defaultdict(dict)
        col_data = dict(zip(prop_list, value_list))
        print(col_data)

        for orig_prop_name, (node_type, prop_name, _type) in mapping:
            if col_data[orig_prop_name]:
                col_records[node_type][prop_name] = _type(
                    self.format_value(prop_name, col_data[orig_prop_name])
                )
        print(col_records)

        # add missing summary_location props
        summary_location_submitter_id = format_submitter_id(
            "summary_location",
            {"reporting_org": col_records["summary_location"]["reporting_org"]},
        )
        col_records["summary_location"].update(
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
            {"report_date": col_records["statistical_summary_report"]["report_date"]},
        )
        col_records["statistical_summary_report"].update(
            {
                "type": "statistical_summary_report",
                "submitter_id": ssr_submitter_id,
                "summary_locations": {"submitter_id": summary_location_submitter_id},
            }
        )

        for node_type in col_records:
            rec = col_records[node_type]
            self.records[node_type][rec["submitter_id"]] = rec

    def submit_metadata(self):
        # TODO check which summary_locations already exist
        for node_type in SUBMISSION_ORDER:
            recs = self.records[node_type].values()
            self.metadata_helper.add_records_to_submit(recs)
            self.metadata_helper.batch_submit_records()
