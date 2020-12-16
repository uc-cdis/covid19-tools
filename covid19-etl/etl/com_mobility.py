import requests
import csv
import re
from contextlib import closing
from datetime import datetime
from dateutil.parser import parse

from etl import base
from utils.metadata_helper import MetadataHelper


def convert_to_int(s):
    try:
        return int(s)
    except Exception:
        return None


def convert_to_list(s):
    if type(s) == list:
        return s
    return [s]


def get_enum_value(l, default, s):
    if s in l:
        return s
    return default


def convert_datetime_to_str(dt):
    if type(dt) != datetime:
        return None
    return dt.strftime("%Y-%m-%d")


def process_county_function(county):
    return county.replace(" County", "").replace(" county", "").strip()


def identity_function(s):
    return s


# The files need to be handled so that they are compatible
# to gen3 fields
SPECIAL_MAP_FIELDS = {
    "country_region_code": ("iso2", identity_function),
    "country_region": ("country_region", identity_function),
    "metro_area": ("metro_area", identity_function),
    "iso_3166_2_code": ("iso_3166_2", identity_function),
    "sub_region_1": ("province_state", process_county_function),
    "sub_region_2": ("county", process_county_function),
    "census_fips_code": ("FIPS", convert_to_int),
    "date": ("report_date", identity_function),
    "retail_and_recreation_percent_change_from_baseline": (
        "retail_and_recreation_percent_change_from_baseline",
        convert_to_int,
    ),
    "grocery_and_pharmacy_percent_change_from_baseline": (
        "grocery_and_pharmacy_percent_change_from_baseline",
        convert_to_int,
    ),
    "parks_percent_change_from_baseline": (
        "parks_percent_change_from_baseline",
        convert_to_int,
    ),
    "transit_stations_percent_change_from_baseline": (
        "transit_stations_percent_change_from_baseline",
        convert_to_int,
    ),
    "workplaces_percent_change_from_baseline": (
        "workplaces_percent_change_from_baseline",
        convert_to_int,
    ),
    "residential_percent_change_from_baseline": (
        "residential_percent_change_from_baseline",
        convert_to_int,
    ),
}


def format_submitter_id(node_name, *argv):
    """Format submitter id"""
    submitter_id = node_name
    for v in argv:
        submitter_id = submitter_id + f"_{v}"
    submitter_id = submitter_id.lower().replace(", ", "_")
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)
    return submitter_id.strip("-")


class COM_MOBILITY(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "Com-Mobility"

        self.metadata_helper = MetadataHelper(
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

        self.summary_locations = []
        self.summary_socio_demographics = []

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

        self.last_submission_date_time = self.metadata_helper.get_last_submission()
        the_lattest_data_datetime = None

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

            for row in reader:
                # ignore any empty row
                if not row:
                    continue

                row_dict = dict(zip(headers, row))
                if row_dict["country_region_code"] != "US":
                    continue

                if (
                    not self.last_submission_date_time
                    or parse(row_dict["date"]) > self.last_submission_date_time
                ):
                    if (
                        the_lattest_data_datetime is None
                        or the_lattest_data_datetime < parse(row_dict["date"])
                    ):
                        the_lattest_data_datetime = parse(row_dict["date"])

                    summary_location = {}
                    summary_socio_demographic = {}

                    summary_location_submitter_id = format_submitter_id(
                        "summary_location",
                        row_dict["country_region_code"],
                        row_dict["sub_region_1"].replace("county", "").strip(),
                        row_dict["sub_region_2"].replace("county", "").strip(),
                        row_dict["metro_area"],
                        row_dict["date"],
                    )

                    summary_socio_demographic_submitter_id = format_submitter_id(
                        "summary_socio_demographic",
                        row_dict["country_region_code"],
                        row_dict["sub_region_1"],
                        row_dict["sub_region_2"],
                        row_dict["metro_area"],
                        row_dict["date"],
                    )

                    summary_location = {
                        "submitter_id": summary_location_submitter_id,
                        "projects": [{"code": self.project_code}],
                    }

                    summary_socio_demographic = {
                        "submitter_id": summary_location_submitter_id,
                        "summary_locations": [
                            {"submitter_id": summary_location_submitter_id}
                        ],
                    }

                    for field in [
                        "country_region_code",
                        "country_region",
                        "sub_region_1",
                        "sub_region_2",
                        "metro_area",
                        "iso_3166_2_code",
                        "census_fips_code",
                    ]:
                        gen3_field, func = SPECIAL_MAP_FIELDS[field]
                        summary_location[gen3_field] = func(row_dict[field])

                    for field in [
                        "retail_and_recreation_percent_change_from_baseline",
                        "grocery_and_pharmacy_percent_change_from_baseline",
                        "parks_percent_change_from_baseline",
                        "transit_stations_percent_change_from_baseline",
                        "workplaces_percent_change_from_baseline",
                        "residential_percent_change_from_baseline",
                        "date",
                    ]:
                        gen3_field, func = SPECIAL_MAP_FIELDS[field]
                        summary_socio_demographic[gen3_field] = func(row_dict[field])

                    self.summary_locations.append(summary_location)
                    self.summary_socio_demographics.append(summary_socio_demographic)
        self.last_submission_date_time = the_lattest_data_datetime

    def submit_metadata(self):
        """
        Converts the data in `self.time_series_data` to Sheepdog records.
        `self.location_data already contains Sheepdog records. Batch submits
        all records in `self.location_data` and `self.time_series_data`
        """

        # Commented
        # Only required for one time submission of summary_location
        print("Submitting summary_location data")
        for loc in self.summary_locations:
            loc_record = {"type": "summary_location"}
            loc_record.update(loc)
            self.metadata_helper.add_record_to_submit(loc_record)
        self.metadata_helper.batch_submit_records()

        print("Submitting summary_socio_demographic data")
        for sc in self.summary_socio_demographics:
            sc_record = {"type": "summary_socio_demographic"}
            sc_record.update(sc)
            self.metadata_helper.add_record_to_submit(sc_record)
        self.metadata_helper.batch_submit_records()
        self.metadata_helper.update_last_submission(
            self.last_submission_date_time.strftime("%Y-%m-%d")
        )
