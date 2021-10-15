from contextlib import closing
import datetime

from etl import base
from utils.idph_helper import fields_mapping
from utils.format_helper import (
    derived_submitter_id,
    format_submitter_id,
)
from utils.metadata_helper import MetadataHelper


class IDPH_ZIPCODE(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "IDPH-zipcode"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.country = "US"
        self.state = "IL"

        self.summary_locations = {}  # { <submitter_id>: <record> }
        self.summary_clinicals = []

    def files_to_submissions(self):
        """
        Reads JSON file and convert the data to Sheepdog records
        """

        latest_submitted_date = self.metadata_helper.get_latest_submitted_date_idph()
        today = datetime.date.today()
        if latest_submitted_date == today:
            print("Nothing to submit: today and latest submitted date are the same.")
            return

        print(
            f"Latest submitted date: {latest_submitted_date}. Getting data until date: {today}"
        )
        for i in range(int((today - latest_submitted_date).days)):
            date = latest_submitted_date + datetime.timedelta(i + 1)
            self.parse_data(date.strftime("%Y-%m-%d"))

    def parse_data(self, date_str):
        """
        Converts a JSON files to data we can submit via Sheepdog. Stores the
        records to submit in `self.summary_locations` and `self.summary_clinicals`.

        Args:
            date_str (str): date in "%Y-%m-%d" format
        """
        existing_summary_locations = (
            self.metadata_helper.get_existing_summary_locations()
        )
        url = f"https://idph.illinois.gov/DPHPublicInformation/api/COVIDExport/GetZip?reportDate={date_str}"
        print("Getting data from {}".format(url))
        with closing(self.get(url, stream=True)) as r:
            data = r.json()
            for zipcode_values in data:
                (summary_location, summary_clinical) = self.parse_zipcode(
                    zipcode_values
                )

                sl_id = summary_location["submitter_id"]
                if sl_id not in existing_summary_locations:
                    self.summary_locations[sl_id] = summary_location
                self.summary_clinicals.append(summary_clinical)

    def parse_zipcode(self, zipcode_values):
        """
        From county-level data, generate the data we can submit via Sheepdog
        """
        date = datetime.datetime.strptime(
            zipcode_values["reportDate"], "%Y-%m-%dT%H:%M:%S"
        ).strftime("%Y-%m-%d")
        zipcode = zipcode_values["zip"]

        summary_location_submitter_id = format_submitter_id(
            "summary_location",
            {"country": self.country, "state": self.state, "zipcode": zipcode},
        )

        summary_location = {
            "submitter_id": summary_location_submitter_id,
            "country_region": self.country,
            "province_state": self.state,
            "zipcode": zipcode,
            "projects": [{"code": self.project_code}],
        }

        summary_clinical_submitter_id = derived_submitter_id(
            summary_location_submitter_id,
            "summary_location",
            "summary_clinical",
            {"date": date},
        )
        summary_clinical = {
            "submitter_id": summary_clinical_submitter_id,
            "date": date,
            "confirmed": zipcode_values["confirmed_cases"],
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        if "demographics" in zipcode_values:
            demographic = zipcode_values["demographics"]

            for k, v in fields_mapping.items():
                field, mapping = v
                demographic_group = demographic[k]

                for item in demographic_group:
                    dst_field = mapping[item[field]]
                    if dst_field:
                        if "count" in item:
                            age_group_count_field = "{}_{}".format(
                                mapping[item[field]], "count"
                            )
                            summary_clinical[age_group_count_field] = item["count"]
                        if "tested" in item:
                            age_group_tested_field = "{}_{}".format(
                                mapping[item[field]], "tested"
                            )
                            summary_clinical[age_group_tested_field] = item["tested"]

        return summary_location, summary_clinical

    def submit_metadata(self):
        """
        Submits the data in `self.summary_locations` and `self.summary_clinicals` to Sheepdog.
        """
        print("Submitting data...")
        print("Submitting summary_location data")
        for sl in self.summary_locations.values():
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
