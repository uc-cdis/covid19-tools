import datetime
import os
from contextlib import closing

from etl import base
from utils.idph_helper import fields_mapping
from utils.format_helper import (
    derived_submitter_id,
    format_submitter_id,
    idph_get_date,
)
from utils.metadata_helper import MetadataHelper

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


class IDPH(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "IDPH"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.country = "US"
        self.state = "IL"

        self.county_dict = {}
        self.il_counties()

        self.summary_locations = []
        self.summary_clinicals = []

    def get_location_and_clinical_submitter_id(self, county, date):
        summary_location_submitter_id = format_submitter_id(
            "summary_location",
            {"country": self.country, "state": self.state, "county": county}
            if county is not None
            else {"country": self.country, "state": self.state},
        )
        summary_clinical_submitter_id = derived_submitter_id(
            summary_location_submitter_id,
            "summary_location",
            "summary_clinical",
            {"date": date},
        )
        return summary_location_submitter_id, summary_clinical_submitter_id

    def il_counties(self):
        with open(
            os.path.join(CURRENT_DIR, "data/IL_counties_central_coords_lat_long.tsv")
        ) as f:
            counties = f.readlines()
            counties = counties[1:]
            counties = map(lambda l: l.strip().split("\t"), counties)

        for county, lat, lon in counties:
            self.county_dict[county] = {"lat": lat, "lon": lon}

    def files_to_submissions(self):
        """
        Reads JSON file and convert the data to Sheepdog records.
        """
        latest_submitted_date = self.metadata_helper.get_latest_submitted_date_idph()
        today = datetime.date.today()
        if latest_submitted_date == today:
            print("Nothing to submit: today and latest submitted date are the same.")
            return

        today_str = today.strftime("%Y%m%d")
        print(f"Getting data for date: {today_str}")

        # they changed the URL on April 1, 2020
        if today > datetime.date(2020, 3, 31):
            url = "http://www.dph.illinois.gov/sitefiles/COVIDTestResults.json"
        else:
            url = f"https://www.dph.illinois.gov/sites/default/files/COVID19/COVID19CountyResults{today_str}.json"
        self.parse_file(latest_submitted_date, url)

    def parse_file(self, latest_submitted_date, url):
        """
        Converts a JSON files to data we can submit via Sheepdog. Stores the
        records to submit in `self.summary_locations` and `self.summary_clinicals`.

        Args:
            latest_submitted_date (date): date for latest submitted date
            url (str): URL at which the JSON file is available
        """
        print("Getting data from {}".format(url))
        with closing(self.get(url, stream=True)) as r:
            data = r.json()
            date = idph_get_date(data["LastUpdateDate"])

            if latest_submitted_date and date == latest_submitted_date.strftime(
                "%Y-%m-%d"
            ):
                print(
                    "Nothing to submit: latest submitted date and date from data are the same."
                )
                return

            for county in data["characteristics_by_county"]["values"]:
                demographic = data.get("demographics", None)
                summary_location, summary_clinical = self.parse_county(
                    date, county, demographic
                )

                self.summary_locations.append(summary_location)
                self.summary_clinicals.append(summary_clinical)

            for illinois_data in data["state_testing_results"]["values"]:
                illinois_historic_data = self.parse_historical_data(illinois_data)
                self.summary_clinicals.append(illinois_historic_data)

    def parse_historical_data(self, illinois_data):
        """
        Parses historical state-level data. "summary_location" node is created
        from "characteristics_by_county" data.

        Args:
            illinois_data (dict): data JSON with "testDate", "total_tested",
                "confirmed_cases" and "deaths"

        Returns:
            dict: "summary_clinical" node for Sheepdog
        """
        county = "Illinois"

        date = datetime.datetime.strptime(
            illinois_data["testDate"], "%m/%d/%Y"
        ).strftime("%Y-%m-%d")

        (
            summary_location_submitter_id,
            summary_clinical_submitter_id,
        ) = self.get_location_and_clinical_submitter_id(county, date)

        summary_clinical = {
            "submitter_id": summary_clinical_submitter_id,
            "date": date,
            "confirmed": illinois_data["confirmed_cases"],
            "testing": illinois_data["total_tested"],
            "deaths": illinois_data["deaths"],
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        return summary_clinical

    def parse_county(self, date, county_json, demographic):
        """
        From county-level data, generate the data we can submit via Sheepdog

        Args:
            date (date): date
            county_json (dict): JSON for county statistics

        Returns:
            (dict, dict): "summary_location" and "summary_clinical" records
        """
        county = county_json["County"]

        (
            summary_location_submitter_id,
            summary_clinical_submitter_id,
        ) = self.get_location_and_clinical_submitter_id(county, date)

        summary_location = {
            "submitter_id": summary_location_submitter_id,
            "country_region": self.country,
            "province_state": self.state,
            "projects": [{"code": self.project_code}],
        }

        # the IDPH data use Illinois in "County" field for aggregated data
        # in Gen3 it would equal to location with "province_state" equal to "IL" and no "County" field
        if county != "Illinois":
            summary_location["county"] = county

        if county in self.county_dict:
            summary_location["latitude"] = self.county_dict[county]["lat"]
            summary_location["longitude"] = self.county_dict[county]["lon"]
        else:
            if county_json["lat"] != 0:
                summary_location["latitude"] = str(county_json["lat"])
            if county_json["lon"] != 0:
                summary_location["longitude"] = str(county_json["lon"])

        summary_clinical = {
            "submitter_id": summary_clinical_submitter_id,
            "date": date,
            "confirmed": county_json["confirmed_cases"],
            "testing": county_json["total_tested"],
            "deaths": county_json["deaths"],
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        if "negative" in county_json:
            summary_clinical["negative"] = county_json["negative"]

        if county == "Illinois" and demographic:
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
