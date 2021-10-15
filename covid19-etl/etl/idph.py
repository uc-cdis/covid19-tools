from contextlib import closing
import datetime
import os
import time

from etl import base
from utils.idph_helper import fields_mapping
from utils.format_helper import (
    derived_submitter_id,
    format_submitter_id,
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

        self.county_dict = {}  # { <county name>: {"lat": int, "lon": int} }

        self.summary_locations = {}  # { <submitter_id>: <record> }
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

    def parse_il_counties(self):
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
        start = time.time()

        latest_submitted_date = self.metadata_helper.get_latest_submitted_date_idph()
        today = datetime.date.today()
        if latest_submitted_date == today:
            print("Nothing to submit: today and latest submitted date are the same.")
            return

        print(
            f"Latest submitted date: {latest_submitted_date}. Getting data until date: {today}"
        )

        self.parse_il_counties()

        # convert date to datetime
        latest_submitted_datetime = (
            datetime.datetime(
                latest_submitted_date.year,
                latest_submitted_date.month,
                latest_submitted_date.day,
            )
            if latest_submitted_date
            else None
        )

        for county in self.county_dict:
            self.parse_county_data(latest_submitted_datetime, county)

        self.parse_state_data(latest_submitted_datetime)

        print("Done in {} secs".format(int(time.time() - start)))

    def parse_county_data(self, latest_submitted_date, county):
        """
        Converts a JSON files to data we can submit via Sheepdog. Stores the
        records to submit in `self.summary_locations` and `self.summary_clinicals`.

        Args:
            latest_submitted_date (datetime): date for latest submitted date
            county (str): county name
        """
        demographics = self.get_demographics(
            # we need to start the day after `latest_submitted_date` but this is easier
            start_date=latest_submitted_date,
            end_date=datetime.date.today(),
            county=county,
        )

        url = f"https://idph.illinois.gov/DPHPublicInformation/api/COVIDExport/GetCountyTestResultsTimeSeries?countyName={county}"
        print("Getting county data from {}".format(url))

        with closing(self.get(url, stream=True)) as r:
            daily_data = r.json()
            for data in daily_data:
                date = datetime.datetime.strptime(
                    data["ReportDate"], "%Y-%m-%dT%H:%M:%S"
                )
                date_str = date.strftime("%Y-%m-%d")
                if latest_submitted_date and date <= latest_submitted_date:
                    continue  # skip historical data we already have

                summary_location, summary_clinical = self.parse_county_data_for_date(
                    date_str, county, data
                )
                summary_clinical = {
                    **summary_clinical,
                    **demographics[date_str],  # add demographics data
                }

                self.summary_locations[
                    summary_location["submitter_id"]
                ] = summary_location
                self.summary_clinicals.append(summary_clinical)

    def parse_state_data(self, latest_submitted_date):
        """
        Parses historical state-level data. "summary_location" node is created
        from "characteristics_by_county" data.

        Args:
            latest_submitted_date (datetime): date for latest submitted date
        """
        url = "https://idph.illinois.gov/DPHPublicInformation/api/COVIDExport/GetIllinoisCases"
        print("Getting state data from {}".format(url))
        county = "Illinois"

        demographics = self.get_demographics(
            # we need to start the day after `latest_submitted_date` but this is easier
            start_date=latest_submitted_date,
            end_date=datetime.date.today(),
            county=county,
        )

        with closing(self.get(url, stream=True)) as r:
            daily_data = r.json()
            for illinois_data in daily_data:
                date = datetime.datetime.strptime(
                    illinois_data["testDate"], "%Y-%m-%dT%H:%M:%S"
                )
                date_str = date.strftime("%Y-%m-%d")
                if latest_submitted_date and date <= latest_submitted_date:
                    continue  # skip historical data we already have

                (
                    summary_location_submitter_id,
                    summary_clinical_submitter_id,
                ) = self.get_location_and_clinical_submitter_id(county, date_str)

                summary_clinical = {
                    "submitter_id": summary_clinical_submitter_id,
                    "date": date_str,
                    "confirmed": illinois_data["confirmed_cases"],
                    "testing": illinois_data["total_tested"],
                    "deaths": illinois_data["deaths"],
                    "summary_locations": [
                        {"submitter_id": summary_location_submitter_id}
                    ],
                    **demographics[date_str],  # add demographics data
                }

                self.summary_clinicals.append(summary_clinical)

    def parse_county_data_for_date(self, date_str, county, county_json):
        """
        From county-level data, generate the data we can submit via Sheepdog

        Args:
            date_str (str): date in "%Y-%m-%d" format
            county_json (dict): JSON for county statistics

        Returns:
            (dict, dict): "summary_location" and "summary_clinical" records
        """
        (
            summary_location_submitter_id,
            summary_clinical_submitter_id,
        ) = self.get_location_and_clinical_submitter_id(county, date_str)

        summary_location = {
            "submitter_id": summary_location_submitter_id,
            "country_region": self.country,
            "province_state": self.state,
            "projects": [{"code": self.project_code}],
            "county": county,
            "latitude": self.county_dict[county]["lat"],
            "longitude": self.county_dict[county]["lon"],
        }

        summary_clinical = {
            "submitter_id": summary_clinical_submitter_id,
            "date": date_str,
            "confirmed": county_json["CumulativeCases"],
            "testing": county_json["TotalTested"],
            "deaths": county_json["Deaths"],
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        return summary_location, summary_clinical

    def get_demographics(self, start_date, end_date, county):
        """
        Args:
            start_date (datetime): first time to fetch demographics data for
            end_date (datetime): last time to fetch demographics data for
            county (str): county name

        Returns:
            (dict): demographics values to add to "summary_clinical" records
        """
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        demographics = {}
        for _type, (field, mapping) in fields_mapping.items():
            type_name = _type.capitalize()
            url = f"https://idph.illinois.gov/DPHPublicInformation/api/COVIDExport/GetDemographics{type_name}?CountyName={county}&beginDate={start_date}&endDate={end_date}"
            print("Getting demographics data from {}".format(url))
            with closing(self.get(url, stream=True)) as r:
                data = r.json()
                for item in data:
                    date = item["ReportDate"].split("T")[0]
                    dst_field = mapping[item[field].strip()]
                    if not dst_field:
                        continue
                    # don't get "deaths" because it's always 0
                    for key in ["count", "tested"]:
                        if key in item:
                            count_field = f"{dst_field}_{key}"
                            if date not in demographics:
                                demographics[date] = {}
                            demographics[date][count_field] = item[key]
        return demographics

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
