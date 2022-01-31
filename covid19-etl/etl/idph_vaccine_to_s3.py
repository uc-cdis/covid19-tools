"""
    This ETL must run _AFTER_ the IDPH_VACCINE ETL completes. It uses the
    data the IDPH_VACCINE ETL submits to Sheepdog.

    Push the following data file to S3:
    {
        # IL only
        "il_county_list": {
            <US county FIPS>: {
                "county": <county name>,
                "by_date": {
                    "<date>": <vaccine_persons_fully_vaccinated>,
                    ...
                },
                ...
            },
            ...
        },
        # Date data was last updated
        "last_updated": "2020-04-20",
        # Total number of vaccinated people
        "total": 0
    }
"""

import boto3
from datetime import datetime
import json
import os

from etl import base
from etl.jhu_to_s3 import MAP_DATA_FOLDER
from utils.country_codes_utils import get_county_to_fips_dictionary
from utils.metadata_helper import MetadataHelper


VACCINES_BY_COUNTY_BY_DATE_FILENAME = "vaccines_by_county_by_date.json"

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


class IDPH_VACCINE_TO_S3(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "IDPH-Vaccine"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )
        self.s3_client = boto3.client("s3")

    def get_existing_data_from_s3(self):
        s3_path = os.path.join(MAP_DATA_FOLDER, VACCINES_BY_COUNTY_BY_DATE_FILENAME)
        bucket = self.s3_bucket.split("s3://")[-1]
        try:
            res = self.s3_client.get_object(Bucket=bucket, Key=s3_path)
            return json.loads(res["Body"].read().decode("utf-8"))
        except Exception as e:
            print(
                f"WARNING: Unable to get existing data from S3. Will get all data from Peregrine instead. Details: {e}"
            )

        # return default empty value
        # IL only - this dataset is only for IL anyway
        return {"il_county_list": {}, "last_updated": None}

    def get_new_data_from_peregrine(self, days_since_last_update):
        """
        Query Peregrine for the vaccine data that is not yet in S3.
        Filter country=US and state=IL to be safe even if the IDPH-Vaccine
        project only contains IL data anyway.
        """
        first = str(days_since_last_update or 0)  # first=0 means all data
        query_string = (
            '{ summary_location (first: 0, project_id: "'
            + self.program_name
            + "-"
            + self.project_code
            + '", country_region: "US", province_state: "IL") {'
            + "county, summary_clinicals (first: "
            + first
            + ', order_by_desc: "date") {'
            + "date, vaccine_persons_fully_vaccinated } } }"
        )
        try:
            response = self.metadata_helper.query_peregrine(query_string)
            return response["data"]
        except Exception as ex:
            print(f"Unable to query peregrine. Detail {ex}")
            raise

    def get_last_updated_date(self, summary_clinicals):
        """
        Return the most recent date found in a list of `summary_clinical` records.
        """
        last_updated_date = None
        for record in summary_clinicals:
            # remove time from some early dates in the dataset
            date = record["date"].split("T")[0]
            if not last_updated_date or datetime.strptime(
                date, "%Y-%m-%d"
            ) > datetime.strptime(last_updated_date, "%Y-%m-%d"):
                last_updated_date = date
        print(f"Dataset last updated date: {last_updated_date}")
        return last_updated_date

    def format_result(self, county_to_fips_dict, existing_data, new_data):
        """
        Parse new data from Peregrine and add it to the existing data.

        Args:
        - `county_to_fips_dict`
        - `existing_data` format: see data file format at the top of this file
        - `new_data`: data from Peregrine, in format:
            {
                summary_location: [
                    {
                        county: <county name>,
                        summary_clinicals: [
                            {
                                date: <str>,
                                vaccine_persons_fully_vaccinated: <int>
                            },
                            ...
                        ]
                    },
                    ...
                ]
            }
        """
        # the date at which this data was last updated
        existing_data["last_updated"] = self.get_last_updated_date(
            new_data["summary_location"][0]["summary_clinicals"]
        )

        chicago_data_by_date = None
        for location in new_data["summary_location"]:
            county = location["county"]

            # get the total count
            if county == "Illinois":
                for record in location["summary_clinicals"]:
                    date = record["date"].split("T")[0]
                    if date == existing_data["last_updated"]:
                        existing_data["total"] = record[
                            "vaccine_persons_fully_vaccinated"
                        ]
                continue

            fips = county_to_fips_dict.get(county)
            if not fips:
                if county in ["Unknown", "Out Of State", "Chicago"]:
                    # we expect no FIPS for these, use the name as identifier
                    fips = county
                else:
                    raise Exception(
                        f"Uh-oh, did not find FIPS code for county '{county}'"
                    )

            data_by_date = {}
            for record in location["summary_clinicals"]:
                # remove time from some early dates in the dataset
                date = record["date"].split("T")[0]
                data_by_date[date] = record["vaccine_persons_fully_vaccinated"]

            if county == "Chicago":
                # the Chicago data are processed later
                chicago_data_by_date = data_by_date
            else:
                if fips in existing_data["il_county_list"]:
                    # merge existing data and new data
                    data_by_date = dict(
                        data_by_date, **existing_data["il_county_list"][fips]["by_date"]
                    )
                existing_data["il_county_list"][fips] = {
                    "county": county,
                    "by_date": data_by_date,
                }

        # we don't separate Chicago from Cook county on the frontend,
        # so add the Chicago counts to the Cook county counts.
        for data in existing_data["il_county_list"].values():
            if data["county"] == "Cook":
                for date in data["by_date"]:
                    # ignore dates that are in Chicago data but not Cook data;
                    # counts without the rest of Cook county would look weird
                    if date in chicago_data_by_date:
                        data["by_date"][date] += chicago_data_by_date[date]
                break

        return existing_data

    def files_to_submissions(self):
        """
        Get the existing vaccine data from S3, query Peregrine for any new
        data, and create an updated JSON file with existing + new data.
        """
        existing_data = self.get_existing_data_from_s3()
        last_updated_date = existing_data["last_updated"]
        if last_updated_date:
            last_updated_date = datetime.strptime(last_updated_date, "%Y-%m-%d")
            days_since_last_update = (datetime.now() - last_updated_date).days
            print(
                f"Data in S3 up to {last_updated_date}; querying Peregrine for the last {days_since_last_update} days of data"
            )
        else:
            days_since_last_update = None

        if days_since_last_update == 0:
            print("Zero days since last update: nothing to do")
            return

        new_data = self.get_new_data_from_peregrine(days_since_last_update)
        county_to_fips_dict = get_county_to_fips_dictionary()
        result = self.format_result(county_to_fips_dict, existing_data, new_data)

        # save to local
        with open(
            os.path.join(CURRENT_DIR, VACCINES_BY_COUNTY_BY_DATE_FILENAME), "w"
        ) as f:
            f.write(
                json.dumps(
                    result,
                    separators=(",", ":"),
                )
            )

    def submit_metadata(self):
        abs_path = os.path.join(CURRENT_DIR, VACCINES_BY_COUNTY_BY_DATE_FILENAME)
        s3_path = os.path.join(MAP_DATA_FOLDER, VACCINES_BY_COUNTY_BY_DATE_FILENAME)
        print(f"Uploading file to S3 at '{s3_path}'")
        self.s3_client.upload_file(
            Filename=abs_path, Bucket=self.s3_bucket, Key=s3_path
        )
        os.remove(abs_path)
