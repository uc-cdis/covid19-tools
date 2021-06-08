import os
import json

import requests

from etl.idph import IDPH
from utils.format_helper import (
    idph_get_date,
    derived_submitter_id,
)
from utils.metadata_helper import MetadataHelper

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

ROOT_URL = "https://idph.illinois.gov/DPHPublicInformation/api/covidVaccine/getVaccineAdministrationCurrent"
COUNTY_COVID_LINK_FORMAT = "https://idph.illinois.gov/DPHPublicInformation/api/covidVaccine/getVaccineAdministration?countyName={}"
COUNTY_DEMO_LINK_FORMAT = "https://idph.illinois.gov/DPHPublicInformation/api/covidvaccine/getVaccineAdministrationDemos?countyname={}"
TOTAL_VACCINE_LINK = "https://idph.illinois.gov/DPHPublicInformation/api/covidvaccine/getStatewideVaccineDetails"


def remove_time_from_date_time(str_datetime):
    datetime_parts = str_datetime.split("T")
    if len(datetime_parts) > 0:
        return datetime_parts[0]
    return str_datetime


class IDPH_VACCINE(IDPH):
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
        self.country = "US"
        self.state = "IL"
        self.date = ""
        self.counties_inventory = {}

        self.summary_locations = {}
        self.summary_clinicals = {}
        self.summary_group_demographic = {}

    def parse_list_of_counties(self):
        """
        Store into `self.date` the date the data was last updated, and
        into `self.counties_inventory` the data in format:
            { <county name>: { <county properties> } }
        """
        response = requests.get(ROOT_URL, headers={"content-type": "json"})
        json_response = json.loads(response.text)
        self.date = idph_get_date(json_response.get("lastUpdatedDate"))
        print(f"Dataset's last updated date: {self.date}")
        root_json = json_response.get("VaccineAdministration")
        if root_json is None:
            return
        for item in root_json:
            county = item.get("CountyName")
            self.counties_inventory[county] = item

    def files_to_submissions(self):
        """
        Reads JSON file and convert the data to Sheepdog records
        """

        # latest_submitted_date = (
        #     self.metadata_helper.get_latest_submitted_date_idph()
        # )
        # if latest_submitted_date != None and latest_submitted_date == self.date:
        #     print(
        #         "Nothing to submit: data of latest submitted date and IDPH are the same."
        #     )
        #     return

        self.parse_link()

    def get_group_clinical_demographic_submitter_id(
        self, summary_clinical_submitter_id, key_dict
    ):
        summary_group_demographic_submitter_id = derived_submitter_id(
            summary_clinical_submitter_id,
            "summary_clinical",
            "summary_group_demographic",
            key_dict,
        )
        return summary_group_demographic_submitter_id

    def map_race(self, value, prop_name):
        race_mapping = {
            "Black or African-American": "Black",
            "Other race": "Other",
            "Native Hawaiian or Other Pacif": "Native Hawaiian or Other Pacific Islander",
            "American Indian or Alaska Nati": "American Indian or Alaska Native",
            "Hispanic or Latino": "Hispanic",
        }
        gender_mapping = {
            "Unknown": "Unknown or Left Blank",
        }
        age_group_mapping = {"65+": "greater than 65"}
        if prop_name == "Race" and value in race_mapping:
            return race_mapping.get(value)
        if prop_name == "Gender" and value in gender_mapping:
            return gender_mapping.get(value)
        if prop_name == "AgeGroup" and value in age_group_mapping:
            return age_group_mapping.get(value)
        return value

    def parse_group_clinical_demographic(self, props_mapping, props_value):
        key_props_name = ["AgeGroup", "Race", "Gender"]
        key_props = {}
        for k in key_props_name:
            key = props_mapping.get(k)
            if key is not None:
                key_props[key] = props_value.get(k)
        props_data = {}
        for (k, v) in props_mapping.items():
            if k in props_value:
                if k in key_props_name:
                    value = props_value.get(k)
                    if k == "AgeGroup":
                        value = value.replace("-", " to ")
                    props_data[v] = self.map_race(value, k)
                else:
                    props_data[v] = props_value.get(k)
        return key_props, props_data

    def parse_link(self):
        """
        Converts the source data to data we can submit via Sheepdog. Stores
        the records to submit in `self.summary_locations`,
        `self.summary_clinicals` and `self.summary_group_demographic`.
        """
        illinois_summary_clinical_submitter_id = self.parse_county_data()
        self.parse_total_state_wide(illinois_summary_clinical_submitter_id)

    def parse_county_data(self):
        """
        For each county, converts the raw data into Sheepdog submissions by
        mapping properties to match the PRC data dictionary.
        Return the `submitter_id` for the state-wide `summary_clinical` record.
        """
        county_vaccine_mapping = {
            "AdministeredCount": "vaccine_administered_count",
            "AdministeredCountChange": "vaccine_administered_count_change",
            "AdministeredCountRollAvg": "vaccine_administered_count_roll_avg",
            "PersonsFullyVaccinated": "vaccine_persons_fully_vaccinated",
            "Report_Date": "date",
            "PctVaccinatedPopulation": "vaccine_persons_fully_vaccinated_pct",
        }

        county_demo_mapping = {
            "AgeGroup": "age_group",
            "Race": "race",
            "Gender": "gender",
            "AdministeredCount": "vaccine_administered_count",
            "PersonsFullyVaccinated": "vaccine_persons_fully_vaccinated",
        }

        inventory_reported = {
            "LHDReportedInventory": "vaccine_LHDR_reported_inventory",
            "CommunityReportedInventory": "vaccine_community_reported_inventory",
            "TotalReportedInventory": "vaccine_reported_inventory",
            "InventoryReportDate": "date",
        }

        self.parse_list_of_counties()
        illinois_summary_clinical_submitter_id = ""
        for county in self.counties_inventory:
            county_covid_response = requests.get(
                COUNTY_COVID_LINK_FORMAT.format(county),
                headers={"content-type": "json"},
            )
            county_covid_data = json.loads(county_covid_response.text).get(
                "CurrentVaccineAdministration"
            )
            county_demo_response = requests.get(
                COUNTY_DEMO_LINK_FORMAT.format(county), headers={"content-type": "json"}
            )
            county_demo_data = json.loads(county_demo_response.text)

            (
                summary_location_submitter_id,
                summary_clinical_submitter_id,
            ) = self.get_location_and_clinical_submitter_id(county, self.date)
            if county.lower() == "illinois":
                illinois_summary_clinical_submitter_id = summary_clinical_submitter_id

            for k in ["Age", "Race", "Gender"]:
                data = county_demo_data.get(k)
                for item in data:
                    keys, props = self.parse_group_clinical_demographic(
                        county_demo_mapping, item
                    )
                    group_demographics_submitter_id = (
                        self.get_group_clinical_demographic_submitter_id(
                            summary_clinical_submitter_id, keys
                        )
                    )
                    props["submitter_id"] = group_demographics_submitter_id
                    props["summary_clinicals"] = [
                        {"submitter_id": summary_clinical_submitter_id}
                    ]
                    self.summary_group_demographic[
                        group_demographics_submitter_id
                    ] = props

            summary_location = {
                "country_region": self.country,
                "submitter_id": summary_location_submitter_id,
                "projects": [{"code": self.project_code}],
                "province_state": self.state,
                "county": county,
            }
            summary_clinical = {
                "submitter_id": summary_clinical_submitter_id,
                "date": self.date,
                "summary_locations": [{"submitter_id": summary_location_submitter_id}],
            }
            for (key, value) in county_vaccine_mapping.items():
                if value == "vaccine_persons_fully_vaccinated_pct":
                    summary_clinical[value] = int(county_covid_data.get(key) * 100)
                elif value == "vaccine_administered_count_roll_avg":
                    summary_clinical[value] = int(county_covid_data.get(key))
                elif value == "date":
                    summary_clinical[value] = remove_time_from_date_time(
                        county_covid_data.get(key)
                    )
                else:
                    summary_clinical[value] = county_covid_data.get(key)
            # for (key, value) in county_demo_mapping.items():
            #     summary_clinical[value] = county_demo_data.get(key)
            for (key, value) in inventory_reported.items():
                summary_clinical[value] = (
                    self.counties_inventory[county].get(key)
                    if value != "date"
                    else remove_time_from_date_time(
                        self.counties_inventory[county].get(key)
                    )
                )

            self.summary_locations[summary_location_submitter_id] = summary_location
            self.summary_clinicals[summary_clinical_submitter_id] = summary_clinical
        return illinois_summary_clinical_submitter_id

    def parse_total_state_wide(self, state_summary_clinical_submitter_id):
        """
        Parse the Illinois total stats
        """
        county_covid_response = requests.get(
            TOTAL_VACCINE_LINK, headers={"content-type": "json"}
        )
        state_total_data = json.loads(county_covid_response.text)
        total_vaccine_mapping = {
            "Total_Delivered": "vaccine_total_delivered_vaccine_doses",
            "Total_Administered": "vaccine_IL_total_administered_vaccine_doses",
            "Persons_Fully_Vaccinated": "vaccine_IL_total_persons_fully_vaccinated",
            "LTC_Allocated": "vaccine_long_term_care_allocated",
            "LTC_Administered": "vaccine_long_term_care_administered",
            "Report_Date": "date",
        }
        for (key, value) in total_vaccine_mapping.items():
            if value != "date":
                self.summary_clinicals[state_summary_clinical_submitter_id][
                    value
                ] = state_total_data.get(key)
            else:
                self.summary_clinicals[state_summary_clinical_submitter_id][
                    value
                ] = remove_time_from_date_time(state_total_data.get(key))

    def submit_metadata(self):
        print("Submitting data...")
        print("Submitting summary_location data")
        for sl in self.summary_locations.values():
            sl_record = {"type": "summary_location"}
            sl_record.update(sl)
            self.metadata_helper.add_record_to_submit(sl_record)
        self.metadata_helper.batch_submit_records()

        print("Submitting summary_clinical data")
        for sc in self.summary_clinicals.values():
            sc_record = {"type": "summary_clinical"}
            sc_record.update(sc)
            self.metadata_helper.add_record_to_submit(sc_record)
        self.metadata_helper.batch_submit_records()

        print("Submitting summary_group_demographic data")
        for sc in self.summary_group_demographic.values():
            sc_record = {"type": "summary_group_demographics"}
            sc_record.update(sc)
            self.metadata_helper.add_record_to_submit(sc_record)
        self.metadata_helper.batch_submit_records()
