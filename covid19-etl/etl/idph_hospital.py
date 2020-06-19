import datetime
import os
import re
from contextlib import closing

import requests

from etl import base
from helper.format_helper import (
    derived_submitter_id,
    format_submitter_id,
    idph_get_date,
)
from helper.metadata_helper import MetadataHelper

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


class IDPH_HOSPITAL(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "IDPH-Hospital"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.country = "US"
        self.state = "IL"

        self.summary_locations = []
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
        today_str = today.strftime("%Y%m%d")

        print(f"Getting data for date: {today_str}")
        url = "https://dph.illinois.gov/sitefiles/COVIDHospitalRegions.json"
        self.parse_file(latest_submitted_date, url)

    def parse_file(self, latest_submitted_date, url):
        """
        Converts a JSON files to data we can submit via Sheepdog. Stores the 
        records to submit in `self.summary_locations` and `self.summary_clinicals`.

        Args:
            latest_submitted_date (date): the date of latest available "summary_clinical" for project
            url (str): URL at which the JSON file is available
        """
        print("Getting data from {}".format(url))
        with closing(requests.get(url, stream=True)) as r:
            data = r.json()
            date = idph_get_date(data["LastUpdateDate"])

            if latest_submitted_date and date == latest_submitted_date.strftime(
                "%Y-%m-%d"
            ):
                print(
                    "Nothing to submit: latest submitted date and date from data are the same."
                )
                return

            (summary_location, summary_clinical_statewide_current,) = self.parse_statewide_values(
                date, data["statewideValues"]
            )

            self.summary_locations.append(summary_location)

            for utilization in data["HospitalUtilizationResults"]:
                summary_clinical = self.parse_historical(
                    utilization, summary_clinical_statewide_current
                )

                self.summary_clinicals.append(summary_clinical)

            for region in data["regionValues"]:
                (summary_location, summary_clinical,) = self.parse_region(date, region)

                self.summary_locations.append(summary_location)
                self.summary_clinicals.append(summary_clinical)

            for sl in self.summary_locations:
                print(sl)
            for sc in self.summary_clinicals:
                print(sc)

    def parse_historical(self, utilization, summary_clinical_statewide_current):
        utilization_mapping = {
            "reportDate": "date",
            "TotalBeds": "state_total_beds",
            "TotalOpenBeds": "total_open_beds",
            "TotalInUseBedsNonCOVID": "total_in_use_beds_non_covid",
            "TotalInUseBedsCOVID": "total_in_use_beds_covid",
            "ICUBeds": "icu_beds",
            "ICUOpenBeds": "icu_open_beds",
            "ICUInUseBedsNonCOVID": "icu_in_use_beds_non_covid",
            "ICUInUseBedsCOVID": "icu_in_use_beds_covid",
            "VentilatorCapacity": "ventilator_capacity",
            "VentilatorAvailable": "ventilator_available",
            "VentilatorInUseNonCOVID": "ventilator_in_use_non_covid",
            "VentilatorInUseCOVID": "ventilator_in_use_covid",
        }
        date = utilization["reportDate"]

        summary_location_submitter_id = format_submitter_id(
            "summary_location",
            {
                "project": "idph_hospital",
                "country": self.country,
                "state": self.state,
            },
        )

        summary_clinical_submitter_id = derived_submitter_id(
            summary_location_submitter_id,
            "summary_location",
            "summary_clinical",
            {"project": "idph_hospital", "date": date},
        )

        summary_clinical = {
            "submitter_id": summary_clinical_submitter_id,
            "date": date,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        for k, v in utilization.items():
            summary_clinical[utilization_mapping[k]] = v

        if summary_clinical_submitter_id == summary_clinical_statewide_current["submitter_id"]:
            summary_clinical.update(summary_clinical_statewide_current)

        return summary_clinical

    def parse_statewide_values(self, date, statewide_values):
        statewide_mapping = {
            "ICUCapacity": "state_icu_capacity",
            "ICUCovidPatients": "state_icu_covid_patients",
            "VentCapacity": "state_vent_capacity",
            "VentCovidPatients": "state_vent_covid_patients",
            "ICUAvailable": "state_icu_available",
            "VentsAvailable": "state_vents_available",
            "TotalBeds": "state_total_beds",
            "TotalBedsAvailable": "state_total_beds_available",
            "TotalBedsUsed": "state_total_beds_used",
            "PctHospitalBedsAvailable": "state_pct_hospital_beds_available",
            "AdultICUCapacity": "state_adult_icu_capacity",
            "ICUOpenBeds": "state_icu_open_beds",
            "ICUBedsUsed": "state_icu_beds_used",
            "ICUOpenBedsPct": "state_icu_open_beds_pct",
            "COVIDPUIPatients": "state_covid_pui_patients",
            "COVIDPUIPatientsPct": "state_covid_pui_patients_pct",
            "COVIDPUIPatientsBedsInUsePct": "state_covid_pui_patients_beds_in_use_pct",
            "VentilatorCapacity": "state_ventilator_capacity",
            "VentilatorsOpen": "state_ventilators_open",
            "VentilatorsOpenPct": "state_Ventilators_open_pct",
            "VentilatorsInUse": "state_ventilators_in_use",
            "VentilatorsInUseCOVID": "state_ventilators_in_use_covid",
            "VentilatorsCOVIDPatientsPct": "state_ventilators_covid_patients_pct",
            "VentilatorsCOVIDPatientsInUsePct": "state_ventilators_covid_patients_in_use_pct",
            "CovidPatientsNonICU": "state_covid_patients_non_icu",
            "TotalCOVIDPUIInICU": "state_total_covid_pui_in_icu",
            "TotalCOVIDPUIInHospital": "state_total_covid_pui_in_hospital",
            "PctBedsCOVIDPUI": "state_pct_beds_covid_pui",
            "MedSurgBeds": "state_med_surg_beds",
            "MedSurgBedsOpen": "state_med_surg_beds_open",
            "MedSurgBedsOpenPct": "state_med_surg_beds_open_pct",
            "MedSurgBedsInUse": "state_med_surg_beds_in_use",
        }

        summary_location_submitter_id = format_submitter_id(
            "summary_location",
            {
                "project": "idph_hospital",
                "country": self.country,
                "state": self.state,
            },
        )

        summary_location = {
            "submitter_id": summary_location_submitter_id,
            "projects": [{"code": self.project_code}],
            "country_region": self.country,
            "province_state": self.state,
        }

        summary_clinical_submitter_id = derived_submitter_id(
            summary_location_submitter_id,
            "summary_location",
            "summary_clinical",
            {"project": "idph_hospital", "date": date},
        )

        summary_clinical = {
            "submitter_id": summary_clinical_submitter_id,
            "date": date,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        for k, v in statewide_values.items():
            summary_clinical[statewide_mapping[k]] = v

        return summary_location, summary_clinical

    def parse_region(self, date, hospital_region):
        """
        From county-level data, generate the data we can submit via Sheepdog
        """
        region = hospital_region["region"]
        region_id = hospital_region["id"]
        region_description = hospital_region["region_description"]

        summary_location_submitter_id = format_submitter_id(
            "summary_location",
            {
                "project": "idph_hospital",
                "country": self.country,
                "state": self.state,
                "region": region,
            },
        )

        summary_location = {
            "country_region": self.country,
            "submitter_id": summary_location_submitter_id,
            "projects": [{"code": self.project_code}],
            "province_state": self.state,
            "state_hospital_region": region,
            "state_region_description": region_description,
        }

        summary_clinical_submitter_id = derived_submitter_id(
            summary_location_submitter_id,
            "summary_location",
            "summary_clinical",
            {"project": "idph_hospital", "date": date},
        )

        summary_clinical = {
            "submitter_id": summary_clinical_submitter_id,
            "date": date,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
            "region_icu_avail": hospital_region["ICUAvail"],
            "region_icu_capacity": hospital_region["ICUCapacity"],
            "region_vents_available": hospital_region["VentsAvailable"],
            "region_vents_capacity": hospital_region["VentsCapacity"],
        }

        return summary_location, summary_clinical

    def submit_metadata(self):
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
