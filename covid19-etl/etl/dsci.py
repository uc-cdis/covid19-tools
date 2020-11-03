import csv
from datetime import datetime
import os

from etl import base
from utils.format_helper import check_date_format
from utils.metadata_helper import MetadataHelper


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


def harmonize_gender(gender):
    return {"male": "Male", "female": "Female", "": "Not reported"}[gender]


def format_date(date):
    datet = datetime.strptime(date, "%d-%b-%y")
    return datet.strftime("%Y-%m-%d")


class DSCI(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "DSCI"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.subjects = []
        self.demographics = []
        self.observations = []

    def files_to_submissions(self):
        with open(
            os.path.join(CURRENT_DIR, "data/dsci_patient.csv"), newline=""
        ) as csvfile:
            reader = csv.reader(csvfile, delimiter=",", quotechar="|")
            header = next(reader)
            print("Headers:", header)
            header = {k: v for v, k in enumerate(header)}

            for row in reader:
                patient_id = row[header["patient_id"]].strip()

                # generate subject record
                subject = {
                    "submitter_id": patient_id,
                    "projects": [{"code": self.project_code}],
                }

                infected_by = row[header["contacted_with"]].strip()
                if infected_by:
                    subject["infected_by"] = list(
                        map(lambda v: v.strip(), infected_by.split(","))
                    )

                confirmed_date = row[header["confirmed_date"]].strip()
                if confirmed_date:
                    confirmed_date = format_date(confirmed_date)
                    check_date_format(confirmed_date)
                    subject["date_confirmation"] = confirmed_date
                    subject["covid_19_status"] = "Positive"

                deceased_date = row[header["deceased_date"]].strip()
                if deceased_date:
                    deceased_date = format_date(deceased_date)
                    check_date_format(deceased_date)
                    subject["deceased_date"] = deceased_date

                # generate demographic record
                demographic = {
                    "submitter_id": f"demographic_{patient_id}",
                    "subjects": {"submitter_id": f"{patient_id}"},
                }

                cols = {"age": "age", "province": "province_state"}

                for k, v in cols.items():
                    value = row[header[k]].strip()
                    if value:
                        demographic[v] = value

                if "age" in demographic:
                    demographic["age"] = int(demographic["age"])

                gender = row[header["gender"]].strip()
                demographic["gender"] = harmonize_gender(gender)

                nationality = row[header["nationality"]].strip()
                if nationality == "indonesia":
                    demographic["country_region"] = "Indonesia"
                elif nationality == "foreigner":
                    pass
                elif nationality:
                    raise Exception('Nationality "{}" is unknown'.format(nationality))

                # generate observation record
                observation = {
                    "submitter_id": f"observation_{patient_id}",
                    "subjects": {"submitter_id": f"{patient_id}"},
                }

                hospital = row[header["hospital"]].strip()
                if hospital:
                    observation["hospital"] = hospital

                state = row[header["current_state"]].strip()
                if state == "deceased":
                    subject["vital_status"] = "Dead"
                elif state == "isolated":
                    observation["isolation_status"] = "Isolated"
                elif state == "released":
                    observation["treatment_status"] = "Released"
                elif state:
                    raise Exception('State "{}" is unknown'.format(state))

                released_date = row[header["released_date"]].strip()
                if released_date:
                    released_date = format_date(released_date)
                    check_date_format(released_date)
                    observation["released_date"] = released_date

                self.subjects.append(subject)
                self.demographics.append(demographic)
                self.observations.append(observation)

    def submit_metadata(self):
        print("Submitting data")
        print("Submitting subject data")
        for loc in self.subjects:
            loc_record = {"type": "subject"}
            loc_record.update(loc)
            self.metadata_helper.add_record_to_submit(loc_record)
        self.metadata_helper.batch_submit_records()

        print("Submitting demographic data")
        for dem in self.demographics:
            dem_record = {"type": "demographic"}
            dem_record.update(dem)
            self.metadata_helper.add_record_to_submit(dem_record)
        self.metadata_helper.batch_submit_records()

        print("Submitting observation data")
        for obs in self.observations:
            obs_record = {"type": "observation"}
            obs_record.update(obs)
            self.metadata_helper.add_record_to_submit(obs_record)
        self.metadata_helper.batch_submit_records()
