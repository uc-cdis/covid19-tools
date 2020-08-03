import csv
import os

from etl import base
from helper.format_helper import check_date_format
from helper.metadata_helper import MetadataHelper


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


def harmonize_gender(gender):
    {"male": "Male", "female": "Female", "": "Not reported"}[gender]


class DS4C(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "DS4C"
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
            os.path.join(CURRENT_DIR, "data/ds4c_PatientInfo.csv"), newline=""
        ) as csvfile:
            reader = csv.reader(csvfile, delimiter=",")
            header = next(reader)
            print("Headers:", header)
            header = {k: v for v, k in enumerate(header)}
            n_1200012238 = 1

            for row in reader:
                patient_id = row[header["patient_id"]].strip()
                if patient_id == "1200012238":
                    # there are 2 rows for the same ID
                    patient_id = f"{patient_id}_{n_1200012238}"
                    n_1200012238 += 1

                # generate subject record
                subject = {
                    "submitter_id": patient_id,
                    "projects": [{"code": self.project_code}],
                }

                confirmed_date = row[header["confirmed_date"]].strip()
                if confirmed_date:
                    check_date_format(confirmed_date)
                    subject["date_confirmation"] = confirmed_date
                    subject["covid_19_status"] = "Positive"

                infected_by = row[header["infected_by"]].strip()
                if infected_by:
                    subject["infected_by"] = list(
                        map(lambda v: v.strip(), infected_by.split(","))
                    )

                deceased_date = row[header["deceased_date"]].strip()
                if deceased_date:
                    check_date_format(deceased_date)
                    subject["deceased_date"] = deceased_date

                # generate demographic record
                demographic = {
                    "submitter_id": f"demographic_{patient_id}",
                    "subjects": {"submitter_id": patient_id},
                    "age_decade": row[header["age"]].strip(),
                    "province_state": row[header["province"]].strip(),
                    "city": row[header["city"]].strip(),
                }

                country = row[header["country"]].strip()
                if country == "Korea":
                    demographic["country_region"] = "South Korea"
                elif country == "United States":
                    demographic["country_region"] = "USA"
                else:
                    demographic["country_region"] = country

                gender = row[header["sex"]].strip()
                demographic["gender"] = harmonize_gender(gender)

                demographic["year_of_birth"] = None

                # generate observation record
                observation = {
                    "submitter_id": f"observation_{patient_id}",
                    "subjects": {"submitter_id": patient_id},
                    "exposure": row[header["infection_case"]].strip(),
                }
                date_onset_symptoms = row[header["symptom_onset_date"]].strip()
                if date_onset_symptoms:
                    check_date_format(row[header["symptom_onset_date"]])
                    observation["date_onset_symptoms"] = date_onset_symptoms

                state = row[header["state"]].strip()
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
                    check_date_format(released_date)
                    observation["released_date"] = released_date

                subject = {k: v if v else None for k, v in subject.items()}
                self.subjects.append(subject)

                demographic = {k: v for k, v in demographic.items() if v}
                self.demographics.append(demographic)

                observation = {k: v for k, v in observation.items() if v}
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
