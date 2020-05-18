import csv

from etl import base
from helper.metadata_helper import MetadataHelper


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

    def files_to_submissions(self):
        with open('etl/data/PatientInfo.csv', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            header = next(reader)
            print(header)
            header = {k: v for v, k in enumerate(header)}

            for row in reader:
                subject = {
                    "submitter_id": row[header["patient_id"]],
                    "projects": [{"code": self.project_code}],
                    "age_decade": row[header["age"]],
                    "province": row[header["province"]],
                    "city": row[header["city"]],
                    "infection_case": row[header["infection_case"]],
                    "date_onset_symptoms": row[header["symptom_onset_date"]],
                    "date_confirmation": row[header["confirmed_date"]],
                }

                country = row[header["country"]]
                if country == "Korea":
                    subject["country"] = "South Korea"
                elif country == "United States":
                    subject["country"] = "USA"
                else:
                    subject["country"] = country

                global_num = row[header["global_num"]]
                if global_num:
                    subject["global_num"] = int(global_num)

                infection_order = row[header["infection_order"]]
                if infection_order:
                    subject["infection_order"] = int(infection_order)

                infected_by = row[header["infected_by"]]
                if infected_by:
                    subject["infected_by"] = int(infected_by)

                contact_number = row[header["contact_number"]]
                if contact_number:
                    subject["contact_number"] = int(contact_number)

                disease = row[header["disease"]]
                if disease:
                    subject["disease"] = 'True'
                else:
                    subject["disease"] = 'False'

                released_date = row[header["released_date"]]
                if released_date:
                    subject["date_death_or_discharge"] = released_date

                deceased_date = row[header["deceased_date"]]
                if deceased_date:
                    subject["date_death_or_discharge"] = deceased_date

                state = row[header["state"]]
                if state == "isolated":
                    subject["isolated"] = "True"
                elif state == "released":
                    subject["recovered"] = "recovered"
                elif state == "deceased":
                    subject["death"] = "True"

                subject = {k: v if v else None for k, v in subject.items()}
                self.subjects.append(subject)

                demographic = {
                    "submitter_id": f'demographic_{row[header["patient_id"]]}',
                    "subjects": {
                        "submitter_id": row[header["patient_id"]]
                    },
                }

                gender = row[header["sex"]]
                if gender == "":
                    demographic["gender"] = "unspecified"
                else:
                    demographic["gender"] = gender

                year_of_birth = row[header["birth_year"]]
                if year_of_birth:
                    demographic["year_of_birth"] = int(year_of_birth)

                demographic = {k: v for k, v in demographic.items() if v}
                self.demographics.append(demographic)

    def submit_metadata(self):
        print("Submitting data")
        print("Submitting subject data")
        for loc in self.subjects:
            loc_record = {"type": "subject"}
            loc_record.update(loc)
            self.metadata_helper.add_record_to_submit(loc_record)
        self.metadata_helper.batch_submit_records()

        print("Submitting demographic data")
        for rep in self.demographics:
            rep_record = {"type": "demographic"}
            rep_record.update(rep)
            self.metadata_helper.add_record_to_submit(rep_record)
        self.metadata_helper.batch_submit_records()
