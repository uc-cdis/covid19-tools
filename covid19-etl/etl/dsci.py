import csv

from etl import base
from helper.metadata_helper import MetadataHelper


class DS4C(base.BaseETL):
    def __init__(self, base_url, access_token):
        super().__init__(base_url, access_token)

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

    def files_to_submissions(self):
        with open('etl/data/patient.csv', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='|')
            header = next(reader)
            print(header)
            header = {k: v for v, k in enumerate(header)}

            for row in reader:
                print(', '.join(row))

                subject = {
                    "submitter_id": f'subject_{row[header["patient_id"]]}',
                    "projects": [{"code": self.project_code}],
                    "date_confirmation": row[header["confirmed_date"]],
                }

                cols = {'age': 'age',
                        'province': 'province',
                        'current_state': 'current_state',
                        'contacted_with': 'infected_by',
                        'released_date': 'released_date',
                        'deceased_date': 'date_death_or_discharge',
                        'hospital': 'hospital'}

                for k, v in cols.items():
                    value = row[header[k]]
                    if value:
                        subject[v] = value

                if 'age' in subject:
                    subject['age'] = int(subject['age'])

                if 'infected_by' in subject:
                    subject['infected_by'] = int(subject['infected_by'])

                self.subjects.append(subject)

                demographic = {
                    "submitter_id": f'demographic_{row[header["patient_id"]]}',
                    "subjects": {
                        "submitter_id": f'subject_{row[header["patient_id"]]}'
                    },
                }

                cols = {'gender': 'gender',
                        'nationality': 'nationality', }

                for k, v in cols.items():
                    value = row[header[k]]
                    if value:
                        demographic[v] = value

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
