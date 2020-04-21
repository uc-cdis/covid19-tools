import csv
import os

from metadata_helper import MetadataHelper

if __name__ == "__main__":
    base_url = "https://chicagoland.pandemicresponsecommons.org"
    program_name = "open"
    project_code = "DS4C"
    token = os.environ.get("ACCESS_TOKEN")
    if not token:
        raise Exception(
            "Need ACCESS_TOKEN environment variable (token for user with read and write access)"
        )
    metadata_helper = MetadataHelper(base_url, program_name=program_name, project_code=project_code, access_token=token)

    with open('PatientInfo.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        header = next(reader)
        print(header)
        header = {k: v for v, k in enumerate(header)}

        for row in reader:
            # print(', '.join(row))

            subject = {
                "submitter_id": row[header["patient_id"]],
                "projects": [{"code": project_code}],
                "age_decade": row[header["age"]],
                "country": row[header["country"]],
                "province": row[header["province"]],
                "city": row[header["city"]],
                "infection_case": row[header["infection_case"]],
                "date_onset_symptoms": row[header["symptom_onset_date"]],
                "date_confirmation": row[header["confirmed_date"]],
            }

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
            deceased_date = row[header["deceased_date"]]

            if released_date:
                subject["date_death_or_discharge"] = released_date
            if deceased_date:
                subject["date_death_or_discharge"] = deceased_date

            state = row[header["state"]]
            if state == "isolated":
                subject["isolated"] = "True"
            elif state == "released":
                subject["recovered"] = "recovered"
            elif state == "deceased":
                subject["death"] = "True"

            # print(subject)

            subject_record = {"type": "subject"}
            subject_record.update(subject)
            metadata_helper.add_record_to_submit(subject_record)

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

            # print(demographic)

            demographic_record = {"type": "demographic"}
            demographic_record.update(demographic)
            metadata_helper.add_record_to_submit(demographic_record)

            # break

        metadata_helper.batch_submit_records()
