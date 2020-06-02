import copy
import csv
import datetime
import re
from contextlib import closing

import requests

from etl import base
from helper.metadata_helper import MetadataHelper


def format_subject_submitter_id(country, submitter_id):
    submitter_id = "subject_dsfsi_{}_{}".format(country.lower(), submitter_id)
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)
    return submitter_id


def format_demographic_submitter_id(subject_submitter_id):
    return "{}".format(
        subject_submitter_id.replace("subject_", "demographic_")
    )


def normalize_current_status(status):
    normalized = {
        "?": None,
        "Alive": "alive",
        "Clinically Stable": "stable",
        "critical condition": "critical",
        "Critical condition": "critical",
        "Dead": "deceased",
        "Death": "deceased",
        "Deceased": "deceased",
        "deceased": "deceased",
        "Died": "deceased",
        "In recovery": "in recovery",
        "In treatment": "in treatment",
        "NA": None,
        "Pos": "positive",
        "Receiving Treatment": "in treatment",
        "Recovered": "recovered",
        "recovered": "recovered",
        "Recovered (3/19/2020)": "recovered",
        "Stable": "stable",
        " Stable": "stable",
        "stable condition": "stable",
        "Tested Negative and hence is not considered as a case anymore": None,
        "Under treatment": "in treatment",
        "unstable": "unstable",
    }

    return normalized[status]


def normalize_symptoms(symptoms):
    normalized = {
        "acute pneumonia": "pneumonia",
        "Asymptomatic": "asymptomatic",
        "breathing complications": "trouble breathing",
        "chest pain": "persistent pain or pressure in the chest",
        "cold": "fever* or feeling feverish/chills",
        "Cold": "fever* or feeling feverish/chills",
        "cough": "cough",
        "Cough": "cough",
        "Cough Fever": "cough",
        "COVID-19 related symptoms": "COVID-19 related symptoms",
        "fever": "fever or feeling feverish/chills",
        "Fever": "fever or feeling feverish/chills",
        "fever and aches and headache": "fever or feeling feverish/chills",
        "Mild to moderate": "mild to moderate",
        "muscle pain": "muscle or body aches",
        "NA": "no data",
        "rhinorrhea": "runny or stuffy nose",
        "Severe": "severe",
        "sneezing": "sneezing",
        "tired": "fatigue (tiredness)",
    }

    list_of_symptoms = re.split(r', | and ', symptoms)

    result = []
    for symptom in list_of_symptoms:
        if symptom.strip() in normalized:
            result.append(normalized[symptom.strip()])
    return result


def normalize_date(d):
    if not d:
        return None

    parsed_date = None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%d-%m-%y", "%m/%d/%Y", "%d/%m/%Y", "%d-%b-%y"):
        try:
            parsed_date = datetime.datetime.strptime(d, fmt)
        except ValueError:
            pass

    if not parsed_date:
        return None

    return parsed_date.strftime("%m/%d/%y")


def normalize_condition(condition):
    normalized = {
        "NA": None,
        "was treated for unspecified underlying condition": None,
    }

    norm = normalized.get(condition, condition)
    if norm == condition:
        return [norm]

    return None


def normalize_gender(gender):
    normalized = {
        "male": "male",
        "male ?": "male",
        "m": "male",
        "female": "female",
        "f": "female",
        "woman": "female",
        "not specified": "unspecified",
        "na": None,
        "x": None,
        "?": None,
        "suleja": None,
    }

    return normalized[gender.lower()]


def normalize_age(age):
    if age == "NA":
        return None

    if age:
        try:
            return int(float(age))
        except ValueError:
            return None

    return None


class DSFSI(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.subjects = []
        self.demographics = []

        self.program_name = "open"
        self.project_code = "DSFSI"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        # structure is
        # (csv field name, (node type, node field name, type of field))
        self.countries_fields = [
            ("case_id", ("subject", "submitter_id", str)),
            ("origin_case_id", (None, None, None)),
            ("date", ("subject", "reporting_date", normalize_date)),
            ("age", ("subject", "age", normalize_age)),
            ("gender", ("demographic", "gender", normalize_gender)),
            ("city", ("subject", "city", str)),
            ("province/state", ("subject", "province", str)),
            ("country", ("subject", "country", str)),
            ("current_status", ("subject", "current_state", normalize_current_status)),
            ("source", ("subject", "source", str)),
            ("symptoms", ("subject", "symptoms", normalize_symptoms)),
            ("date_onset_symptoms", ("subject", "date_onset_symptoms", normalize_date)),
            ("date_admission_hospital", ("subject", "date_admission_hospital", normalize_date)),
            ("date_confirmation", ("subject", "date_confirmation", normalize_date)),
            ("underlying_conditions", ("subject", "underlying_conditions", normalize_condition)),
            ("travel_history_dates", ("subject", "travel_history_dates", list)),
            ("travel_history_location", ("subject", "travel_history_location", list)),
            ("death_date", ("subject", "date_death_or_discharge", normalize_date)),
            ("notes_for_discussion", ("subject", "additional_information", str)),
        ]

    def files_to_submissions(self):
        """
        Reads CSV files and converts the data to Sheepdog records
        """
        urls = {
            "Algeria": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-algeria.csv",
            "Angola": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-angola.csv",
            "Benin": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-benin.csv",
            "Burkina Faso": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-burkina-faso.csv",
            "Cabo Verde": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-cabo-verde.csv",
            "Cameroon": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-cameroon.csv",
            "Central African Republic": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-central-african-republic.csv",
            "Chad": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-chad.csv",
            "Côte d'Ivoire": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-cote-divoire.csv",
            "Democratic Republic of the Congo": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-democratic-republic-of-the-congo.csv",
            "Djibouti": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-djibouti.csv",
            # here should be an Egypt dataset, but it's not useful and omitted on purpose
            "Equatorial Guinea": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-equatorial-guinea.csv",
            "Eritrea": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-eritrea.csv",
            "Eswatini": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-eswatini.csv",
            "Ethiopia": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-ethiopia.csv",
            "Gabon": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-gabon.csv",
            "Gambia": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-gambia.csv",
            "Ghana": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-ghana.csv",
            "Guinea Bissau": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-guinea-bissau.csv",
            "Guinea": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-guinea.csv",
            "Kenya": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-kenya.csv",
            "Liberia": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-liberia.csv",
            "Madagascar": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-madagascar.csv",
            "Mali": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-mali.csv",
            "Mauritania": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-mauritania.csv",
            "Mauritius": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-mauritius.csv",
            "Mozambique": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-mozambique.csv",
            "Namibia": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-namibia.csv",
            "Niger": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-niger.csv",
            "Nigeria": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-nigeria.csv",
            "Republic of Congo": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-republic-of-congo.csv",
            "Rwanda": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-rwanda.csv",
            "Senegal": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-senegal.csv",
            "Seychelles": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-seychelles.csv",
            "Somalia": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-somalia.csv",
            "South Africa": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-south-africa.csv",
            "Sudan": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-sudan.csv",
            "Tanzania": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-tanzania.csv",
            "Togo": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-togo.csv",
            "Uganda": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-uganda.csv",
            "Zambia": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-zambia.csv",
            "Zimbabwe": "https://raw.githubusercontent.com/dsfsi/covid19africa/master/data/line_lists/line-list-zimbabwe.csv",
        }

        for k, url in urls.items():
            self.parse_file(k, url)

    def parse_file(self, country, url):
        print("Getting data from {}".format(url))
        with closing(requests.get(url, stream=True)) as r:
            f = (line.decode("utf-8") for line in r.iter_lines())
            reader = csv.reader(f, delimiter=",", quotechar='"')

            headers = next(reader)

            assert (
                    headers[0] != "404: Not Found"
            ), "  Unable to get file contents, received {}.".format(headers)

            countries_with_empty_columns = [
                "Angola",
                "Burkina Faso",
                "Cabo Verde",
                "Cameroon",
                "Central African Republic",
                "Chad",
                "Côte d'Ivoire",
                "Democratic Republic of the Congo",
                "Djibouti",
                "Equatorial Guinea",
                "Eritrea",
                "Eswatini",
                "Gabon",
                "Guinea Bissau",
                "Guinea",
                "Liberia",
                "Madagascar",
                "Mali",
                "Mauritania",
                "Mauritius",
                "Mozambique",
                "Republic of Congo",
                "Senegal",
                "Seychelles",
                "Somalia",
                "Sudan",
                "Tanzania",
                "Togo",
                "Uganda",
                "Zambia",
            ]

            countries_with_mistyped_column = [
                "South Africa",

            ]

            countries_without_notes = [
                "Eritrea",
                "Eswatini",
                "Gabon",
                "Madagascar",
                "Mali",
                "Mauritania",
                "Mauritius",
                "Mozambique",
                "Republic of Congo",
                "Senegal",
                "Seychelles",
                "Somalia",
                "Sudan",
                "Tanzania",
                "Togo",
                "Uganda",
                "Zambia",
            ]

            tmp = copy.deepcopy(self.countries_fields)
            if country in countries_with_empty_columns:
                tmp.insert(0, ("", (None, None, None)))

            if country in countries_with_mistyped_column:
                tmp[14] = ("underlyng_conditions", ("subject", "underlying_conditions", str))

            if country in countries_without_notes:
                del tmp[-1]

            if country == "Ethiopia":
                tmp.insert(8, ("original_status", (None, None, None)))
                del tmp[10]
                tmp.insert(14, ("closed_date", (None, None, None)))
                tmp.insert(16, ("quarantine_status", (None, None, None)))
                del tmp[19]
                tmp.insert(19, ("contact", (None, None, None)))
                tmp.append(("source", (None, None, None)))

            if country == "Niger":
                del tmp[9]
                tmp.insert(9, ("source 1", (None, None, None)))
                tmp.insert(10, ("source 2", (None, None, None)))

            updated_headers_mapping = {field: (k, mapping) for k, (field, mapping) in enumerate(tmp)}
            expected_h = list(updated_headers_mapping.keys())
            obtained_h = headers[: len(expected_h)]
            obtained_h = [header.strip() for header in obtained_h]

            assert (
                    obtained_h == expected_h
            ), "CSV headers have changed\nexpected: {}\n     got: {})".format(
                expected_h, obtained_h
            )

            idx = 0
            last = None
            if country == "South Africa":
                last = 275

            for row in reader:
                idx += 1
                if last and idx == last:
                    break

                subject, demographic = self.parse_row(
                    country, row, updated_headers_mapping
                )

                self.subjects.append(subject)
                self.demographics.append(demographic)

    def parse_row(self, country, row, mapping):
        subject = {"country": country}
        demographic = {}

        for k, (i, (node_type, node_field, type_conv)) in mapping.items():
            if node_field:
                value = row[i]
                if value:
                    if node_type == "subject":
                        subject[node_field] = type_conv(value)
                    if node_type == "demographic":
                        demographic[node_field] = type_conv(value)

        case_id = subject["submitter_id"]
        subject["submitter_id"] = format_subject_submitter_id(subject["country"], subject["submitter_id"])

        if subject["country"] == "South Africa" and case_id == "110":
            if subject["age"] == 34:
                subject["submitter_id"] += "_1"
            if subject["age"] == 27:
                subject["submitter_id"] += "_2"

        subject["projects"] = [{"code": self.project_code}]

        demographic["submitter_id"] = format_demographic_submitter_id(
            subject["submitter_id"]
        )
        demographic["subjects"] = [
            {"submitter_id": subject["submitter_id"]}
        ]

        return subject, demographic

    def submit_metadata(self):
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
