import copy
import csv
import datetime
import re
from contextlib import closing

from etl import base
from utils.metadata_helper import MetadataHelper


def format_subject_submitter_id(country, submitter_id):
    submitter_id = "subject_dsfsi_{}_{}".format(country.lower(), submitter_id)
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)
    return submitter_id


def format_node_submitter_id(subject_submitter_id, node_name):
    return subject_submitter_id.replace("subject_", f"{node_name}_")


def normalize_current_status(status):
    normalized = {
        "?": None,
        "alive": "alive",
        "clinically stable": "stable",
        "critical condition": "critical",
        "dead": "deceased",
        "death": "deceased",
        "deceased": "deceased",
        "died": "deceased",
        "in recovery": "in recovery",
        "in treatment": "in treatment",
        "na": None,
        "pos": "positive",
        "receiving treatment": "in treatment",
        "recovered": "recovered",
        "recovered (3/19/2020)": "recovered",
        "stable": "stable",
        "stable condition": "stable",
        "tested negative and hence is not considered as a case anymore": None,
        "under treatment": "in treatment",
        "unstable": "unstable",
    }

    return normalized[status.lower().strip()]


def normalize_symptoms(symptoms):
    normalized = {
        "acute pneumonia": "pneumonia",
        "asymptomatic": "asymptomatic",
        "breathing complications": "trouble breathing",
        "chest pain": "persistent pain or pressure in the chest",
        "cold": "fever* or feeling feverish/chills",
        "cough Fever": "cough",
        "cough": "cough",
        "covid-19 related symptoms": "COVID-19 related symptoms",
        "fever and aches and headache": "fever or feeling feverish/chills",
        "fever": "fever or feeling feverish/chills",
        "mild to moderate": "mild to moderate",
        "muscle pain": "muscle or body aches",
        "na": "no data",
        "rhinorrhea": "runny or stuffy nose",
        "severe": "severe",
        "sneezing": "sneezing",
        "tired": "fatigue (tiredness)",
    }

    list_of_symptoms = re.split(r", | and ", symptoms)

    result = []
    for symptom in list_of_symptoms:
        symptom = symptom.lower().strip()
        if symptom in normalized:
            result.append(normalized[symptom])

    return result


def normalize_date(date):
    if not date or date in ["NA", "N/A"]:
        return None

    d = date
    d = d.replace("Returned", "")
    d = d.replace("between", "")
    d = d.strip()

    if d == "early March":
        d = "2020-03-01"

    parsed_date = None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%d-%m-%y", "%m/%d/%Y", "%d-%b-%y", "%m.%d.%Y"):
        try:
            parsed_date = datetime.datetime.strptime(d, fmt)
        except ValueError:
            pass

    if not parsed_date:
        print(f"Couldn't parse date '{date}', returning date=None")
        return None

    return parsed_date.strftime("%Y-%m-%d")


def normalize_date_list(dates_string):
    # split dates by:
    # - `,`
    # - ` and `
    # - `  `
    date_list = re.split(r"\,| and |  ", dates_string)

    # handle data such as "3/14/2020-3/21/2020"
    if len(date_list) == 1 and "/" in dates_string:
        date_list = dates_string.split("-")

    date_list = [normalize_date(d.strip()) for d in date_list]
    date_list = [d for d in date_list if d]
    return date_list


def normalize_location(loc):
    loc = loc.strip()

    if loc in ["", "NA"]:
        return None

    if loc.lower().startswith("the ") or loc.lower().startswith("and "):
        loc = loc[4:]

    mapping = {
        "travelled": "Unknown location",
        "travel": "Unknown location",
        "local": "Local travel",
        "wa": "Local travel",  # Wa is a city in Guana. There is only 1 subject with this location and their country is Guana
        "united kingdom": "UK",
        "uk licester city": "UK",
        "uk london": "UK",
        "united states of america": "USA",
        "the united states of america": "USA",
        "us": "USA",
        "visiting resident of turkey travelled": "Turkey",
        "congo brazaville": "Congo Brazzaville",
    }

    res = mapping.get(loc.lower(), loc)
    res = res[0].upper() + res[1:]  # capitalize only 1st char

    return res


def normalize_location_list(loc_string):
    loc_string = loc_string.strip()

    # special cases...
    if loc_string == "Madrid  Spain via Doha  Qatar  Namibia":
        loc_string = "Madrid, Spain, Qatar, Namibia"
    elif loc_string == "Djibuti Brazil India Congo Brazaville":
        loc_string = "Djibuti, Brazil, India, Congo Brazaville"
    elif loc_string == "Namibia from London on 18 March":
        loc_string = "Namibia"
    elif (
        loc_string
        == "Johannesburg on 07 February 2020 and returned on 13 February 2020"
    ):
        loc_string = "Johannesburg, South Africa"
    elif (
        loc_string
        == "studying at WITS University in South Africa. He was tested in South Africa  but travelled back before results came out"
    ):
        loc_string = "South Africa"
    elif loc_string.startswith("Travelled to the United Kingdom Austria and "):
        loc_string = loc_string.replace(
            "Travelled to the United Kingdom Austria and ", "United Kingdom, Austria, "
        )
    elif loc_string == "Travelled to Germany France Switzerland and Austria":
        loc_string = "Germany, France, Switzerland, Austria"
    elif loc_string == "Travelled to France Germany and the Netherlands":
        loc_string = "France, Germany, the Netherlands"
    elif (
        loc_string
        == "Travelled to Saudi Arabia the United Kingdom Switzerland and Austria"
    ):
        loc_string = "Saudi Arabia, the United Kingdom, Switzerland, Austria"
    elif loc_string in [
        "no travel history",
        "no international travel history",
        "with no international travel history",
        "with pending travel history",
        "Contact with patient confirmed positive",
    ]:
        loc_string = ""

    loc_string = loc_string.replace("Travelled from", "")
    loc_string = loc_string.replace("Travelled to", "")
    loc_string = loc_string.replace(" and ", ", ")
    loc_string = loc_string.strip()
    if loc_string.lower().startswith("the "):
        loc_string = loc_string[4:]

    loc_list = re.split(r" & |\, | to |; ", loc_string)

    # locations with multiple words that should not be split on spaces
    no_space_split = [
        "Saudi Arabia",
        "Burkina Faso",
        "Cote d'Ivoire",
        "United Kingdom",
        "Congo Brazzaville",
        "UK London",
        "UK Licester City",
        "High risk countries",
        "United Arab Emirates",
        "Middle East",
        "United States of America",
        "South Africa",
        "High risk country",
        "South Sudan",
        "the Democratic Republic of Congo",
        "Democratic Republic of Congo",
        "New Zealand",
    ]

    # split by space
    if len(loc_list) == 1 and " " in loc_string and loc_string not in no_space_split:
        loc_list = loc_string.split(" ")
        loc_list = [l for l in loc_list if l]

    loc_list = [normalize_location(l) for l in loc_list]
    loc_list = [l for l in loc_list if l]

    return loc_list


def normalize_condition(condition):
    normalized = {"NA": None}

    norm = normalized.get(condition, condition)
    if norm == condition:
        return [norm]

    return None


def normalize_gender(gender):
    normalized = {
        "m": "Male",
        "male": "Male",
        "male ?": "Male",
        "f": "Female",
        "female": "Female",
        "woman": "Female",
        "not specified": "Unknown",
        "suleja": "Not reported",
        "na": "Not reported",
        "x": "Not reported",
        "?": "Not reported",
    }

    return normalized[gender.lower()]


def normalize_age(age):
    try:
        return int(float(age))
    except ValueError:
        return None


class DSFSI(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.subjects = []
        self.demographics = []
        self.observations = []

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
            ("date", ("observation", "reporting_date", normalize_date)),
            ("age", ("demographic", "age", normalize_age)),
            ("gender", ("demographic", "gender", normalize_gender)),
            ("city", ("demographic", "city", str)),
            ("province/state", ("demographic", "province_state", str)),
            ("country", ("demographic", "country_region", str)),
            (
                "current_status",
                ("subject", "tmp_current_status", normalize_current_status),
            ),
            (
                "source",
                ("observation", "reporting_source_url", str),
            ),  # type of fields "None" is used to remove the value
            ("symptoms", ("observation", "symptoms", normalize_symptoms)),
            (
                "date_onset_symptoms",
                ("observation", "date_onset_symptoms", normalize_date),
            ),
            (
                "date_admission_hospital",
                ("observation", "date_admission_hospital", normalize_date),
            ),
            ("date_confirmation", ("subject", "date_confirmation", normalize_date)),
            ("underlying_conditions", (None, None, None)),
            ("travel_history_dates", ("subject", "travel_history_dates", str)),
            ("travel_history_location", ("subject", "travel_history_location", str)),
            ("death_date", ("subject", "deceased_date", normalize_date)),
            ("notes_for_discussion", (None, None, None)),
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
        with closing(self.get(url, stream=True)) as r:
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

            countries_with_mistyped_column = ["South Africa"]

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

            # Ok, this is ugly... But, almost all the countries have some ugliness in the CSV format...
            # And this code deals with it
            tmp = copy.deepcopy(self.countries_fields)
            if country in countries_with_empty_columns:
                tmp.insert(0, ("", (None, None, None)))

            if country in countries_with_mistyped_column:
                tmp[14] = ("underlyng_conditions", (None, None, None))

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

            updated_headers_mapping = {
                field: (k, mapping) for k, (field, mapping) in enumerate(tmp)
            }
            expected_h = list(updated_headers_mapping.keys())
            obtained_h = headers[: len(expected_h)]
            obtained_h = [header.strip() for header in obtained_h]

            assert (
                obtained_h == expected_h
            ), "CSV headers have changed\nexpected: {}\n     got: {})".format(
                expected_h, obtained_h
            )

            # South Africa dataset has only 274 nice cases
            # Everything after has the same data and don't have any meaningful information
            idx = 0
            last = None
            if country == "South Africa":
                last = 275

            for row in reader:
                idx += 1
                if last and idx == last:
                    break

                subject, demographic, observation = self.parse_row(
                    country, row, updated_headers_mapping
                )

                self.subjects.append(subject)
                self.demographics.append(demographic)
                self.observations.append(observation)

    def parse_row(self, country, row, mapping):
        subject = {}
        demographic = {}
        observation = {}

        for (i, (node_type, node_field, type_conv)) in mapping.values():
            if node_field:
                value = row[i]
                if value:
                    if node_type == "subject":
                        if type_conv is None:
                            subject[node_field] = None
                            continue
                        subject[node_field] = type_conv(value)
                    if node_type == "demographic":
                        if type_conv is None:
                            demographic[node_field] = None
                            continue
                        demographic[node_field] = type_conv(value)

        # init subject node
        case_id = subject["submitter_id"]
        subject["submitter_id"] = format_subject_submitter_id(
            country, subject["submitter_id"]
        )
        subject["projects"] = [{"code": self.project_code}]

        # Only South Africa dataset has a record with the same case_id...
        # Because this code deals only with individual rows, it's hard coded right now
        if country == "South Africa" and case_id == "110":
            if demographic["age"] == 34:
                subject["submitter_id"] += "_1"
            elif demographic["age"] == 27:
                subject["submitter_id"] += "_2"

        # init demographic node
        demographic["submitter_id"] = format_node_submitter_id(
            subject["submitter_id"], "demographic"
        )
        demographic["subjects"] = [{"submitter_id": subject["submitter_id"]}]

        # init observation node
        observation["submitter_id"] = format_node_submitter_id(
            subject["submitter_id"], "observation"
        )
        observation["subjects"] = [{"submitter_id": subject["submitter_id"]}]

        if subject.get("date_confirmation"):
            subject["covid_19_status"] = "Positive"

        state = subject.get("tmp_current_status")
        if "tmp_current_status" in subject:
            del subject["tmp_current_status"]
        if state == "deceased":
            subject["vital_status"] = "Dead"
        elif state in ["alive"]:
            subject["vital_status"] = state.capitalize()
        elif state in ["positive"]:
            subject["covid_19_status"] = state.capitalize()
        elif state == "isolated":
            observation["isolation_status"] = state.capitalize()
        elif state in ["released", "recovered", "in recovery", "in treatment"]:
            observation["treatment_status"] = state.capitalize()
        elif state in ["stable", "unstable", "critical"]:
            observation["condition"] = state.capitalize()
        elif state:
            raise Exception('State "{}" is unknown'.format(state))

        if "travel_history_dates" in subject:
            date_list = normalize_date_list(subject["travel_history_dates"])
            if date_list:
                subject["travel_history_dates"] = date_list
            else:
                del subject["travel_history_dates"]

        if "travel_history_location" in subject:
            loc_list = normalize_location_list(subject["travel_history_location"])
            if loc_list:
                subject["travel_history_location"] = loc_list
            else:
                del subject["travel_history_location"]

        return subject, demographic, observation

    def submit_metadata(self):
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
