import csv
from pathlib import Path

from etl import base
from helper.file_helper import FileHelper
from helper.format_helper import (
    derived_submitter_id,
    format_submitter_id,
)
from helper.metadata_helper import MetadataHelper

COXRAY_DATA_PATH = "../data"


def harmonize_sex(sex):
    sex_mapping = {
        "F": "Female",
        "M": "Male",
    }
    return sex_mapping.get(sex, None)


def harmonize_survival(survival):
    survival_mapping = {
        "Y": "Alive",
        "N": "Dead",
    }
    return survival_mapping.get(survival, None)


def harmonize_finding(finding):
    return finding.split(",")


def harmonize_intubated(intubated):
    intubated_mapping = {
        "Y": "True",
        "N": "False",
    }
    return intubated_mapping.get(intubated, None)


def harmonize_needed_supplemental_O2(needed_supplemental_O2):
    needed_supplemental_O2_mapping = {
        "Y": "True",
        "N": "False",
    }
    return needed_supplemental_O2_mapping.get(needed_supplemental_O2, None)


def harmonize_went_icu(went_icu):
    went_icu_mapping = {
        "Y": "True",
        "N": "False",
    }
    return went_icu_mapping.get(went_icu, None)


def harmonize_intubation_present(intubation_present):
    intubation_present_mapping = {
        "Y": "True",
        "N": "False",
    }
    return intubation_present_mapping.get(intubation_present, None)


def harmonize_in_icu(in_icu):
    in_icu_mapping = {
        "Y": "True",
        "N": "False",
    }
    return in_icu_mapping.get(in_icu, None)


def harmonize_extubated(extubated):
    extubated_mapping = {
        "Y": "True",
        "N": "False",
    }
    return extubated_mapping.get(extubated, None)


fields_mapping = {
    # "patientid": ("subject", "submitter_id", None),
    "offset": ("follow_up", "offset", int),
    "sex": ("demographic", "gender", harmonize_sex),
    "age": ("demographic", "age", int),
    "finding": ("observation", "pneumonia_type", harmonize_finding),
    "survival": ("subject", "vital_status", harmonize_survival),
    "intubated": ("observation", "ventilator_status", harmonize_intubated),
    "intubation_present": (
        "follow_up",
        "intubation_present",
        harmonize_intubation_present,
    ),
    "went_icu": ("observation", "icu_status", harmonize_went_icu),
    "in_icu": ("follow_up", "in_icu", harmonize_in_icu),
    "needed_supplemental_O2": (
        "observation",
        "needed_supplemental_O2",
        harmonize_needed_supplemental_O2,
    ),
    "extubated": ("follow_up", "extubated", harmonize_extubated),
    "temperature": ("follow_up", "temperature", float),
    "pO2_saturation": ("follow_up", "pO2_saturation", float),
    "leukocyte_count": ("follow_up", "leukocyte_count", float),
    "neutrophil_count": ("follow_up", "neutrophil_count", float),
    "lymphocyte_count": ("follow_up", "lymphocyte_count", float),
    "view": ("follow_up", "view", None),
    "modality": ("follow_up", "modality", None),
    "date": ("imaging_file", "image_date", None),
    "location": ("imaging_file", "image_location", None),
    "folder": ("imaging_file", "folder", None),
    "filename": ("imaging_file", "file_name", None),
    "doi": ("study", "study_doi", None),
    "url": ("imaging_file", "image_url", None),
    "license": ("imaging_file", "license", None),
    "clinical_notes": ("imaging_file", "clinical_notes", None),
    "other_notes": ("imaging_file", "other_notes", None),
}


class COXRAY(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "COXRAY"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.file_helper = FileHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.nodes = {
            "core_metadata_collection": [],
            "study": [],
            "subject": [],
            "observation": [],
            "follow_up": [],
            "demographic": [],
            "imaging_file": [],
        }

    def files_to_submissions(self):
        with open(Path(COXRAY_DATA_PATH).joinpath("metadata.csv")) as f:
            reader = csv.reader(f, delimiter=",", quotechar='"')
            headers = next(reader)
            for row in reader:
                row_nodes = self.parse_row(headers, row)
                for k, v in row_nodes.items():
                    self.nodes[k].append(v)

    def parse_row(self, headers, row):
        cmc_submitter_id = format_submitter_id("cmc_coxray", {})
        subject_submitter_id = format_submitter_id(
            "subject_coxray", {"patientid": row[headers.index("patientid")]}
        )
        observation_submitter_id = derived_submitter_id(
            subject_submitter_id, "subject_coxray", "observation_coxray", {}
        )
        follow_up_submitter_id = derived_submitter_id(
            subject_submitter_id,
            "subject_coxray",
            "follow_up_coxray",
            {"offset": row[headers.index("offset")]},
        )
        demographic_submitter_id = derived_submitter_id(
            subject_submitter_id, "subject_coxray", "demographic_coxray", {},
        )
        imaging_file_submitter_id = format_submitter_id(
            "imaging_file_coxray", {"filename": row[headers.index("filename")]}
        )
        study_submitter_id = format_submitter_id(
            "study_coxray", {"doi": row[headers.index("doi")]}
        )

        filename = row[headers.index("filename")]
        filename = Path(filename)
        filepath = Path(COXRAY_DATA_PATH).joinpath("images", filename)
        filepath_exist = filepath.exists()

        nodes = {
            "core_metadata_collection": {
                "submitter_id": cmc_submitter_id,
                "projects": [{"code": self.project_code}],
            },
            "study": {
                "submitter_id": study_submitter_id,
                "projects": [{"code": self.project_code}],
            },
            "subject": {
                "submitter_id": subject_submitter_id,
                "projects": [{"code": self.project_code}],
                "studies": [{"submitter_id": study_submitter_id}],
            },
            "observation": {
                "submitter_id": observation_submitter_id,
                "subjects": [{"submitter_id": subject_submitter_id}],
            },
            "follow_up": {
                "submitter_id": follow_up_submitter_id,
                "subjects": [{"submitter_id": subject_submitter_id}],
            },
            "demographic": {
                "submitter_id": demographic_submitter_id,
                "subjects": [{"submitter_id": subject_submitter_id}],
            },
        }

        if filepath_exist:
            data_type = "".join(filename.suffixes)
            did, rev, md5sum, filesize = self.file_helper.find_by_name(
                filename=filename
            )
            assert (
                did
            ), f"file {filename} does not exist in the index, rerun COXRAY_FILE ETL"
            self.file_helper.update_authz(did=did, rev=rev)

            nodes["imaging_file"] = {
                "submitter_id": imaging_file_submitter_id,
                "subjects": [{"submitter_id": subject_submitter_id}],
                "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                "data_type": data_type,
                "data_format": "Image File",
                "data_category": "X-Ray Image",
                "file_size": filesize,
                "md5sum": md5sum,
                "object_id": did,
            }
        else:
            print(
                f"subject references the file that doesn't exist as a file: {filepath}"
            )

        for k, (node, field, converter) in fields_mapping.items():
            value = row[headers.index(k)]
            if node in nodes and value:
                if converter:
                    nodes[node][field] = converter(value)
                else:
                    nodes[node][field] = value

        return nodes

    def submit_metadata(self):
        print("Submitting data...")

        for k, v in self.nodes.items():
            submitter_id_exist = []
            print(f"Submitting {k} data...")
            for node in v:
                node_record = {"type": k}
                node_record.update(node)
                submitter_id = node_record["submitter_id"]
                if submitter_id not in submitter_id_exist:
                    submitter_id_exist.append(submitter_id)
                    self.metadata_helper.add_record_to_submit(node_record)
            self.metadata_helper.batch_submit_records()
