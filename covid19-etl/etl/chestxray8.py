from pathlib import Path

from etl import base
from helper.file_helper import FileHelper
from helper.format_helper import format_submitter_id
from helper.metadata_helper import MetadataHelper

CHESTXRAY8_DATA_PATH = "../data"


class CHESTXRAY8(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "ChestX-ray8"

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

        self.cmc_submitter_id = format_submitter_id("cmc_chestxray8", {})
        self.core_metadata_collection = [{
            "submitter_id": self.cmc_submitter_id,
            "projects": [{"code": self.project_code}],
        }]
        self.imaging_file = []

    def files_to_submissions(self):
        for image_type in ("No_findings", "Pneumonia"):
            for image_filepath in Path(CHESTXRAY8_DATA_PATH). \
                    joinpath("COVID-19"). \
                    joinpath("X-Ray Image DataSet"). \
                    joinpath(image_type).iterdir():
                did, rev, md5, size = self.file_helper.find_by_name(image_filepath.name)
                if not did:
                    guid = self.file_helper.upload_file(image_filepath)
                    print(f"file {image_filepath.name} uploaded with guid: {guid}")
                else:
                    print(f"file {image_filepath.name} exists in indexd... skipping...")

                imaging_file_submitter_id = format_submitter_id(
                    "imaging_file_chestxray8", {"filename": image_filepath.name}
                )
                uploaded_imaging_file = {
                    "submitter_id": imaging_file_submitter_id,
                    "core_metadata_collections": [{"submitter_id": self.cmc_submitter_id}],
                    "data_type": "PNG",
                    "data_format": "Image File",
                    "data_category": "X-Ray Image",
                    "file_name": image_filepath.name,
                    "file_size": size,
                    "md5sum": md5,
                    "object_id": did,
                }

                self.imaging_file.append(uploaded_imaging_file)

    def submit_metadata(self):
        print("Submitting data...")

        print("Submitting core_metadata_collection data")
        for cmc in self.core_metadata_collection:
            cmc_record = {"type": "core_metadata_collection"}
            cmc_record.update(cmc)
            self.metadata_helper.add_record_to_submit(cmc_record)
        self.metadata_helper.batch_submit_records()

        print("Submitting imaging_file data")
        for ifile in self.imaging_file:
            if_record = {"type": "imaging_file"}
            if_record.update(ifile)
            self.metadata_helper.add_record_to_submit(if_record)
        self.metadata_helper.batch_submit_records()
