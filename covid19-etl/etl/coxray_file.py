from pathlib import Path

from etl import base
from helper.file_helper import FileHelper

COXRAY_DATA_PATH = "../data"


class COXRAY_FILE(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "COXRAY"

        self.file_helper = FileHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

    def files_to_submissions(self):
        for image_filepath in Path(COXRAY_DATA_PATH).joinpath("images").iterdir():
            did, rev, md5, size = self.file_helper.find_by_name(image_filepath.name)
            if not did:
                guid = self.file_helper.upload_file(image_filepath)
                print(f"file {image_filepath.name} uploaded with guid: {guid}")
            else:
                print(f"file {image_filepath.name} exists in indexd... skipping...")

    def submit_metadata(self):
        pass
