import os
from pathlib import Path

from etl import base
from helper.file_helper import FileHelper


class NCBI_FILE(base.BaseETL):
    def __init__(self, base_url, access_token):
        super().__init__(base_url, access_token)

        self.program_name = "open"
        self.project_code = "NCBI"

        self.file_helper = FileHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

    def file_to_submissions(self, filepath):
        filename = os.path.basename(filepath)
        did, rev, md5, size = self.file_helper.find_by_name(filename)
        if not did:
            guid = self.file_helper.upload_file(filepath)
            print(f"file {image_filepath.name} uploaded with guid: {guid}")
        else:
            print(f"file {image_filepath.name} exists in indexd... skipping...")


class BLASTN_FILE(NCBI_FILE):
    def __init__(self, base_url, access_token, s3_url):
        super().__init__(base_url, access_token)
        self.s3_url = s3_url
        self.prefix = "ncbi_blast"

    def extract(self, start=0):
        pass


class PEPTIDES_FILE(NCBI_FILE):
    def __init__(self, base_url, access_token, s3_url):
        super().__init__(base_url, access_token)
        self.s3_url = s3_url
        self.prefix = "ncbi_peptides"

    def extract(self, start=0):
        pass
