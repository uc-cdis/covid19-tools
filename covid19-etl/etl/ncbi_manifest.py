import asyncio
import csv
from pathlib import Path
import re
import json
import requests
import time
from dateutil.parser import parse

from botocore import UNSIGNED
from botocore.config import Config
import boto3
import codecs

from etl import base
from utils.async_file_helper import AsyncFileHelper
from utils.metadata_helper import MetadataHelper


MAX_RETRIES = 4
SLEEP_SECONDS = 15


def validate_sra_value_format(row_num, field_name, value):
    """function to check if the data is in right format"""
    if field_name in ["file_name", "authz", "release_date"]:
        return value

    if field_name == "guid":
        pattern = (
            "dg.63D5/[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$"
        )
    elif field_name == "url":
        pattern = "s3://.*"
    elif field_name == "size":
        pattern = "[0-9]+$"
    elif field_name == "md5":
        pattern = "[a-z0-9]{32}$"
    else:
        raise Exception(f"There is no {field_name}")
    if re.match(pattern, value):
        return value
    raise Exception(
        f"SRA manifest: Wrong format for '{field_name}' (row #{row_num}). Expected value to match '{pattern}' but got '{value}'"
    )


class NCBI_MANIFEST(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "NCBI"
        self.token = access_token
        self.last_submission = {}

        self.file_helper = AsyncFileHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

    def format_ncbi_sra_row(self, row_num, row, headers):
        validated_data = []
        for i, field_name in enumerate(headers):
            if field_name == "authz":
                value = f"/programs/{self.program_name}/projects/{self.project_code}"
            else:
                value = validate_sra_value_format(row_num, field_name, row[i].strip())
            if field_name == "size":
                value = int(value)
            elif field_name == "release_date":
                value = parse(re.sub(r":[0-9]{3}", "", value))
            validated_data.append(value)
        return validated_data

    def read_ncbi_sra_manifest(self):
        sra_s3_bucket = "sra-pub-sars-cov2"
        sra_manifest = "sra-src/Manifest"
        print(f"Reading SRA manifest 's3://{sra_s3_bucket}/{sra_manifest}'...")
        headers = ["guid", "file_name", "size", "md5", "authz", "url", "release_date"]
        tries = 0
        row_num = 0
        last_row_num = 0
        while tries <= MAX_RETRIES:
            try:
                s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
                s3_object = s3.Object(sra_s3_bucket, sra_manifest)
                line_stream = codecs.getreader("utf-8")
                input = line_stream(s3_object.get()["Body"])
                reader = csv.reader(input, delimiter="\t")
                for row_num, row in enumerate(reader):
                    if row_num < last_row_num:
                        continue
                    if row_num % 10000 == 0 and row_num != 0:
                        print(f"Processed {row_num} rows")
                    yield self.format_ncbi_sra_row(row_num, row, headers)
                break  # done reading the manifest!
            except Exception as e:
                print(
                    f"Failed to stream 's3://{sra_s3_bucket}/sra_manifest' (try #{tries}). Retrying... Details:\n  {e}"
                )
                if tries == MAX_RETRIES:
                    raise e
                time.sleep(SLEEP_SECONDS)
                tries += 1
                last_row_num = row_num

    def submit_metadata(self):
        start = time.strftime("%X")

        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(asyncio.gather(self.process_sra_manifest()))
        finally:
            try:
                if AsyncFileHelper.session:
                    future = AsyncFileHelper.close_session()
                    if future:
                        loop.run_until_complete(asyncio.gather(future))
            finally:
                loop.close()

        # update the project's `last_submission_identifier` so that next
        # time we can skip processing the rows we just processed
        self.metadata_helper.update_project_last_submission(
            json.dumps(self.last_submission)
        )

        end = time.strftime("%X")
        print(f"Running time: From {start} to {end}")

    async def process_sra_manifest(self):
        project_last_submission = self.metadata_helper.get_project_last_submission()
        try:
            if project_last_submission:
                self.last_submission = json.loads(project_last_submission)
        except Exception as e:
            print(
                f"Unable to parse JSON from `last_submission_identifier`: {project_last_submission}. Details:\n  {e}"
            )
            # will use init value of {}
        last_submitted_guid = self.last_submission.get("ncbi_manifest")
        print(
            f"Last submitted GUID ('last_submission_identifier'): {last_submitted_guid}"
        )

        seen_last_submitted_guid = False
        last_successful_guid = None
        failed = False
        for (
            guid,
            filename,
            size,
            md5,
            authz,
            url,
            release_date,
        ) in self.read_ncbi_sra_manifest():
            if (
                not last_submitted_guid  # no data was ever submitted yet
                or seen_last_submitted_guid  # this data is not already indexed
            ):
                tries = 0
                file_is_indexed = False
                while tries <= MAX_RETRIES:
                    try:
                        file_is_indexed = (
                            await self.file_helper.async_indexd_record_exists(guid)
                        )
                        break
                    except Exception as e:
                        print(
                            f"ERROR: Fail to query indexd for {guid} (try #{tries}). Retrying... Details:\n  {e}"
                        )
                        if tries == MAX_RETRIES:
                            failed = True
                        tries += 1
                        await asyncio.sleep(SLEEP_SECONDS)

                if file_is_indexed:
                    print(f"{filename} ({guid}) is already indexed")
                    continue

                print(f"Indexing {filename} ({guid}) (release date: {release_date})")
                tries = 0
                while tries <= MAX_RETRIES:
                    try:
                        await self.file_helper.async_index_record(
                            guid, size, filename, url, authz, md5
                        )
                        break
                    except Exception as e:
                        print(
                            f"ERROR: Fail to create new indexd record for {guid} (try #{tries}). Retrying... Details:\n  {e}"
                        )
                        if tries == MAX_RETRIES:
                            failed = True
                        tries += 1
                        await asyncio.sleep(SLEEP_SECONDS)

            if failed:
                # stop processing the manifest, but don't exit because we
                # need to record the `last_submission_identifier`
                break

            last_successful_guid = guid

            # found the last submitted data! start indexing on the next row
            if guid == last_submitted_guid:
                print("Found last submitted GUID!")
                seen_last_submitted_guid = True

        self.last_submission["ncbi_manifest"] = last_successful_guid

        if failed:
            raise Exception("Failed to process manifest")
