import asyncio
from pathlib import Path
import re
import json
import requests
import time
import datetime
from dateutil.parser import parse

from botocore import UNSIGNED
from botocore.config import Config
import boto3
import codecs

from etl import base
from utils.async_file_helper import AsyncFileHelper
from utils.metadata_helper import MetadataHelper


MAX_RETRIES = 5


def conform_data_format(data, field_name):
    """function to check if the data is in right format"""
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
    if re.match(pattern, data):
        return data
    raise Exception(
        f"Wrong format for {field_name}. Expect {data} has the format of {pattern}"
    )


class NCBI_MANIFEST(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.manifest_bucket = "sra-pub-sars-cov2"
        self.sra_src_manifest = "sra-src/Manifest"
        self.program_name = "open"
        self.project_code = "ncbi-covid-19"
        self.token = access_token
        self.last_submission_identifier = None

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

    def read_ncbi_manifest(self, key):
        """read the manifest"""
        tries = 0
        last_row_num = 0
        while tries < MAX_RETRIES:
            try:
                s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
                s3_object = s3.Object(self.manifest_bucket, key)
                line_stream = codecs.getreader("utf-8")
                row_num = 0
                for line in line_stream(s3_object.get()["Body"]):
                    row_num = row_num + 1
                    if row_num < last_row_num:
                        continue
                    if row_num % 1000 == 0:
                        print(f"Proccessed {row_num} rows of {key}")
                    words = line.split("\t")
                    guid = conform_data_format(words[0].strip(), "guid")
                    size = int(conform_data_format(words[2].strip(), "size"))
                    md5 = conform_data_format(words[3].strip(), "md5")
                    authz = f"/programs/{self.program_name}/project/{self.project_code}"
                    url = conform_data_format(words[5].strip(), "url")
                    release_date = parse(re.sub(r":[0-9]{3}", "", words[6].strip()))
                    yield guid, size, md5, authz, url, release_date
                break
            except Exception as e:
                print(f"Can not stream {key}. Retrying...")
                time.sleep(30)
                tries += 1
                last_row_num = row_num

    def submit_metadata(self):
        start = time.strftime("%X")

        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(
                asyncio.gather(self.index_manifest(self.sra_src_manifest))
            )
            loop.run_until_complete(asyncio.gather(AsyncFileHelper.close_session()))

        finally:
            loop.close()
        end = time.strftime("%X")
        print(f"Running time: From {start} to {end}")

    async def index_manifest(self, manifest):
        query_string = (
            '{ project (first: 0, dbgap_accession_number: "'
            + self.project_code
            + '") { last_submission_identifier } }'
        )
        try:
            response = self.metadata_helper.query_peregrine(query_string)
            self.last_submission_identifier = parse(
                response["data"]["project"][0]["last_submission_identifier"]
            )
        except Exception as ex:
            self.last_submission_identifier = None

        now = datetime.datetime.now()
        last_submission_date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

        for (guid, size, md5, authz, url, release_date) in self.read_ncbi_manifest(
            manifest
        ):
            if (
                not self.last_submission_identifier
                or release_date > self.last_submission_identifier
            ):
                filename = url.split("/")[-1]
                retrying = True

                while retrying:
                    try:
                        did, _, _, _, _, _ = await self.file_helper.async_find_by_name(
                            filename
                        )
                        retrying = False
                    except Exception as e:
                        print(
                            f"ERROR: Fail to query indexd for {filename}. Detail {e}. Retrying ..."
                        )
                        await asyncio.sleep(5)

                if did:
                    print(f"{filename} was already indexed")
                    continue

                print(f"start to index {filename}")
                retries = 0
                while retries < MAX_RETRIES:
                    try:
                        await self.file_helper.async_index_record(
                            guid, size, filename, url, authz, md5
                        )
                        break
                    except Exception as e:
                        retries += 1
                        print(
                            f"ERROR: Fail to create new indexd record for {guid}. Detail {e}. Retrying ..."
                        )
                        await asyncio.sleep(5)

        headers = {
            "content-type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }
        record = {
            "code": self.project_code,
            "dbgap_accession_number": self.project_code,
            "last_submission_identifier": last_submission_date_time,
        }
        res = requests.put(
            "{}/api/v0/submission/{}".format(self.base_url, self.program_name),
            headers=headers,
            data=json.dumps(record),
        )
