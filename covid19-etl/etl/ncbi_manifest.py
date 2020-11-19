import asyncio
from pathlib import Path
import re
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


def conform_data_format(data, field_name):
    """function to check if the data is in right format"""
    if field_name == "guid":
        pattern = "(.+/)?[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$"
    elif field_name == "url":
        pattern = "s3://.*"
    elif field_name == "authz":
        pattern = "/programs/open/project/.+"
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
        self.sra_run_manifest = "run/Manifest"
        self.program_name = "open"
        self.project_code = "ncbi-covid-19"

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
        s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
        s3_object = s3.Object(self.manifest_bucket, key)
        line_stream = codecs.getreader("utf-8")
        for line in line_stream(s3_object.get()["Body"]):
            words = line.split("\t")
            guid = conform_data_format(words[0].strip(), "guid")
            size = int(conform_data_format(words[2].strip(), "size"))
            md5 = conform_data_format(words[3].strip(), "md5")
            authz = conform_data_format(words[4].strip(), "authz")
            url = conform_data_format(words[5].strip(), "url")
            release_date = parse(re.sub(r":[0-9]{3}", "", words[6].strip()))
            yield guid, size, md5, authz, url, release_date

    def submit_metadata(self):
        start = time.strftime("%X")

        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(
                asyncio.gather(
                    self.index_manifest(self.sra_run_manifest),
                    self.index_manifest(self.sra_src_manifest),
                )
            )

        finally:
            loop.close()
        end = time.strftime("%X")
        print(f"Running time: From {start} to {end}")

    async def index_manifest(self, manifest):
        query_string = (
            '{ project (first: 0, dbgap_accession_number: "'
            + self.project_code
            + '") { last_manifest_indexing_run } }'
        )
        try:
            response = self.metadata_helper.query_peregrine(query_string)
            last_run_manifest_indexing = parse(
                response["data"]["project"][0]["last_manifest_indexing_run"]
            )
        except Exception as ex:
            last_run_manifest_indexing = None

        for (guid, size, md5, authz, url, release_date) in self.read_ncbi_manifest(
            manifest
        ):
            if (
                not last_run_manifest_indexing
                or release_date > last_run_manifest_indexing
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
                    continue

                retrying = True
                while retrying:
                    try:
                        await self.file_helper.async_index_record(
                            guid, size, filename, url, authz, md5
                        )
                        retrying = False
                    except Exception as e:
                        print(
                            f"ERROR: Fail to create new indexd record for {guid}. Detail {e}. Retrying ..."
                        )
                        await asyncio.sleep(5)
