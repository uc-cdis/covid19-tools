import os
from pathlib import Path
import json
import re
import gzip
import subprocess
import shlex
from contextlib import closing
import asyncio
import time

from etl import base
from helper.async_file_helper import AsyncFileHelper
from helper.metadata_helper import MetadataHelper

from botocore import UNSIGNED
from botocore.config import Config
import boto3
import codecs


DATA_PATH = os.path.dirname(os.path.abspath(__file__))


class NCBI_FILE(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "NCBI"
        self.queue = asyncio.Queue()
        self.access_number_set = set()

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

        self.bucket = "sra-pub-sars-cov2-metadata-us-east-1"
        self.nodes = {
            "virus_sequence_contig": ["contigs/contigs.json"],
            "virus_sequence_peptide": ["peptides/peptides.json"],
            "virus_sequence_blastn": [
                "blastn/blastn.tsv",
                "acc\tqacc\tstaxid\tsacc\tslen\tlength\tbitscore\tscore\tpident\tsskingdom\tevalue\tssciname\n",
            ],
            "virus_sequence_notc": ["hmmsearch_notc/hmmsearch_notc.json"],
        }

    def get_existed_accession_numbers(self, node_name):
        query_string = "{ " + node_name + " (first:0) { submitter_id } }"
        response = self.metadata_helper.query_node_data(query_string)
        if response.status_code != 200:
            raise Exception(
                f"ERROR: can not get a list of accession numbers of the node {node_name}"
            )
        records = response.json()["data"][node_name]
        return records

    async def file_to_submissions(self, filepath):
        filename = os.path.basename(filepath)
        did, rev, md5, size = await self.file_helper.async_find_by_name(filename)
        if not did:
            guid = await self.file_helper.async_upload_file(filepath)
            print(f"file {filepath.name} uploaded with guid: {guid}")
        else:
            await self.file_helper.async_update_authz(did, rev)
            print(f"file {filepath.name} exists in indexd... skipping...")
        os.remove(filepath)

    async def process_row(
        self, line, node_name, ext, headers, accession_number, n_rows, f, excluded_set
    ):
        r1 = re.findall("[SDE]RR\d+", line)
        if len(r1) == 0 and n_rows == 0:
            return f, accession_number
        assert (
            len(r1) == 1
        ), "The files have changed (expected {} contains accession number in the format of [SDE]RR\d+). We may need to update the ETL code".format(
            line
        )
        read_accession_number = r1[0]

        if read_accession_number in excluded_set:
            return f, accession_number
        # self.queue.put_nowait(read_accession_number)
        self.access_number_set.add(f"{node_name}_{read_accession_number}")

        if not accession_number or int(read_accession_number[3:]) != int(
            accession_number[3:]
        ):
            if f:
                f.close()
                # asyncio.ensure_future(self.file_to_submissions(
                #     Path(f"{DATA_PATH}/{node_name}_{accession_number}.{ext}"))
                # )
                await self.file_to_submissions(
                    Path(f"{DATA_PATH}/{node_name}_{accession_number}.{ext}")
                )

            accession_number = read_accession_number
            f = open(f"{DATA_PATH}/{node_name}_{accession_number}.{ext}", "w")
            if headers:
                f.write(headers)
        f.write(line)
        return f, accession_number

    async def index_ncbi_data_file(
        self, node_name, ext, key, excluded_set, headers=None
    ):
        s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
        s3_object = s3.Object(self.bucket, key)
        line_stream = codecs.getreader("utf-8")
        accession_number = None
        n_rows = 0
        f = None
        try:
            for line in line_stream(s3_object.get()["Body"]):
                f, accession_number = await self.process_row(
                    line,
                    node_name,
                    ext,
                    headers,
                    accession_number,
                    n_rows,
                    f,
                    excluded_set,
                )
                n_rows += 1
                if n_rows % 1000 == 0:
                    print(f"Finish process {n_rows} of file {node_name}")
        except Exception as e:
            # close the file
            if f:
                f.close()
            await asyncio.sleep(10)
        self.queue.put_nowait(None)

    def submit_metadata(self):
        start = time.strftime("%X")
        loop = asyncio.get_event_loop()
        tasks = []
        for node_name, value in self.nodes.items():
            key = value[0]
            headers = value[1] if len(value) > 1 else None

            lists = []
            ext = re.search("\.(.*)$", key).group(1)
            tasks.append(
                asyncio.ensure_future(
                    self.index_ncbi_data_file(node_name, ext, key, set(lists), headers)
                )
            )

        try:
            loop.run_until_complete(asyncio.gather(*tasks))
        finally:
            loop.close()
        end = time.strftime("%X")
        print(f"Running time: From {start} to {end}")


class SRA_TAXONOMY_FILE(NCBI_FILE):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.bucket = "sra-pub-sars-cov2-metadata-us-east-1"
        self.key = "sra_taxonomy/coronaviridae_07_31_2020_000000000000.gz"
        self.node_name = "virus_sequence_run_taxonomy"

    def submit_metadata(self):
        s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
        s3_object = s3.Object(self.bucket, self.key)
        file_path = f"{DATA_PATH}/{self.node_name}_11.gz"
        s3_object.download_file(file_path)
        asyncio.run(super().file_to_submissions(Path(file_path)))
