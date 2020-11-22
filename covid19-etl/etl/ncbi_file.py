import os
from pathlib import Path
import aiofiles as aiof
import re
import gzip
import asyncio
import time
from contextlib import suppress

from etl import base
from utils.async_file_helper import AsyncFileHelper
from utils.metadata_helper import MetadataHelper

from botocore import UNSIGNED
from botocore.config import Config
import boto3
import codecs


DATA_PATH = os.path.dirname(os.path.abspath(__file__))
MAX_RETRIES = 5


class NCBI_FILE(base.BaseETL):
    """Class for handle NCBI data file"""

    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

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

        self.bucket = "sra-pub-sars-cov2-metadata-us-east-1"
        self.nodes = {
            "virus_sequence_contig": ["contigs/contigs.json"],
            "virus_sequence_peptide": ["peptides/peptides.json"],
            "virus_sequence_blastn": [
                "blastn/blastn.tsv",
                "acc\tqacc\tstaxid\tsacc\tslen\tlength\tbitscore\tscore\tpident\tsskingdom\tevalue\tssciname\n",
            ],
            "virus_sequence_hmm_search": ["hmmsearch_notc/hmmsearch_notc.json"],
            "virus_sequence_run_taxonomy": [
                "sra_taxonomy/coronaviridae_07_31_2020_000000000000.gz"
            ],
        }

    def submit_metadata(self):
        """Main function to submit the data"""

        start = time.strftime("%X")
        loop = asyncio.get_event_loop()
        tasks = []
        for node_name, value in self.nodes.items():
            if node_name == "virus_sequence_run_taxonomy":
                continue
            key = value[0]
            headers = value[1] if len(value) > 1 else None

            ext = re.search("\.(.*)$", key).group(1)
            tasks.append(
                asyncio.ensure_future(
                    self.index_ncbi_data_file(node_name, ext, key, headers)
                )
            )

        try:
            results = loop.run_until_complete(asyncio.gather(*tasks))

            loop.run_until_complete(
                asyncio.gather(self.index_virus_sequence_run_taxonomy_file(results[0]))
            )

        finally:
            loop.close()
        end = time.strftime("%X")
        print(f"Running time: From {start} to {end}")

    async def index_virus_sequence_run_taxonomy_file(self, accession_numbers):
        """
        Chop the index virus sequence run taxonomy file into multiple smaller files
        by accession number and index them.

        Args:
            accession_numbers(set): a set of the interested accession numbers
        """

        s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
        s3_object = s3.Object(self.bucket, self.nodes["virus_sequence_run_taxonomy"][0])
        file_path = f"{DATA_PATH}/virus_sequence_run_taxonomy.gz"
        s3_object.download_file(file_path)

        results = {}
        with gzip.open(file_path, "rb") as f:
            line = f.readline()
            if line:
                header = line.decode("UTF-8")
            while line:
                row = line.decode("UTF-8")
                words = row.split(",")
                if (
                    words
                    and words[0] in accession_numbers
                    or accession_numbers == {"*"}
                ):
                    if words[0] in results:
                        results[words[0]].append(row)
                    else:
                        results[words[0]] = [row]

                line = f.readline()

        for accession_number, rows in results.items():
            file_path = (
                f"{DATA_PATH}/virus_sequence_run_taxonomy_{accession_number}.csv"
            )
            async with aiof.open(file_path, "w") as out:
                await out.write(header)
                for row in rows:
                    await out.write(row)
                    await out.flush()
            await self.file_to_indexd(Path(file_path))

    async def index_ncbi_data_file(self, node_name, ext, key, headers=None):
        """
        Asynchornous function to index NCBI data file into multiple smaller files
        by accession number and index them to indexd

        Args:
            node_name(str): node name
            ext(str): the file extension (json|tsv|csv)
            key(str): the s3 object key where the file lives
            headers(str): headers of the input file
        """
        # Build excluded_set which contains all existed accession numbers
        # Don't need to re-submit the data already exist in the database
        excluded_set = await self.get_existed_accession_numbers(node_name)

        tries = 0
        while tries < MAX_RETRIES:
            try:
                # Setup s3 connection for data streaming
                s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
                s3_object = s3.Object(self.bucket, key)
                line_stream = codecs.getreader("utf-8")

                #
                accession_numbers = set()

                # Keep track current accession number
                accession_number = None
                # Keep track number of read rows
                n_rows = 0
                # Keep track the current opening file
                f = None

                try:
                    # Stream the data from s3 bucket by reading line by line
                    for line in line_stream(s3_object.get()["Body"]):
                        try:
                            # Handle the line.
                            f, accession_number = await self.parse_row(
                                line,
                                node_name,
                                ext,
                                headers,
                                accession_number,
                                n_rows,
                                f,
                                excluded_set,
                            )
                            accession_numbers.add(accession_number)
                            n_rows += 1
                            if n_rows % 10000 == 0:
                                print(f"Finish process {n_rows} of file {node_name}")

                        except Exception as e:
                            print(f"ERROR: {e}")
                            # close the file
                            if f:
                                f.close()
                            with suppress(asyncio.TimeoutError):
                                await asyncio.sleep(10)
                except Exception as e:
                    print(f"ERROR: Can not download {key}. Detail {e}")
                    raise
                # Index the last file
                await self.file_to_indexd(
                    Path(f"{DATA_PATH}/{node_name}_{accession_number}.{ext}")
                )
                break
            except Exception as e:
                print(f"Can not stream {key} from s3. Retrying...")
                tries += 1
        return accession_numbers

    async def get_existed_accession_numbers(self, node_name):
        """
        Get a list of existed accession numbers from the graph

        Args:
            node_name(str): node name
        Returns:
            list(str): list of accession numbers
        """

        query_string = "{ " + node_name + " (first:0) { submitter_id } }"
        response = await self.metadata_helper.async_query_peregrine(query_string)
        records = response["data"][node_name]
        return set([record["submitter_id"] for record in records])

    async def parse_row(
        self, line, node_name, ext, headers, accession_number, n_rows, f, excluded_set
    ):
        """
        Parse a data row

        Args:
            line(str): a data row
            node_name(str): node name
            ext(str): the file extension (json|tsv|csv)
            headers(str): headers of the input file
            accession_number(str): the current accession number
            n_rows(int): number of rows processed
            f(file): the opening file
            excluded_set(set): a set of accession number need to be ignored

        Returns:
            f(file): the opening file
            accession_number(string): the current accession number
        """

        # Parse for the accession number
        r1 = re.findall("[SDE]RR\d+", line)
        if len(r1) == 0 and n_rows == 0:
            return f, accession_number
        assert (
            len(r1) == 1
        ), "The files have changed (expected {} contains accession number in the format of [SDE]RR\d+). We may need to update the ETL code".format(
            line
        )
        read_accession_number = r1[0]

        # Ignore the accession number existing in the excluded set
        if read_accession_number in excluded_set:
            return f, accession_number

        # If the line contains new accession_number, close the opening file, index it
        # and open new file for the new accession_number
        if not accession_number or read_accession_number != accession_number:
            if f:
                f.close()
                await self.file_to_indexd(
                    Path(f"{DATA_PATH}/{node_name}_{accession_number}.{ext}")
                )

            accession_number = read_accession_number
            f = open(f"{DATA_PATH}/{node_name}_{accession_number}.{ext}", "w")
            if headers:
                f.write(headers)
        f.write(line)
        return f, accession_number

    async def file_to_indexd(self, filepath):
        """Asynchornous call to index the data file"""
        filename = os.path.basename(filepath)
        retrying = True
        while retrying:
            try:
                did, _, _, _, _, _ = await self.file_helper.async_find_by_name(filename)
                retrying = False
            except Exception as e:
                print(
                    f"ERROR: Fail to query indexd for {filename}. Detail {e}. Retrying ..."
                )
                with suppress(asyncio.TimeoutError):
                    await asyncio.sleep(5)

        if not did:
            retrying = True
            while retrying:
                try:
                    guid = await self.file_helper.async_upload_file(filepath)
                    print(f"file {filepath.name} uploaded with guid: {guid}")
                    retrying = False
                except Exception as e:
                    print(
                        f"ERROR: Fail to upload file {filepath}. Detail {e}. Retrying ..."
                    )
                    with suppress(asyncio.TimeoutError):
                        await asyncio.sleep(5)
        else:
            print(f"file {filepath.name} exists in indexd... skipping...")
        os.remove(filepath)
