import os
import asyncio
import gzip
import time
import re
from datetime import datetime
from functools import partial

from botocore import UNSIGNED
from botocore.config import Config
import boto3
import codecs
from google.cloud import bigquery

from etl import base
from utils.async_file_helper import AsyncFileHelper
from utils.format_helper import format_submitter_id
from utils.metadata_helper import MetadataHelper
from etl.ncbi_file import NCBI_FILE

DATA_PATH = os.path.dirname(os.path.abspath(__file__))

FILE_EXTENSIONS = ["json", "tsv", "gb", "fastqsanger", "fasta", "fastq", "hmmer", "bam"]

FILE_EXTENSION_MAPPING = {"fq": "fastq", "fa": "fasta"}

MAX_RETRIES = 3


def get_file_extension(filename):
    """get file extension from the filename"""

    _, file_extension = os.path.splitext(filename)
    file_extension = file_extension.replace(".", "")

    if file_extension in FILE_EXTENSIONS:
        return file_extension
    elif file_extension in FILE_EXTENSION_MAPPING:
        return FILE_EXTENSION_MAPPING[file_extension]

    # Special handling
    for extension in FILE_EXTENSIONS:
        res = re.findall(f"\.{extension}\.", filename)
        if len(res) > 0:
            return extension

    for extension in FILE_EXTENSION_MAPPING:
        res = re.findall(f"\.{extension}\.", filename)
        if len(res) > 0:
            return FILE_EXTENSION_MAPPING[extension]

    return "unknown"


def convert_to_int(s):
    try:
        return int(s)
    except Exception:
        return None


def convert_to_list(s):
    if type(s) == list:
        return s
    return [s]


def get_enum_value(l, default, s):
    if s in l:
        return s
    return default


def convert_datetime_to_str(dt):
    if type(dt) != datetime:
        return None
    return dt.strftime("%Y-%m-%d")


def identity_function(s):
    return s


# The files need to be handled so that they are compatible
# to gen3 fields
SPECIAL_MAP_FIELDS = {
    "avgspotlen": ("avgspotlen", int, convert_to_int),
    "file_size": ("bytes", int, convert_to_int),
    "datastore_provider": ("datastore_provider", list, convert_to_list),
    "datastore_region": ("datastore_region", list, convert_to_list),
    "ena_first_public_run": ("ena_first_public_run", list, convert_to_list),
    "ena_last_update_run": ("ena_last_update_run", list, convert_to_list),
    "mbases": ("mbases", int, convert_to_int),
    "mbytes": ("mbytes", int, convert_to_int),
    "data_format": (
        "datastore_filetype",
        str,
        partial(
            get_enum_value,
            [
                "fasta",
                "fastq",
                "sff",
                "slx",
                "slxfq",
                "qual",
                "pbi",
                "srf",
                "txt",
                "fna",
            ],
            "other",
        ),
    ),
    "librarylayout": (
        "librarylayout",
        str,
        partial(get_enum_value, ["Paired", "Single", None], None),
    ),
    "release_date": ("releasedate", datetime, convert_datetime_to_str),
    "collection_date": ("collection_date_sam", datetime, convert_datetime_to_str),
    "ncbi_bioproject": ("bioproject", str, identity_function),
    "ncbi_biosample": ("biosample", str, identity_function),
    "sample_accession": ("sample_acc", str, identity_function),
    "country_region": ("geo_loc_name_country_calc", str, identity_function),
    "continent": (
        "geo_loc_name_country_continent_calc",
        str,
        partial(
            get_enum_value,
            [
                "Asia",
                "Africa",
                "Antarctica",
                "Australia",
                "Europe",
                "North America",
                "South America",
            ],
            None,
        ),
    ),
}


def read_ncbi_manifest(bucket, key, accession_number_filename_map):
    """read the manifest"""
    s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
    s3_object = s3.Object(bucket, key)
    line_stream = codecs.getreader("utf-8")
    for line in line_stream(s3_object.get()["Body"]):
        words = line.split("\t")
        r1 = re.findall("[SDE]RR\d+", words[5])
        if len(r1) >= 1:
            accession_number_filename_map[r1[0]] = words[1]


class NCBI(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "ncbi-covid-19"
        self.manifest_bucket = "sra-pub-sars-cov2"
        self.sra_src_manifest = "sra-src/Manifest"
        self.accession_number_filename_map = {}

        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.file_helper = AsyncFileHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.data_file = NCBI_FILE(
            base_url=self.base_url,
            s3_bucket=self.project_code,
            access_token=access_token,
        )

        self.submitting_data = {
            "sample": [],
            "virus_sequence": [],
            "core_metadata_collection": [],
            "virus_sequence_run_taxonomy": [],
            "virus_sequence_contig": [],
            "virus_sequence_blastn": [],
            "virus_sequence_contig_taxonomy": [],
            "virus_sequence_peptide": [],
            "virus_sequence_hmm_search": [],
        }

        self.submitting_data["core_metadata_collection"].append(
            {
                "submitter_id": format_submitter_id("cmc_ncbi_covid19", {}),
                "projects": [{"code": self.project_code}],
            }
        )

        read_ncbi_manifest(
            self.manifest_bucket,
            self.sra_src_manifest,
            self.accession_number_filename_map,
        )

    def submit_metadata(self):
        start = time.strftime("%X")
        loop = asyncio.get_event_loop()
        tasks = []

        for node_name, _ in self.data_file.nodes.items():
            if node_name == "virus_sequence_run_taxonomy":
                continue
            else:
                tasks.append(
                    asyncio.ensure_future(self.files_to_node_submissions(node_name))
                )

        try:
            results = loop.run_until_complete(asyncio.gather(*tasks))
            loop.run_until_complete(
                asyncio.gather(
                    self.files_to_virus_sequence_run_taxonomy_submission(results[0])
                )
            )
            if AsyncFileHelper.session:
                loop.run_until_complete(asyncio.gather(AsyncFileHelper.close_session()))
        finally:
            loop.close()
        end = time.strftime("%X")

        for k, v in self.submitting_data.items():
            print(f"Submitting {k} data...")
            for node in v:
                node_record = {"type": k}
                node_record.update(node)
                self.metadata_helper.add_record_to_submit(node_record)
            self.metadata_helper.batch_submit_records()

        print(f"Running time: From {start} to {end}")

    async def files_to_virus_sequence_run_taxonomy_submission(
        self, submitting_accession_numbers
    ):
        """get submitting data for virus_sequence_run_taxonomy node"""

        if not submitting_accession_numbers:
            return

        records = self._get_response_from_big_query(submitting_accession_numbers)

        # Keep track accession_numbers having link to virus_sequence nodes
        accession_number_set = set()
        for record in records:
            if record["acc"] in self.accession_number_filename_map:
                accession_number = record["acc"]
                print(f"Get from bigquery response {accession_number}")
                success = await self._parse_big_query_response(record)
                if success:
                    accession_number_set.add(accession_number)

        cmc_submitter_id = format_submitter_id("cmc_ncbi_covid19", {})
        for accession_number in submitting_accession_numbers:
            virus_sequence_run_taxonomy_submitter_id = format_submitter_id(
                "virus_sequence_run_taxonomy", {"accession_number": accession_number}
            )
            submitted_json = {
                "submitter_id": virus_sequence_run_taxonomy_submitter_id,
                "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                "accession_number": accession_number,
                "data_type": "Virus Sequence Run Taxonomy Analysis",
                "data_format": "json",
                "data_category": "Kmer-based Taxonomy Analysis",
            }

            # Add link to virus sequence node
            if accession_number in accession_number_set:
                submitted_json["virus_sequences"] = [
                    {"submitter_id": f"virus_sequence_{accession_number}"}
                ]

            filename = f"virus_sequence_run_taxonomy_{accession_number}.csv"
            print(f"Get indexd info of {filename}")
            trying = True
            while trying:
                try:
                    (
                        did,
                        rev,
                        md5sum,
                        filesize,
                        file_name,
                        authz,
                    ) = await self.file_helper.async_find_by_name(filename=filename)
                    trying = False
                except Exception as e:
                    print(
                        f"Can not get indexd record of {filename}. Detail {e}. Retrying..."
                    )

            assert (
                did
            ), f"file {filename} does not exist in the index, rerun NCBI_FILE ETL"

            if not authz:
                tries = 0
                while tries < MAX_RETRIES:
                    try:
                        await self.file_helper.async_update_authz(did=did, rev=rev)
                        break
                    except Exception as e:
                        tries += 1
                        print(
                            f"Can not update indexd for {did}. Detail {e}. Retrying..."
                        )

            submitted_json["file_size"] = filesize
            submitted_json["md5sum"] = md5sum
            submitted_json["object_id"] = did
            submitted_json["file_name"] = file_name

            self.submitting_data["virus_sequence_run_taxonomy"].append(submitted_json)

    async def files_to_node_submissions(self, node_name):
        """Get submitting data for the node"""

        retrying = True
        while retrying:
            try:
                submitting_accession_numbers = (
                    await self.get_submitting_accession_number_list(node_name)
                )
                retrying = False
            except Exception as e:
                print(
                    f"Can not query peregine with {node_name}. Detail {e}. Retrying..."
                )

        for accession_number in submitting_accession_numbers:
            submitter_id = format_submitter_id(
                node_name, {"accession_number": accession_number}
            )

            cmc_submitter_id = format_submitter_id("cmc_ncbi_covid19", {})

            contig_submitter_id = format_submitter_id(
                "virus_sequence_contig", {"accession_number": accession_number}
            )
            peptide_submitter_id = format_submitter_id(
                "virus_sequence_peptide", {"accession_number": accession_number}
            )
            run_taxonomy_submitter_id = format_submitter_id(
                "virus_sequence_run_taxonomy", {"accession_number": accession_number}
            )

            contig_taxonomy_submitter_id = format_submitter_id(
                "virus_sequence_contig_taxonomy", {"accession_number": accession_number}
            )

            if node_name == "virus_sequence_contig":
                submitted_json = {
                    "submitter_id": submitter_id,
                    "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                    "virus_sequences_run_taxonomies": [
                        {"submitter_id": run_taxonomy_submitter_id}
                    ],
                    "accession_number": accession_number,
                    "data_type": "Virus Sequence Contig",
                    "data_format": "json",
                    "data_category": "Nucleotide Contig",
                }
            elif node_name == "virus_sequence_blastn":
                submitted_json = {
                    "submitter_id": submitter_id,
                    "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                    "virus_sequence_contigs": [{"submitter_id": contig_submitter_id}],
                    "accession_number": accession_number,
                    "data_type": "Virus Sequence Blastn",
                    "data_format": "tsv",
                    "data_category": "Nucleotide Blast",
                }
            elif node_name == "virus_sequence_peptide":
                submitted_json = {
                    "submitter_id": submitter_id,
                    "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                    "virus_sequence_contigs": [{"submitter_id": contig_submitter_id}],
                    "accession_number": accession_number,
                    "data_type": "Peptides Annotation Using VIGOR3",
                    "data_format": "json",
                    "data_category": "Peptides Annotation",
                }
            elif node_name == "virus_sequence_hmm_search":
                submitted_json = {
                    "submitter_id": submitter_id,
                    "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                    "virus_sequence_peptides": [{"submitter_id": peptide_submitter_id}],
                    "accession_number": accession_number,
                    "data_type": "Virus Sequence HMM Search",
                    "data_format": "json",
                    "data_category": "HMMER Scab of Contigs",
                }
            elif node_name == "virus_sequence_contig_taxonomy":
                submitted_json = {
                    "submitter_id": submitter_id,
                    "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                    "virus_sequence_contigs": [{"submitter_id": contig_submitter_id}],
                    "accession_number": accession_number,
                    "data_type": "Contig Taxonomy",
                    "data_format": "json",
                    "data_category": "Kmer-based Taxonomy Analysis of Contigs",
                }

            else:
                raise Exception(f"ERROR: {node_name} does not exist")

            ext = re.search("\.(.*)$", self.data_file.nodes[node_name][0]).group(1)
            filename = f"{node_name}_{accession_number}.{ext}"

            print(f"Get indexd record of {filename}")

            retrying = True
            while retrying:
                try:
                    (
                        did,
                        rev,
                        md5sum,
                        filesize,
                        file_name,
                        authz,
                    ) = await self.file_helper.async_find_by_name(filename=filename)
                    retrying = False
                except Exception as e:
                    print(
                        f"ERROR: Fail to query indexd for {filename}. Detail {e}. Retrying..."
                    )
                    await asyncio.sleep(5)

            assert (
                did
            ), f"file {filename} does not exist in the index, rerun NCBI_FILE ETL"

            if not authz:
                tries = 0
                while tries < MAX_RETRIES:
                    try:
                        await self.file_helper.async_update_authz(did=did, rev=rev)
                        break
                    except Exception as e:
                        tries += 1
                        print(
                            f"ERROR: Fail to update indexd for {filename}. Detail {e}. Retrying..."
                        )
                        await asyncio.sleep(5)

            submitted_json["file_size"] = filesize
            submitted_json["md5sum"] = md5sum
            submitted_json["object_id"] = did
            submitted_json["file_name"] = file_name

            self.submitting_data[node_name].append(submitted_json)
        return submitting_accession_numbers

    # async def get_submitting_accession_number_list_for_run_taxonomy(self):
    #     """get submitting number list for run_taxonomy file"""

    #     node_name = "virus_sequence_run_taxonomy"
    #     submitting_accession_numbers = set()
    #     existed_accession_numbers = await self.data_file.get_existed_accession_numbers(
    #         node_name
    #     )

    #     s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
    #     s3_object = s3.Object(self.data_file.bucket, self.data_file.nodes[node_name][0])
    #     file_path = f"{DATA_PATH}/virus_sequence_run_taxonomy.gz"
    #     s3_object.download_file(file_path)

    #     n_lines = 0
    #     with gzip.open(file_path, "rb") as f:
    #         while True:
    #             bline = f.readline()
    #             if not bline:
    #                 break
    #             n_lines += 1
    #             if n_lines % 10000 == 0:
    #                 print(f"Finish process row {n_lines} of file {node_name}")
    #             line = bline.decode("UTF-8")
    #             r1 = re.findall("[SDE]RR\d+", line)
    #             if len(r1) == 0:
    #                 continue
    #             read_accession_number = r1[0]
    #             if (
    #                 f"{node_name}_{read_accession_number}"
    #                 not in existed_accession_numbers
    #             ):
    #                 submitting_accession_numbers.add(read_accession_number)
    #     return list(submitting_accession_numbers)

    async def get_submitting_accession_number_list(self, node_name):
        """get submitting acession number list"""

        submitting_accession_numbers = set()
        existed_accession_numbers = await self.data_file.get_existed_accession_numbers(
            node_name
        )

        s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
        s3_object = s3.Object(self.data_file.bucket, self.data_file.nodes[node_name][0])
        line_stream = codecs.getreader("utf-8")
        n_lines = 0
        for line in line_stream(s3_object.get()["Body"]):
            r1 = re.findall("[SDE]RR\d+", line)
            n_lines += 1
            if n_lines % 10000 == 0:
                print(f"Finish process row {n_lines} of file {node_name}")
            if len(r1) == 0:
                continue
            read_accession_number = r1[0]
            if (
                f"{node_name}_{read_accession_number}".lower()
                not in existed_accession_numbers
            ):
                submitting_accession_numbers.add(read_accession_number)

        return list(submitting_accession_numbers)

    def _get_response_from_big_query(self, accession_numbers):
        """
        Get data from big query. The format of the response json is
        described as below:
        [{
            "acc": "DRR220591",
            "assay_type": "RNA-Seq",
            "center_name": "KUMAMOTO",
            "consent": "public",
            "experiment": "DRX210904",
            "sample_name": "SAMD00217265",
            "instrument": "Illumina NovaSeq 6000",
            "librarylayout": "PAIRED",
            "libraryselection": "RANDOM",
            "librarysource": "TRANSCRIPTOMIC",
            "platform": "ILLUMINA",
            "sample_acc": "DRS139760",
            "biosample": "SAMD00217265",
            "organism": "Mus musculus",
            "sra_study": "DRP006149",
            #'releasedate': datetime.datetime(2020, 6, 4, 0, 0, tzinfo=<UTC>),
            "bioproject": "PRJDB9618",
            "mbytes": 2160,
            "loaddate": None,
            "avgspotlen": 300,
            "mbases": 6395,
            "insertsize": None,
            "library_name": None,
            "biosamplemodel_sam": [],
            "collection_date_sam": [],
            "geo_loc_name_country_calc": None,
            "geo_loc_name_country_continent_calc": None,
            "geo_loc_name_sam": [],
            "ena_first_public_run": [],
            "ena_last_update_run": [],
            "sample_name_sam": ["WT3_plus"],
            "datastore_filetype": ["sra"],
            "datastore_provider": ["gs", "ncbi", "s3"],
            "datastore_region": ["gs.US", "ncbi.public", "s3.us-east-1"],
        }]
        """

        assert accession_numbers != [], "accession_numbers is not empty"

        start = 0
        offset = 100
        client = bigquery.Client()
        while start < len(accession_numbers):
            end = min(start + offset, len(accession_numbers))
            stm = 'SELECT * FROM `nih-sra-datastore`.sra.metadata where consent = "public"'

            stm = stm + f' and (acc = "{accession_numbers[start]}"'
            for accession_number in accession_numbers[start + 1 : end]:
                stm = stm + f' or acc = "{accession_number}"'
            stm = stm + ")"

            query_job = client.query(stm)

            results = query_job.result()  # Waits for job to complete.

            for row in results:
                yield dict(row)
            start = end

    async def _parse_big_query_response(self, response):
        """
        Parse the big query response and get indexd record

        Return True if success

        """

        accession_number = response["acc"]

        sample = {}
        virus_sequence = {}

        sample["submitter_id"] = f"sample_{accession_number}"
        sample["projects"] = [{"code": self.project_code}]

        for field in [
            "ncbi_bioproject",
            "ncbi_biosample",
            "sample_accession",
            "host_associated_environmental_package_sam",
            "organism",
            "collection_date",
            "country_region",
            "continent",
        ]:
            if field in SPECIAL_MAP_FIELDS:
                old_name, dtype, handler = SPECIAL_MAP_FIELDS[field]
                sample[field] = handler(response.get(old_name))
            elif field in response:
                sample[field] = str(response.get(field))

        virus_sequence["submitter_id"] = f"virus_sequence_{accession_number}"
        for field in [
            "assay_type",
            "avgspotlen",
            "bytes",
            "center_name",
            "consent",
            "datastore_provider",
            "datastore_region",
            "description_sam",
            "ena_checklist_sam",
            "ena_first_public_run",
            "ena_last_update_run",
            "experiment",
            "insdc_center_name_sam",
            "insdc_first_public_sam",
            "insdc_center_alias_sam",
            "insdc_last_update_sam",
            "investigation_type_sam",
            "insdc_status_sam",
            "instrument",
            "library_name",
            "libraryselection",
            "librarysource",
            "mbases",
            "mbytes",
            "platform",
            "sra_accession_sam",
            "sra_study",
            "title_sam",
            "release_date",
            "data_format",
            "librarylayout",
        ]:
            if field in SPECIAL_MAP_FIELDS:
                old_name, dtype, handler = SPECIAL_MAP_FIELDS[field]
                virus_sequence[field] = handler(response.get(old_name))
            elif field in response:
                virus_sequence[field] = str(response.get(field))

        virus_sequence["samples"] = [{"submitter_id": sample["submitter_id"]}]
        virus_sequence["data_category"] = "Nucleotide"
        virus_sequence["data_type"] = "Sequence"
        virus_sequence["file_name"] = self.accession_number_filename_map[
            accession_number
        ]

        virus_sequence["data_format"] = get_file_extension(virus_sequence["file_name"])
        filename = virus_sequence["file_name"]

        retrying = True
        while retrying:
            try:
                (
                    did,
                    rev,
                    md5sum,
                    filesize,
                    file_name,
                    authz,
                ) = await self.file_helper.async_find_by_name(filename=filename)
                retrying = False
            except Exception as e:
                print(
                    f"ERROR: Fail to get indexd for {filename}. Detail {e}. Retrying..."
                )
                await asyncio.sleep(5)

        if not did:
            print(
                f"file {filename} does not exist in the index, rerun NCBI_MANIFEST ETL"
            )
            return False

        if not authz:
            retries = 0
            while retries < MAX_RETRIES:
                try:
                    await self.file_helper.async_update_authz(did=did, rev=rev)
                    break
                except Exception as e:
                    print(
                        f"ERROR: Fail to update indexd for {filename}. Detail {e}. Retrying..."
                    )
                    retries += 1
                    await asyncio.sleep(5)

        virus_sequence["file_size"] = filesize
        virus_sequence["md5sum"] = md5sum
        virus_sequence["object_id"] = did

        self.submitting_data["virus_sequence"].append(virus_sequence)
        self.submitting_data["sample"].append(sample)
        return True
