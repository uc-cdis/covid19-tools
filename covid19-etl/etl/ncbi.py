from collections import defaultdict
import csv
import os
import asyncio
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

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

FILE_EXTENSIONS = ["json", "tsv", "gb", "fastqsanger", "fasta", "fastq", "hmmer", "bam"]

FILE_EXTENSION_MAPPING = {"fq": "fastq", "fa": "fasta"}

MAX_RETRIES = 4
SLEEP_SECONDS = 15

RESUBMIT_ALL_METADATA = False


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


def _raise(ex):
    raise ex


# The fields need to be handled so that they are compatible
# with gen3 fields. Format: { <gen3 name>: ( <original name>, <handler> ) }
SRA_SPECIAL_MAP_FIELDS = {
    "avgspotlen": ("avgspotlen", convert_to_int),
    "file_size": ("bytes", convert_to_int),
    "datastore_provider": ("datastore_provider", convert_to_list),
    "datastore_region": ("datastore_region", convert_to_list),
    "ena_first_public_run": ("ena_first_public_run", convert_to_list),
    "ena_last_update_run": ("ena_last_update_run", convert_to_list),
    "mbases": ("mbases", convert_to_int),
    "mbytes": ("mbytes", convert_to_int),
    "data_format": (
        "datastore_filetype",
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
        partial(get_enum_value, ["Paired", "Single", None], None),
    ),
    "release_date": ("releasedate", convert_datetime_to_str),
    "collection_date": ("collection_date_sam", convert_datetime_to_str),
    "ncbi_bioproject": ("bioproject", lambda v: v),
    "ncbi_biosample": ("biosample", lambda v: v),
    "sra_accession": ("sample_acc", lambda v: v),
    "country_region": ("geo_loc_name_country_calc", lambda v: v),
    "continent": (
        "geo_loc_name_country_continent_calc",
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


GENBANK_TO_GEN3_MAPPING = {
    "Species": {"node": "sample", "prop": "species"},
    "Genus": {"node": "sample", "prop": "genus"},
    "Family": {"node": "sample", "prop": "family"},
    "Accession": [
        {
            "node": "sample",
            "prop": "genbank_accession",
        },
        {
            "node": "sample",
            "prop": "submitter_id",
            "transform": lambda v: f"sample_genbank_{v}",
        },
        {
            "node": "virus_sequence",
            "prop": "submitter_id",
            "transform": lambda v: f"virus_sequence_{v}",
        },
    ],
    "SRA_Accession": {"node": "sample", "prop": "sra_accession"},
    "Molecule_type": {"node": "sample", "prop": "sample_type"},
    "Geo_Location": {"node": "sample", "prop": "country_region"},
    "USA": {"node": "sample", "prop": "province_state"},
    "Host": {"node": "sample", "prop": "host"},
    "Isolation_Source": {"node": "sample", "prop": "isolation_source"},
    "Collection_Date": {"node": "sample", "prop": "collection_date"},
    "BioSample": {"node": "sample", "prop": "ncbi_biosample"},
    "Authors": {"node": "sample", "prop": "submitting_lab"},
    "Release_Date": {"node": "virus_sequence", "prop": "release_date"},
    "Length": {
        "node": "virus_sequence",
        "prop": "sequence_length",
        "transform": lambda v: int(v),
    },
    "Sequence_Type": {
        "node": "virus_sequence",
        "prop": "data_source",
        "transform": lambda v: _raise(
            Exception(
                f'Sequence_Type should be one of ["GenBank", "NCBI", "GISAID"] Got "{v}"'
            )
        )
        if v not in ["GenBank", "NCBI", "GISAID"]
        else v,
    },
    "Nuc_Completeness": {
        "node": "virus_sequence",
        "prop": "completeness",
        "transform": lambda v: _raise(
            Exception(
                f'Nuc_Completeness should be one of ["partial", "complete"]. Got "{v}"'
            )
        )
        if v not in ["partial", "complete"]
        else v,
    },
    "GenBank_Title": {"node": "virus_sequence", "prop": "genbank_title"},
    "Lineage": {"node": "virus_sequence", "prop": "pangolin_lineage"},
    # not including: Genotype, Segment, Publications, LineageProbability, tax_id
}


def read_ncbi_sra_manifest(accession_number_to_guids_map):
    """read the manifest"""
    sra_s3_bucket = "sra-pub-sars-cov2"
    sra_manifest = "sra-src/Manifest"
    print(f"Reading SRA manifest 's3://{sra_s3_bucket}/{sra_manifest}'...")
    s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
    s3_object = s3.Object(sra_s3_bucket, sra_manifest)
    line_stream = codecs.getreader("utf-8")
    for i, line in enumerate(line_stream(s3_object.get()["Body"])):
        # TODO csv
        words = line.split("\t")
        guid = words[0]
        url = words[5]
        r1 = re.findall("[SDE]RR\d+", url)
        if len(r1) >= 1:
            accession_number = r1[0]
            accession_number_to_guids_map[accession_number].append(guid)
        if i % 10000 == 0 and i != 0:
            print(f"Processed {i} rows")


class NCBI(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "ncbi"
        self.accession_number_to_guids_map = defaultdict(list)
        self.all_accession_numbers = set()

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
            # from from SRA and GenBank data:
            "sample": {},  # { <sra accession>: <sample record> }
            # from SRA data:
            "virus_genome": [],
            "core_metadata_collection": [],
            "virus_genome_run_taxonomy": [],
            "virus_genome_contig": [],
            "virus_sequence_blastn": [],
            "virus_genome_contig_taxonomy": [],
            "virus_sequence_peptide": [],
            "virus_sequence_hmm_search": [],
            # from GenBank data:
            "virus_sequence": [],
        }

        self.submitting_data["core_metadata_collection"].append(
            {
                "submitter_id": "cmc_ncbi_covid19",
                "projects": [{"code": self.project_code}],
            }
        )

    def files_to_submissions(self):
        start = time.strftime("%X")

        read_ncbi_sra_manifest(self.accession_number_to_guids_map)

        loop = asyncio.get_event_loop()
        tasks = []

        for node_name, _ in self.data_file.nodes.items():
            if node_name == "virus_genome_run_taxonomy":
                continue
            else:
                tasks.append(
                    asyncio.ensure_future(self.files_to_node_submissions(node_name))
                )

        try:
            results = loop.run_until_complete(asyncio.gather(*tasks))
            submitting_accession_numbers = (
                list(self.all_accession_numbers)
                if RESUBMIT_ALL_METADATA
                else results[0]
            )
            loop.run_until_complete(
                asyncio.gather(
                    self.files_to_virus_genome_run_taxonomy_submission(
                        submitting_accession_numbers
                    )
                )
            )
        finally:
            try:
                if AsyncFileHelper.session:
                    future = AsyncFileHelper.close_session()
                    if future:
                        loop.run_until_complete(asyncio.gather(future))
            finally:
                loop.close()

        # now that we have the SRA metadata ready, check if there is sample
        # data from GenBank to add
        self.process_genbank_manifest()

        end = time.strftime("%X")
        print(f"Processing time: From {start} to {end}")

    def submit_metadata(self):
        start = time.strftime("%X")

        for node, records in self.submitting_data.items():
            print(f"Submitting {node} data: {len(records)} records")
            if isinstance(records, dict):  # samples are in a dict
                records = records.values()
            for _record in records:
                record = {"type": node}
                record.update(_record)
                self.metadata_helper.add_record_to_submit(record)
            self.metadata_helper.batch_submit_records()

        end = time.strftime("%X")
        print(f"Submitting time: From {start} to {end}")

    async def files_to_virus_genome_run_taxonomy_submission(
        self, submitting_accession_numbers
    ):
        """get submitting data for virus_genome_run_taxonomy node

        Same concept as `files_to_node_submissions`, but also parse `sample`
        and `virus_genome` data from BigQuery, and link
        `virus_genome_run_taxonomy` nodes to `virus_genome` nodes."""

        if not submitting_accession_numbers:
            return

        bigquery_records = self._get_response_from_big_query(
            submitting_accession_numbers
        )

        # Keep track of accession numbers with a link to a virus_genome node
        accession_number_set = set()
        for record in bigquery_records:
            if record["acc"] in self.accession_number_to_guids_map:
                accession_number = record["acc"]
                print(f"Get from bigquery response: {accession_number}")
                success = await self._parse_big_query_response(record)
                if success:
                    accession_number_set.add(accession_number)

        cmc = self.submitting_data["core_metadata_collection"][0]
        cmc_submitter_id = cmc["submitter_id"]
        for accession_number in submitting_accession_numbers:
            virus_genome_run_taxonomy_submitter_id = format_submitter_id(
                "virus_genome_run_taxonomy", {"accession_number": accession_number}
            )
            submitted_json = {
                "submitter_id": virus_genome_run_taxonomy_submitter_id,
                "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                "accession_number": accession_number,
                "data_type": "Virus Sequence Run Taxonomy Analysis",
                "data_format": "json",
                "data_category": "Kmer-based Taxonomy Analysis",
            }

            # Add link to virus sequence node
            if accession_number in accession_number_set:
                submitted_json["virus_genomes"] = [
                    {"submitter_id": f"virus_genome_{accession_number}"}
                ]

            filename = f"virus_genome_run_taxonomy_{accession_number}.csv"
            print(f"Get indexd info for '{filename}'")
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
                        f"Cannot get indexd record of {filename}. Retrying... Details:\n  {e}"
                    )

            assert (
                did
            ), f"file '{filename}' does not exist in indexd, rerun NCBI_FILE ETL"

            if not authz:
                tries = 0
                while tries < MAX_RETRIES:
                    try:
                        await self.file_helper.async_update_authz(did=did, rev=rev)
                        break
                    except Exception as e:
                        print(
                            f"Cannot update indexd for {did}. Retrying... Details:\n  {e}"
                        )
                        if tries == MAX_RETRIES:
                            raise e
                        tries += 1

            submitted_json["file_size"] = filesize
            submitted_json["md5sum"] = md5sum
            submitted_json["object_id"] = did
            submitted_json["file_name"] = file_name

            self.submitting_data["virus_genome_run_taxonomy"].append(submitted_json)

    async def files_to_node_submissions(self, node_name):
        """Get submitting data for the node
        (for each accession number, find the indexd record for this node and
        accession number, and map it to the graph. The files should have
        already been indexed by the NBCI_FILE ETL)"""

        tries = 0
        while tries <= MAX_RETRIES:
            try:
                submitting_accession_numbers = (
                    await self.get_submitting_accession_number_list(node_name)
                )
                break
            except Exception as e:
                print(
                    f"Cannot query peregine with {node_name} (try #{tries}). Retrying... Details:\n  {e}."
                )
                if tries == MAX_RETRIES:
                    raise e
                tries += 1

        for accession_number in submitting_accession_numbers:
            submitter_id = format_submitter_id(
                node_name, {"accession_number": accession_number}
            )

            cmc_submitter_id = format_submitter_id("cmc_ncbi_covid19", {})

            contig_submitter_id = format_submitter_id(
                "virus_genome_contig", {"accession_number": accession_number}
            )
            peptide_submitter_id = format_submitter_id(
                "virus_sequence_peptide", {"accession_number": accession_number}
            )
            run_taxonomy_submitter_id = format_submitter_id(
                "virus_genome_run_taxonomy", {"accession_number": accession_number}
            )

            if node_name == "virus_genome_contig":
                submitted_json = {
                    "submitter_id": submitter_id,
                    "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                    "virus_genomes_run_taxonomies": [
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
                    "virus_genome_contigs": [{"submitter_id": contig_submitter_id}],
                    "accession_number": accession_number,
                    "data_type": "Virus Sequence Blastn",
                    "data_format": "tsv",
                    "data_category": "Nucleotide Blast",
                }
            elif node_name == "virus_sequence_peptide":
                submitted_json = {
                    "submitter_id": submitter_id,
                    "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                    "virus_genome_contigs": [{"submitter_id": contig_submitter_id}],
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
            elif node_name == "virus_genome_contig_taxonomy":
                submitted_json = {
                    "submitter_id": submitter_id,
                    "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                    "virus_genome_contigs": [{"submitter_id": contig_submitter_id}],
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

            retrying = True  # TODO MAX_RETRIES
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
                        f"ERROR: Fail to query indexd for {filename}. Retrying... Details:\n  {e}"
                    )
                    await asyncio.sleep(SLEEP_SECONDS)

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
                        print(
                            f"ERROR: Fail to update indexd for {filename}. Retrying... Details:\n  {e}"
                        )
                        if tries == MAX_RETRIES:
                            raise e
                        tries += 1
                        await asyncio.sleep(SLEEP_SECONDS)

            submitted_json["file_size"] = filesize
            submitted_json["md5sum"] = md5sum
            submitted_json["object_id"] = did
            submitted_json["file_name"] = file_name

            self.submitting_data[node_name].append(submitted_json)

        return submitting_accession_numbers

    async def get_submitting_accession_number_list(self, node_name):
        """get list of accession numbers to submit for this node
        (accession numbers that don't exist yet in the graph data)"""

        submitting_accession_numbers = set()
        existing_accession_numbers = (
            await self.data_file.get_existing_accession_numbers(node_name)
        )
        print(
            f"[{node_name}] {len(existing_accession_numbers)} existing accession numbers"
        )

        s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
        key = self.data_file.nodes[node_name][0]
        s3_object = s3.Object(self.data_file.bucket, key)
        line_stream = codecs.getreader("utf-8")
        n_lines = 0
        print(f"[{node_name}] Reading file 's3://{self.data_file.bucket}/{key}'...")
        for line in line_stream(s3_object.get()["Body"]):
            r1 = re.findall("[SDE]RR\d+", line)
            n_lines += 1
            if n_lines % 100000 == 0:
                print(f"[{node_name}] Processed {n_lines} rows")
            if len(r1) == 0:
                continue
            read_accession_number = r1[0]
            if RESUBMIT_ALL_METADATA:
                self.all_accession_numbers.add(read_accession_number)
            if (
                f"{node_name}_{read_accession_number}".lower()
                not in existing_accession_numbers
            ):
                submitting_accession_numbers.add(read_accession_number)

        print(
            f"[{node_name}] {len(submitting_accession_numbers)} new accession numbers to submit"
        )
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
        print("Getting data from BigQuery...")
        assert accession_numbers != [], "accession_numbers is empty"

        start = 0
        offset = 100
        client = bigquery.Client()
        while start < len(accession_numbers):
            print(f"  {start} / {len(accession_numbers)}")
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

        Store in `self.submitting_data["virus_genome"]` and
        `self.submitting_data["sample"]` the data to submit.

        Return True if success

        """

        sra_accession_number = response["acc"]

        sample = {}
        virus_genome = {}

        sample["submitter_id"] = f"sample_sra_{sra_accession_number}"
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
            if field in SRA_SPECIAL_MAP_FIELDS:
                old_name, handler = SRA_SPECIAL_MAP_FIELDS[field]
                sample[field] = handler(response.get(old_name))
            elif field in response:
                sample[field] = str(response.get(field))

        virus_genome["submitter_id"] = f"virus_genome_{sra_accession_number}"
        for field in [
            "assay_type",
            "avgspotlen",
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
            "sra_study",
            "title_sam",
            "release_date",
            "data_format",
            "librarylayout",
        ]:
            if field in SRA_SPECIAL_MAP_FIELDS:
                old_name, handler = SRA_SPECIAL_MAP_FIELDS[field]
                virus_genome[field] = handler(response.get(old_name))
            elif field in response:
                virus_genome[field] = str(response.get(field))

        virus_genome["samples"] = [{"submitter_id": sample["submitter_id"]}]
        virus_genome["data_category"] = "Nucleotide"
        virus_genome["data_type"] = "Sequence"

        for guid in self.accession_number_to_guids_map[sra_accession_number]:
            retrying = True  # TODO MAX_RETRIES
            record = None
            while retrying:
                try:
                    record = await self.file_helper.async_get_indexd_record(guid)
                    retrying = False
                except Exception as e:
                    print(
                        f"ERROR: Fail to get indexd record for {guid}. Retrying... Details:\n  {e}"
                    )
                    await asyncio.sleep(SLEEP_SECONDS)

            if not record:
                print(
                    f"ERROR: file {guid} does not exist in indexd, rerun NCBI_MANIFEST ETL"
                )
                return False

            this_virus_genome = virus_genome.copy()
            this_virus_genome["file_name"] = record["file_name"]
            this_virus_genome["data_format"] = get_file_extension(record["file_name"])

            if not record["authz"]:
                tries = 0
                while tries < MAX_RETRIES:
                    try:
                        await self.file_helper.async_update_authz(
                            did=guid, rev=record["rev"]
                        )
                        break
                    except Exception as e:
                        print(
                            f"ERROR: Fail to update indexd record for {guid}. Retrying... Details:\n  {e}"
                        )
                        if tries == MAX_RETRIES:
                            raise e
                        tries += 1
                        await asyncio.sleep(SLEEP_SECONDS)

            this_virus_genome["file_size"] = record["size"]
            this_virus_genome["md5sum"] = record.get("hashes", {}).get("md5")
            this_virus_genome["object_id"] = guid

            self.submitting_data["virus_genome"].append(this_virus_genome)
        self.submitting_data["sample"][sra_accession_number] = sample
        return True

    def process_genbank_manifest(self):
        genbank_s3_bucket = "sra-pub-sars-cov2-metadata-us-east-1"
        genbank_manifest = "genbank/csv/NCBI-Virus-Metadata.csv"
        covid_species = "Severe acute respiratory syndrome-related coronavirus"
        print(
            f"Selecting covid19 data from GenBank manifest 's3://{genbank_s3_bucket}/{genbank_manifest}'..."
        )

        s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
        s3_object = s3.Object(genbank_s3_bucket, genbank_manifest)
        line_stream = codecs.getreader("utf-8")

        tries = 0
        while tries < MAX_RETRIES:
            try:
                input = line_stream(s3_object.get()["Body"])
                reader = csv.DictReader(input, delimiter=",", quotechar='"')

                for i, row in enumerate(reader):
                    if i % 10000 == 0 and i != 0:
                        print(f"Processed {i} rows")
                    # TODO compare i to last processed row to skip existing data
                    if row["Species"] != covid_species:
                        # we don't care about turnips. skip non-covid data
                        continue
                    gb_sample, gb_virus_sequence = self.genbank_row_to_gen3_records(row)
                    self.submitting_data["virus_sequence"].append(gb_virus_sequence)
                    sra_sample = self.get_sra_sample_to_update(gb_sample)
                    id = f"genbank_{gb_sample['genbank_accession']}"
                    if sra_sample:
                        sample = self.merge_sra_and_genbank_samples(
                            sra_sample, gb_sample
                        )
                        self.submitting_data["sample"][id] = sample
                    else:
                        self.submitting_data["sample"][id] = gb_sample
            except Exception as e:
                print(f"Unable to read GenBank manifest. Retrying... Details:\n  {e}")
                if tries == MAX_RETRIES:
                    raise e
                tries += 1

    def genbank_row_to_gen3_records(self, row):
        records = {
            "sample": {},
            "virus_sequence": {},
        }
        for genbank_prop, genbank_value in row.items():
            if genbank_prop not in GENBANK_TO_GEN3_MAPPING or not genbank_value:
                continue
            mappings = GENBANK_TO_GEN3_MAPPING[genbank_prop]
            if not isinstance(mappings, list):
                mappings = [mappings]
            for mapping in mappings:
                gen3_value = (
                    mapping["transform"](genbank_value)
                    if "transform" in mapping
                    else genbank_value
                )
                records[mapping["node"]][mapping["prop"]] = gen3_value

        # add link
        records["virus_sequence"]["samples"] = [
            {"submitter_id": records["sample"]["submitter_id"]}
        ]

        return records["sample"], records["virus_sequence"]

    def get_sra_sample_to_update(self, sample):
        sra_accession = sample.get("sra_accession")
        if not sra_accession:
            return

        sample_to_update = None
        if sra_accession in self.submitting_data["sample"]:
            sample_to_update = self.submitting_data["sample"][sra_accession]
        else:
            query_string = f"""{{
                sample (project_id: "{self.program_name}-{self.project_code}", sra_accession: "{sra_accession}") {{
                    submitter_id
                }}
            }}"""
            res = self.metadata_helper.query_peregrine(query_string)
            samples = res["data"]["sample"]
            if len(samples) > 1:
                raise Exception(f"Found 2 samples with sra_accession='{sra_accession}'")
            elif len(samples) == 1:
                sample_to_update = samples[0]
        return sample_to_update

    def merge_sra_and_genbank_samples(self, sra_sample, genbank_sample):
        # overwrite SRA metadata with GenBank metadata
        sample = dict(sra_sample, **genbank_sample)
        # update submitter_id. The current value is:
        # `sample_genbank_<genbank accession>`, and we want:
        # `sample_genbank_<genbank accession>_sra_<sra_accession>`
        sample["submitter_id"] += f"_sra_{genbank_sample['sra_accession']}"
        return sample
