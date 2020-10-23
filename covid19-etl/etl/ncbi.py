import asyncio
import csv
import json
from pathlib import Path
import re

from botocore import UNSIGNED
from botocore.config import Config
import boto3
import codecs
from google.cloud import bigquery

from etl import base
from helper.file_helper import FileHelper
from helper.async_file_helper import AsyncFileHelper
from helper.format_helper import derived_submitter_id, format_submitter_id
from helper.metadata_helper import MetadataHelper
from etl.ncbi_file import NCBI_FILE


def format_location_submitter_id(country):
    """summary_location_<country>"""
    submitter_id = "summary_location_{}".format(country)
    submitter_id = submitter_id.lower().replace(", ", "_")
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)
    return submitter_id.strip("-")


class NCBI(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "NCBI-COVID-19"
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
            "summary_location": [],
            "virus_sequence": [],
            "sample": [],
            "virus_sequence_run_taxonomy": [],
            "core_metadata_collection": [],
            "virus_sequence_contig": [],
            "virus_sequence_blastn": [],
            "virus_sequence_contig_taxonomy": [],
            "virus_sequence_peptide": [],
            "virus_sequence_hmmsearch": [],
        }

    async def get_existed_accession_numbers(self, node_name):
        """
        Get a list of existed accession numbers from the graph

        Args:
            node_name(str): node name
        Returns:
            list(str): list of accession numbers
        """

        query_string = "{ " + node_name + " (first:0) { submitter_id } }"
        response = await self.metadata_helper.query_node_data(query_string)
        records = response["data"][node_name]
        return set([record["submitter_id"] for record in records])

    async def get_submitting_accession_number_list(self, node_name):
        submitting_accession_numbers = set()
        existed_accession_numbers = await self.get_existed_accession_numbers(node_name)
        s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
        s3_object = s3.Object(self.data_file.bucket, self.data_file.nodes[node_name][0])
        line_stream = codecs.getreader("utf-8")

        for line in line_stream(s3_object.get()["Body"]):
            r1 = re.findall("[SDE]RR\d+", line)
            if len(r1) == 0:
                continue
            read_accession_number = r1[0]
            if f"{node_name}_{read_accession_number}" not in existed_accession_numbers:
                submitting_accession_numbers.add(read_accession_number)

        return list(submitting_accession_numbers)

    async def files_to_virus_sequence_run_taxonomy_submission(self):

        virus_sequence_run_taxonomy_submitter_id = format_submitter_id(
            "virus_sequences_run_taxonomy_submitter_id", {}
        )
        cmc_submitter_id = format_submitter_id("cmc_ncbi", {})

        submitted_json = {
            "submitter_id": virus_sequence_run_taxonomy_submitter_id,
            "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
            "virus_sequence_contigs": [{"submitter_id": contig_submitter_id}],
            "accession_number": accession_number,
            "data_type": "Contig Taxonomy",
            "data_format": "json",
            "data_category": "Kmer-based Taxonomy Analysis of Contigs",
        }
        self.submitting_data["virus_sequence_run_taxonomy"].append(submitted_json)

    async def files_to_node_submissions(self, node_name):
        """Get submitting data for the node"""

        submitting_accession_numbers = await self.get_submitting_accession_number_list(
            node_name
        )

        for accession_number in submitting_accession_numbers:
            submitter_id = format_submitter_id(
                node_name, {"accession_number": accession_number}
            )

            cmc_submitter_id = format_submitter_id("cmc_ncbi", {})

            contig_submitter_id = format_submitter_id(
                "virus_sequence_contig", {"accession_number": accession_number}
            )
            blastn_submitter_id = format_submitter_id(
                "virus_sequence_blastn", {"accession_number": accession_number}
            )
            contig_taxonomy_submitter_id = format_submitter_id(
                "virus_sequence_contig_taxonomy", {"accession_number": accession_number}
            )
            peptide_submitter_id = format_submitter_id(
                "virus_sequence_peptide", {"accession_number": accession_number}
            )
            hmmsearch_submitter_id = format_submitter_id(
                "virus_sequence_hmmsearch", {"accession_number": accession_number}
            )

            if node_name == "virus_sequence_contig":
                submitted_json = {
                    "submitter_id": submitter_id,
                    "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                    "virus_sequences_run_taxonomies": [
                        {"submitter_id": "virus_sequences_run_taxonomy_submitter_id"}
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
            elif node_name == "virus_sequence_hmmsearch":
                submitted_json = {
                    "submitter_id": submitter_id,
                    "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                    "virus_sequence_peptides": [{"submitter_id": peptide_submitter_id}],
                    "accession_number": accession_number,
                    "data_type": "Virus Sequence HMM Search",
                    "data_format": "json",
                    "data_category": "HMMER Scab of Contigs",
                }
            else:
                raise Exception(f"ERROR: {node_name} does not exist")

            (
                did,
                rev,
                md5sum,
                filesize,
            ) = await self.async_file_helper.async_find_by_name(filename=filename)
            did, rev, md5sum, filesize = did, rev, md5sum, filesize

            assert (
                did
            ), f"file {node_name} does not exist in the index, rerun NCBI_FILE ETL"
            self.file_helper.async_update_authz(did=did, rev=rev)

            submitted_json["file_size"] = filesize
            submitted_json["md5sum"] = md5sum
            submitted_json["object_id"] = did

            self.submitting_data[node_name].append(submitted_json)

    def _get_response_from_big_query(self, accession_numbers):
        """Get data from big query"""

        stm = 'SELECT * FROM `nih-sra-datastore`.sra.metadata where consent = "public"'

        assert (accession_numbers, []), "accession_numbers need to be not empty"
        stm = stm + f' and (acc = "{accession_numbers[0]}"'
        for accession_number in accession_numbers[1:]:
            stm = stm + f' or acc like "accession_number"'
        stm = stm + ")"

        client = bigquery.Client()
        query_job = client.query(stm)

        results = query_job.result()  # Waits for job to complete.
        records = [dict(row) for row in results]
        return records

    def _parse_big_query_response(self, response):
        """Parse the big query response"""

        accession_number = response["acc"]

        sample = {}
        virus_sequence = {}
        summary_location = {}

        sample["submitter_id"] = response["sample_name"]
        sample["projects"] = ([{"code": self.project_code}],)
        sample["collection_date"] = response["collection_date_sam"]
        sample["ncbi_bioproject"] = response["bioproject"]
        sample["ncbi_biosample"] = response["biosample"]
        sample["sample_accession"] = response["sample_acc"]
        for field in [
            "biosamplemodel_sam",
            "host_associated_environmental_package_sam",
            "organism",
        ]:
            sample[field] = response[field]

        virus_sequence["submitter_id"] = f"virus_sequence_{accession_number}"
        for field in [
            "acc",
            "assay_type",
            "avgspotlen",
            "bytes",
            "center_name",
            "consent",
            "datastore_filetype",
            "datastore_provider",
            "datastore_region",
            "description_sam",
            "ena_checklist_sam" "ena_first_public_run",
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
            "librarylayout",
            "libraryselection",
            "librarysource",
            "mbases",
            "mbytes",
            "platform",
            "releasedate",
            "sra_accession_sam",
            "sra_study",
            "title_sam",
        ]:
            virus_sequence[field] = response[field]
        virus_sequence["samples"] = [{"samples": sample["submitter_id"]}]

        summary_location["submitter_id"] = format_location_submitter_id(
            response["geo_loc_name_country_calc"]
        )
        summary_location["projects"] = [{"code": self.project_code}]
        summary_location["country_region"] = response["geo_loc_name_country_calc"]
        summary_location["continent"] = response["geo_loc_name_country_continent_calc"]
        summary_location["latitude"] = response["geographic_location__latitude__sam"]
        summary_location["longitude"] = response["geographic_location__longitude__sam"]

        self.submitting_data["virus_sequence"].append(virus_sequence)
        self.submitting_data["summary_location"].append(summary_location)
        self.submitting_data["sample"].append(sample)

    def submit_metadata(self):

        for node_name in []:
            self.files_to_submissions(node_name)
        return

        # for node_name, value in self.nodes.items():
        #     key = value[0]
        #     headers = value[1] if len(value) > 1 else None

        #     lists = []
        #     ext = re.search("\.(.*)$", key).group(1)
        #     tasks.append(
        #         asyncio.ensure_future(
        #             self.index_ncbi_data_file(node_name, ext, key, set(lists), headers)
        #         )
        #     )

        # print("Submitting data...")

        # for k, v in self.nodes.items():
        #     submitter_id_exist = []
        #     print(f"Submitting {k} data...")
        #     for node in v:
        #         node_record = {"type": k}
        #         node_record.update(node)
        #         submitter_id = node_record["submitter_id"]
        #         if submitter_id not in submitter_id_exist:
        #             submitter_id_exist.append(submitter_id)
        #             self.metadata_helper.add_record_to_submit(node_record)
        #     self.metadata_helper.batch_submit_records()
