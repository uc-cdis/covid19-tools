import csv
from pathlib import Path
import re

from botocore import UNSIGNED
from botocore.config import Config
import boto3
import codecs

from etl import base
from helper.file_helper import FileHelper
from helper.format_helper import derived_submitter_id, format_submitter_id
from helper.metadata_helper import MetadataHelper


fields_mapping = {
    # "patientid": ("subject", "submitter_id", None),
    "offset": ("follow_up", "offset", int),
    "age": ("demographic", "age", int),
    "temperature": ("follow_up", "temperature", float),
    "other_notes": ("imaging_file", "other_notes", None),
}


class NCBI(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "NCBI"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.file_helper = FileHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.nodes = {
            "core_metadata_collection": [],
            "study": [],
            "subject": [],
            "observation": [],
            "follow_up": [],
            "demographic": [],
            "imaging_file": [],
        }

    def files_to_submissions(self):
        cmc_submitter_id = format_submitter_id("cmc_ncbi", {})
        nodes = {
            "core_metadata_collection": {
                "submitter_id": cmc_submitter_id,
                "projects": [{"code": self.project_code}],
            }
        }
        latest_drr_number = 0
        latest_err_number = 0
        latest_srr_number = 0
        current_drr_number = 0
        current_err_number = 0
        current_srr_number = 0

        s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
        s3_object = s3.Object(
            "sra-pub-sars-cov2-metadata-us-east-1", "contigs/contigs.json"
        )
        line_stream = codecs.getreader("utf-8")
        for line in line_stream(s3_object.get()["Body"]):
            r1 = re.findall("[SDE]RR\d+", line)
            if len(r1) != 1:
                raise Exception("Unexpected format of contigs.json")
            accession_number = r1[0]
            print(accession_number)
            counter = int(accession_number[3:])
            if (
                "DRR" in accession_number
                and counter < latest_drr_number
                or counter == current_drr_number
            ):
                continue
            if (
                "ERR" in accession_number
                and counter < latest_err_number
                or counter == current_err_number
            ):
                continue
            if (
                "SRR" in accession_number
                and counter < latest_srr_number
                or counter == current_srr_number
            ):
                continue
            if "DRR" in accession_number:
                current_drr_number = counter
            elif "ERR" in accession_number:
                current_err_number = counter
            elif "SRR" in accession_number:
                current_srr_number = counter
            self.parse_accession_number(accession_number, cmc_submitter_id)

    def parse_accession_number(self, accession_number, cmc_submitter_id):
        run_taxonomy_submitter_id = format_submitter_id(
            "virus_sequence_run_taxonomy", {"accession_number": {accession_number}}
        )
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

        nodes = {
            "virus_sequence_run_taxonomy": {
                "submitter_id": run_taxonomy_submitter_id,
                "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                "accession_number": accession_number,
                "data_type": "Virus Sequence Blastn",
                "data_format": "csv",
                "data_category": "Kmer-based Taxonomy Analysis",
            },
            "virus_sequence_contig": {
                "submitter_id": contig_submitter_id,
                "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                "virus_sequences_run_taxonomies": [
                    {"submitter_id": run_taxonomy_submitter_id}
                ],
                "accession_number": accession_number,
                "data_type": "Virus Sequence Contig",
                "data_format": "json",
                "data_category": "Nucleotide Contig",
            },
            "virus_sequence_blastn": {
                "submitter_id": blastn_submitter_id,
                "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                "virus_sequence_contigs": [{"submitter_id": contig_submitter_id}],
                "accession_number": accession_number,
                "data_type": "Virus Sequence Blastn",
                "data_format": "tsv",
                "data_category": "Nucleotide Blast",
            },
            "virus_sequence_contig_taxonomy": {
                "submitter_id": contig_taxonomy_submitter_id,
                "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                "virus_sequence_contigs": [{"submitter_id": contig_submitter_id}],
                "accession_number": accession_number,
                "data_type": "Contig Taxonomy",
                "data_format": "json",
                "data_category": "Kmer-based Taxonomy Analysis of Contigs",
            },
            "virus_sequence_peptide": {
                "submitter_id": peptide_submitter_id,
                "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                "virus_sequence_contigs": [{"submitter_id": contig_submitter_id}],
                "accession_number": accession_number,
                "data_type": "Peptides Annotation Using VIGOR3",
                "data_format": "json",
                "data_category": "Peptides Annotation",
            },
            "virus_sequence_hmmsearch": {
                "submitter_id": hmmsearch_submitter_id,
                "core_metadata_collections": [{"submitter_id": cmc_submitter_id}],
                "virus_sequence_peptides": [{"submitter_id": peptide_submitter_id}],
                "accession_number": accession_number,
                "data_type": "Virus Sequence HMM Search",
                "data_format": "json",
                "data_category": "HMMER Scab of Contigs",
            },
        }

        for node_name, _ in nodes.items():
            ext = nodes[node_name]["data_format"]
            filename = f"{node_name}_{accession_number}.{ext}"
            # did, rev, md5sum, filesize = self.file_helper.find_by_name(
            #     filename=filename
            # )
            did, rev, md5sum, filesize = "test_did", "", "test_md5sum", 9999
            assert (
                did
            ), f"file {node_name} does not exist in the index, rerun NCBI_FILE ETL"
            # self.file_helper.update_authz(did=did, rev=rev)
            nodes[node_name]["file_size"] = filesize
            nodes[node_name]["md5sum"] = md5sum
            nodes[node_name]["object_id"] = did

    def submit_metadata(self):
        print("Submitting data...")

        for k, v in self.nodes.items():
            submitter_id_exist = []
            print(f"Submitting {k} data...")
            for node in v:
                node_record = {"type": k}
                node_record.update(node)
                submitter_id = node_record["submitter_id"]
                if submitter_id not in submitter_id_exist:
                    submitter_id_exist.append(submitter_id)
                    self.metadata_helper.add_record_to_submit(node_record)
            self.metadata_helper.batch_submit_records()
