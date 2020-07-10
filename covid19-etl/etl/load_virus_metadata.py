import glob
import hashlib
from os import path
import yaml

from etl import base
from helper.metadata_helper import MetadataHelper

"""
This script assumes you have a collection of Genbank-format "genome" files (*.gb),
fasta-format sequence files (*.fasta), fasta-format alignment files (*.aln),
and HMM files (*.hmm). You could create these files using these scripts:

https://github.com/bioteam/covid-bioinformatics

Execute this script within the dir containing this collection of files.
(or modify the read() method). This script reads metadata values from the
load_virus_metadata.yaml file which should be in the same directory as this script.
"""


CURRENT_DIR = path.dirname(path.realpath(__file__))


class LOAD_VIRUS_METADATA(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        # Get all input strings from YAML
        script = path.splitext(path.basename(__file__))[0].strip("/")
        print(CURRENT_DIR, script)
        script = path.join(CURRENT_DIR, script + ".yaml")
        with open(script) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)

        self.verbose = config["verbose"]
        self.program_name = config["program_name"]
        self.project_code = config["project_code"]
        self.virus_genome_data_category = config["virus_genome_data_category"]
        self.virus_genome_data_type = config["virus_genome_data_type"]
        self.virus_genome_data_format = config["virus_genome_data_format"]
        self.virus_genome_source = config["virus_genome_source"]
        self.virus_genome_type = config["virus_genome_type"]
        self.virus_sequence_type = config["virus_sequence_type"]
        self.virus_sequence_data_type = config["virus_sequence_data_type"]
        self.virus_sequence_data_format = config["virus_sequence_data_format"]
        self.virus_sequence_alignment_type = config["virus_sequence_alignment_type"]
        self.virus_sequence_alignment_data_type = config[
            "virus_sequence_alignment_data_type"
        ]
        self.virus_sequence_alignment_data_format = config[
            "virus_sequence_alignment_data_format"
        ]
        self.virus_sequence_alignment_tool = config["virus_sequence_alignment_tool"]
        self.virus_sequence_hmm_type = config["virus_sequence_hmm_type"]
        self.virus_sequence_hmm_data_type = config["virus_sequence_hmm_data_type"]
        self.virus_sequence_hmm_data_format = config["virus_sequence_hmm_data_format"]
        self.virus_genomes = []
        self.virus_sequences = []
        self.virus_sequence_alignments = []
        self.virus_sequence_hmms = []

        self.metadata_helper = MetadataHelper(
            base_url=base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

    def checksum(self, filename):
        with open(filename, "rb") as f:
            bytes = f.read()
        return hashlib.md5(bytes).hexdigest()

    def files_to_submissions(self):
        latest_submitted_date = (
            self.metadata_helper.get_latest_submitted_data_virus_genome()
        )
        today = datetime.date.today()
        if latest_submitted_date == today:
            print("Nothing to submit: today and latest submitted date are the same.")
            return

    def submit_metadata(self):
        latest_submitted_date = (
            self.metadata_helper.get_latest_submitted_data_virus_genome()
        )
        today = datetime.date.today()
        if latest_submitted_date == today:
            print("Nothing to submit: today and latest submitted date are the same.")
            return
        self.read()
        self.write()

    def read(self):
        self.genomes = glob.glob("*.gb", recursive=False)
        self.seqs = glob.glob("*.fasta", recursive=False)
        self.alns = glob.glob("*.aln", recursive=False)
        self.hmms = glob.glob("*.hmm", recursive=False)

    def write(self):
        # Genomes
        for genome in self.genomes:
            virus_genome_submitter_id = genome.replace(".", "_")
            virus_genome = {
                "data_category": self.virus_genome_data_category,
                "data_type": self.virus_genome_data_type,
                "data_format": self.virus_genome_data_format,
                "source": self.virus_genome_source,
                "submitter_id": virus_genome_submitter_id,
                "file_name": genome,
                "md5sum": self.checksum(genome),
                "file_size": path.getsize(genome),
                "projects": [{"code": self.project_code}],
            }
            self.virus_genomes.append(virus_genome)

        if self.verbose:
            print("Submitting virus_genome data")
        for genome in self.virus_genomes:
            genome_record = {"type": self.virus_genome_type}
            genome_record.update(genome)
            self.metadata_helper.add_record_to_submit(genome_record)
        self.metadata_helper.batch_submit_records()

        # Sequences
        for seq in self.seqs:
            virus_sequence_id = seq.replace(".", "_")
            # Data Category: Protein or Nucleotide
            seqtype = "Protein" if "-aa.fasta" in seq else "Nucleotide"
            virus_sequence = {
                "data_category": seqtype,
                "data_type": self.virus_sequence_data_type,
                "data_format": self.virus_sequence_data_format,
                "submitter_id": virus_sequence_id,
                "file_name": seq,
                "md5sum": self.checksum(seq),
                "file_size": path.getsize(seq),
                "projects": [{"code": self.project_code}],
            }
            self.virus_sequences.append(virus_sequence)

        if self.verbose:
            print("Submitting virus_sequence data")
        for seq in self.virus_sequences:
            seq_record = {"type": self.virus_sequence_type}
            seq_record.update(seq)
            self.metadata_helper.add_record_to_submit(seq_record)
        self.metadata_helper.batch_submit_records()

        # Alignments
        for aln in self.alns:
            virus_sequence_alignment_id = aln.replace(".", "_")
            # Data Category: Protein or Nucleotide
            seqtype = "Protein" if "-aa.aln" in aln else "Nucleotide"
            virus_sequence_alignment = {
                "data_category": seqtype,
                "data_type": self.virus_sequence_alignment_data_type,
                "data_format": self.virus_sequence_alignment_data_format,
                "submitter_id": virus_sequence_alignment_id,
                "file_name": aln,
                "md5sum": self.checksum(aln),
                "file_size": path.getsize(aln),
                "projects": [{"code": self.project_code}],
                "alignment_tool": self.virus_sequence_alignment_tool,
            }
            self.virus_sequence_alignments.append(virus_sequence_alignment)

        if self.verbose:
            print("Submitting virus_sequence_alignment data")
        for aln in self.virus_sequence_alignments:
            aln_record = {"type": self.virus_sequence_alignment_type}
            aln_record.update(aln)
            self.metadata_helper.add_record_to_submit(aln_record)
        self.metadata_helper.batch_submit_records()

        # HMMs
        for hmm in self.hmms:
            virus_sequence_hmm_id = hmm.replace(".", "_")
            # Data Category: Protein or Nucleotide
            seqtype = "Protein" if "-aa.hmm" in hmm else "Nucleotide"
            virus_sequence_hmm = {
                "data_category": seqtype,
                "data_type": self.virus_sequence_hmm_data_type,
                "data_format": self.virus_sequence_hmm_data_format,
                "submitter_id": virus_sequence_hmm_id,
                "file_name": hmm,
                "md5sum": self.checksum(hmm),
                "file_size": path.getsize(hmm),
                "projects": [{"code": self.project_code}],
            }
            self.virus_sequence_hmms.append(virus_sequence_hmm)

        if self.verbose:
            print("Submitting virus_sequence_hmm data")
        for hmm in self.virus_sequence_hmms:
            hmm_record = {"type": self.virus_sequence_hmm_type}
            hmm_record.update(hmm)
            self.metadata_helper.add_record_to_submit(hmm_record)
        self.metadata_helper.batch_submit_records()
