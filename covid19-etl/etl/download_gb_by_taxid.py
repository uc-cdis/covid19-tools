import re
import os
import itertools
import numpy
import yaml
from Bio import Entrez
from Bio import SeqIO

from etl import base
from helper.metadata_helper import MetadataHelper

'''
Example NCBI Taxonomy ids:
1042633 (a single record for testing): "Aureopterix sterops"
2697049: "Severe acute respiratory syndrome coronavirus 2"
694009 (parent of 2697049): "Severe acute respiratory syndrome-related coronavirus"
See https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?lvl=0&id=694009

To Do

- Logging. Note that the code just does print()
- Errors. Note that we just do an except()
- Deleting data sets from last job (don't think we need results from every job)

'''

class DOWNLOAD_GB_BY_TAXID(base.BaseETL):

    def __init__(self, base_url, access_token):
        super().__init__(base_url, access_token)
        script = os.path.splitext(os.path.basename(__file__))[0]
        with open('{}.yaml'.format(script)) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        self.email = config['email']
        self.min_len = config['min_len']
        self.max_len = config['max_len']
        self.split = config['split']
        self.recurse = config['recurse']
        self.verbose = config['verbose']
        self.taxid = config['taxid']
        self.seq_format = config['seq_format']
        self.retmax = config['retmax']
        # Getting name and code from the YAML
        self.program_name = config['program_name']
        self.project_code = config['project_code']

        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

    def files_to_submissions(self):
        self.search()
        self.filter()

    def submit_metadata(self):
        self.write()

    def search(self):
        nummatch = re.match(r'^\d+$', str(self.taxid))

        if not nummatch and self.verbose:
            print("String '" + self.taxid + "' is not an NCBI taxon id")
            return

        Entrez.email = self.email

        if self.recurse == True:
            try:
                handle = Entrez.esearch(db="nuccore", 
                                        idtype="acc",
                                        retmax=5000, 
                                        term="txid{}[Organism:exp]".format(self.taxid))
                records = Entrez.read(handle)
                handle.close()
            except (RuntimeError) as exception:
                print("Error retrieving sequence ids using Taxonomy id '" +
                      str(self.taxid) + "'" + str(exception))

            for link in records['IdList']:
                self.nt_ids.append(link)
        else:
            try:
                links = Entrez.read(
                    Entrez.elink(dbfrom="taxonomy",
                                 db="nucleotide",
                                 idtype="acc",
                                 id=self.taxid))
            except (RuntimeError) as exception:
                print("Error retrieving sequence ids using Taxonomy id '" +
                      str(self.taxid) + "'" + str(exception))

            if len(links[0]["LinkSetDb"]) == 0:
                print("No sequences found with id " + self.taxid)
                return

            for link in links[0]["LinkSetDb"][0]["Link"]:
                self.nt_ids.append(link["Id"])

        if self.verbose:
            print("Esearch id count for Taxonomy id {0}: {1}".format(
                self.taxid, len(self.nt_ids)))

        self.efetch()

    def efetch(self):
        Entrez.email = self.email
        # Split the list of ids into batches of 'retmax' size for Entrez
        num_chunks = int(len(self.nt_ids)/self.retmax) + 1

        try:
            for id_chunk in numpy.array_split(numpy.array(self.nt_ids), num_chunks):
                if self.verbose:
                    print("Going to download records: {}".format(id_chunk))
                handle = Entrez.efetch(
                    db="nucleotide",
                    rettype=self.seq_format,
                    retmode="text",
                    id=','.join(id_chunk)
                )
                # Creating the SeqRecord objects here makes filter() easier
                self.records = itertools.chain(
                    self.records, SeqIO.parse(handle, self.seq_format))
        except (RuntimeError) as exception:
            print("Error retrieving sequences using id '" +
                  str(self.taxid) + "':" + str(exception))

    def filter(self):
        # "genomes" are arbitrarily defined as sequences > min_len
        if self.min_len:
            filtered = []
            for record in self.records:
                if len(record) >= self.min_len:
                    filtered.append(record)
            self.records = filtered

    def write(self):
        if self.split:
            for record in self.records:
                seqfile = record.name + '.' + self.seq_format
                #SeqIO.write(record, seqfile, self.seq_format)
                self.metadata_helper.add_record_to_submit(seqfile)
            self.metadata_helper.batch_submit_records()
        else:
            seqfile = 'taxid-' + str(self.taxid) + '.' + self.seq_format
            #SeqIO.write(self.records, seqfile, self.seq_format)
            self.metadata_helper.add_record_to_submit(seqfile)
            self.metadata_helper.batch_submit_records()

