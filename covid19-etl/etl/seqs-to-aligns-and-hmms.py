#!/usr/local/bin/python3

import os.path
import tempfile
import subprocess
import sys
from Bio import AlignIO
from Bio import SeqIO

from etl import base
from helper.metadata_helper import MetadataHelper


def main():
    builder = Seqs_To_Aligns_And_Hmms()
    builder.read()
    builder.make_align()
    builder.make_hmm()

class SEQS_TO_ALIGNS_AND_HMMS:

    def __init__(self, base_url, access_token):
        super().__init__(base_url, access_token)
        script = os.path.splitext(os.path.basename(__file__))[0]
        # Get all input strings from YAML, including program_name, project_code
        with open('{}.yaml'.format(script)) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        self.verbose = config['verbose']
        self.aligner = config['aligner']
        self.hmmbuild = config['hmmbuild']
        self.program_name = config['program_name']
        self.project_code = config['project_code']
        self.data_category = config['data_category']
        self.data_type = config['data_type']
        self.data_format = config['data_format']
        self.source = config['source']
        self.hmm_type = config['hmm_type']
        self.alignment_type = config['alignment_type']
        #
        # TO DO
        #
        self.files = files
        self.seqs, self.alns, self.hmms = dict(), dict(), dict()

    def read(self):
        #
        # TO DO
        #
        full_paths = [os.path.join(os.getcwd(), path) for path in self.files]
        for path in full_paths:
            seqs = []
            # Get baseneme, for example "S-aa"
            name = os.path.basename(path).split('.')[0]
            try:
                # Each one of these is a multiple fasta file
                for index, fa in enumerate(SeqIO.parse(path,"fasta")):
                    seqs.append(fa)
                self.seqs[name] = self.remove_dups(seqs)
            except (RuntimeError) as exception:
                print("Error parsing sequences in '" +
                      str(path) + "':" + str(exception))

    def remove_dups(self, seqs):
        d = dict()
        for seq in seqs:
            d[str(seq.seq)] = seq
        return list(d.values())
        
    def make_align(self):
        for name in self.seqs:
            seqfile = tempfile.NamedTemporaryFile('w', delete=False)
            SeqIO.write(self.seqs[name], seqfile.name, 'fasta')
            if self.verbose:
                print("Alignment input sequence file: {}".format(seqfile.name))
            cmd = self.make_align_cmd(seqfile.name, name)
            if self.verbose:
                print("Alignment command is '{}'".format(cmd))
            try:
                subprocess.run(cmd, check=True)
            except (subprocess.CalledProcessError) as exception:
                print("Error running '{}':".format(self.aligner) + str(exception))
            self.alns[name] = name + '.aln'

    def make_align_cmd(self, infile, name):
        '''
        time muscle -in M-aa.fasta -out M-aa.aln
            real	0m19.963s
            user	0m19.594s
            sys	0m0.238s
        time clustalo -i M-aa.fasta -o x.aln --outfmt=fasta
            real	0m23.045s
            user	0m18.665s
            sys	0m4.255s
        time mafft --auto M-aa.fasta > M-aa.aln
            real	0m2.254s
            user	0m2.036s
            sys	0m0.170s
        '''
        if self.aligner == 'muscle':
            return [self.aligner, '-in', infile, '-out', name + '.aln']
        elif self.aligner == 'mafft':
            return [self.aligner, '--auto', infile, '>', name + '.aln']
        elif self.aligner == 'clustalo':
            return [self.aligner, '-i', infile, '-o', name + '.aln', '--outfmt=fasta']
        else:
            sys.exit("No command for aligner {}".format(self.aligner))

    def make_hmm(self):
        for name in self.alns:
            if self.verbose:
                print("hmmbuild input file is '{}'".format(self.alns[name]))
            # Either --amino or --dna
            opt = '--amino' if '-aa' in name else '--dna'
            subprocess.run([self.hmmbuild, opt, name + '.hmm', self.alns[name]])
            self.hmms[name] = name + '.hmm'

if __name__ == "__main__":
    main()
