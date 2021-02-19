import os
import sys
import subprocess
import shutil
import argparse

##Argument parser
parser = argparse.ArgumentParser(description="Split a multi-fasta file into individual fasta files using the sequence ID as their file name.")
parser.add_argument("-f", "--multifasta", metavar="multifasta", help="Input multi-fasta file", type=str)
args = parser.parse_args()

file_name=args.multifasta

# function to check if directory exists
def ifexist(mydir):
	if os.path.exists(mydir):
		shutil.rmtree(mydir)
	os.makedirs(mydir)

# Makes directories for file creation

output_single_fd="output_single_fasta"

create_dir_list=[output_single_fd]
for d in range(len(create_dir_list)):
	ifexist(create_dir_list[d])



#pull apart multifasta and place them into the output folder
subprocess.call("awk '/^>/{s=++d\".fa\"} {print > s}' %s" % (file_name),shell=True)
subprocess.call("mv *.fa output_single_fasta/", shell=True)

#rename the files with the sequence id
output_files=os.listdir(output_single_fd)

for i in output_files:
    file="output_single_fasta/%s" %(i)
    with open(file) as f:
        first_line = f.readline()
    new_name=first_line[1:]
    new_name=new_name[:-1]
    new_name=new_name.replace("/","_")
    new_name="output_single_fasta/%s.fasta" %(new_name)
    os.rename(file,new_name)

    print(new_name)
