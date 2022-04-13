library(tidyverse)
library(readr)
library(stringi)

#NOTE: This files has been updated as of the 2021-04-19 dataset which included values within the pangolin_lineage property.


#Before running this script, file submission must occur:
#Run the "split_multifasta.py" script to transform the multifasta into a directory with single files.
#Using the gen3-client upload these files to the commons for mapping. Use the following general command: `./gen3-client upload --profile=<Profile> --upload-path=directory/path/to/output_single_fasta/.`
#On the gen3 commons, go to submit data. First create a core_metadata_collection entity under the project, "Walder-SIU-SARS-CoV2", with the following submitter_id pattern: "SIU-SARS-CoV2_<year-month-day>"
#Go to the "map my files page" and map the files in your account. Select the project "Walder-SIU-SARS-CoV2", select the node "virus sequence", file type is "fasta", data_category is "nucleotide", data_type is "Sequence", and the core_metadata_collection entity will be the one that was just created.
#After mapping, go to the project page <url/Walder-SIU-SARS-CoV2>, click on the "virus_sequence" node and then download the tsv to the directory designated in "path_to_files". 
#At this point run this script.

###########################
#
# Edit this section
#
###########################


#Paste in your file directory path to where your file lives
path_to_files="COVID19/SIU_data/Metadata/"

#Input needed for the metadata file and the name of the CMC node submitter_id
metadata_file="2022-01-10.csv"
cmc="SIU-SARS-CoV2_2022-01-10"




#############################
#
# Code below, do not change
#
#############################



#Read in files
df=read_csv(file = paste(path_to_files,metadata_file,sep=""))

df_vs_in=read_tsv(file = paste(path_to_files,"virus_sequence.tsv",sep=""))

#Filter out the columns based on their CMC submitter_id, thus allowing for new submissions (df_vs), while keeping old submissions (df_vs_old) to be tacked on later.

df_vs=df_vs_in[grep(pattern = cmc,x = df_vs_in$core_metadata_collections.submitter_id),]
df_vs_old=df_vs_in[!grepl(pattern = cmc,x = df_vs_in$core_metadata_collections.submitter_id),]

#This will remove full columns that contain no information.
df_vs=df_vs[colSums(!is.na(df_vs))>0]
df_vs_old=df_vs_old[colSums(!is.na(df_vs_old))>0]
df=df[colSums(!is.na(df))>0]

#Convert all column headers into DD properties
df_new=mutate(df, submitter_id=strain, collection_date=date,continent=region,country_region=country,province_state=division,county=location,sequence_length=length,host_age=age,host_sex=sex,nextstrain_clade=Nextstrain_clade,submitting_lab_PI=authors)%>%select(-strain,-date,-region,-country,-division,-location,-length,-age,-sex,-Nextstrain_clade,-authors,-host_age,-host_sex)

#This is a new property that was filled out in a more recent metadata file
df_new=select(df_new,-date_submitted)

#Change the host="human" to host="Homo sapiens"
df_new$host[grep(pattern = "Human",df$host)]<-"Homo sapiens"

#Remove "/" in submitter_id names
df_new$submitter_id=str_replace_all(df_new$submitter_id,"/","_")

#Add columns for sample submission.
df_sample=select(df_new,-sequence_length,-nextstrain_clade,-pangolin_lineage)%>%mutate(type="sample",projects.code="SIU-SARS-CoV2")

#Fix County Names
df_sample$county=substr(df_sample$county,1,nchar(df_sample$county)-13)

#This splits apart the submitter_id with the auto generated information from the gen3-client for the samples.submitter_id.
df_vs_new=mutate(df_vs,samples.submitter_id=submitter_id,project_id="Walder-SIU-SARS-CoV2")%>%separate(samples.submitter_id,c("Nothing","samples.submitter_id"),sep = "Walder-SIU-SARS-CoV2_")%>%select(-id,-core_metadata_collections.id,-Nothing)

#This cuts off the end of the submitter_id that was made by the gen3-client for the sample submitter_id.
df_vs_new$samples.submitter_id=substr(df_vs_new$samples.submitter_id,1,nchar(df_vs_new$samples.submitter_id)-5)

#pull metadata from raw data for virus sequence node.
df_vs_clin=select(df_new,submitter_id,sequence_length,nextstrain_clade,pangolin_lineage)%>%mutate(samples.submitter_id=submitter_id)%>%select(-submitter_id)

#join the existing virus sequence node in the commons with the metadata from the raw data.
df_vs_sub=left_join(df_vs_new,df_vs_clin)

#Write out both files to TSV format.
write_tsv(df_sample,paste(path_to_files,"sample_new_submission.tsv",sep=""), na="")
write_tsv(df_vs_sub,paste(path_to_files,"vs_new_submission.tsv",sep=""), na="")


#After the script has run, submit the two new files on the project page <url/Walder-SIU-SARS-CoV2> using the "upload file" option. 
#Submit the "sample_new_submission.tsv" first, followed by the "vs_new_submission.tsv".


###############################
#
# NOTES
#
###############################

#The following samples and data files have been removed from the SIU datasets:
# hCoV-19/USA/IL-IDPH-COO-I-000961/2021 (mismatch between county and zipcode)
# 
# 
# 
# 
# 
