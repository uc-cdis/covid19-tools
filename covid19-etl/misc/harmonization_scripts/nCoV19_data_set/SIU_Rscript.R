library(tidyverse)
library(readr)
library(stringi)

#Paste in your file directory path to where your file lives
path_to_files="enter/file/path"

df=read_csv(file = paste(path_to_files,"Gagnon_Chicago.csv",sep=""))

df_vs=read_tsv(file = paste(path_to_files,"virus_sequence.tsv",sep=""))

#This will remove full columns that contain no information.
df_vs=df_vs[colSums(!is.na(df_vs))>0]
df=df[colSums(!is.na(df))>0]

#Convert all column headers into DD properties
df_new=mutate(df, submitter_id=strain, collection_date=date,continent=region,country_region=country,province_state=division,county=location,sequence_length=length,host_age=age,host_sex=sex,nextstrain_clade=Nextstrain_clade,submitting_lab_PI=authors)%>%select(-strain,-date,-region,-country,-division,-location,-length,-age,-sex,-Nextstrain_clade,-authors,-host_age,-host_sex)

#Remove "/" in submitter_id names
df_new$submitter_id=str_replace_all(df_new$submitter_id,"/","_")

#Add columns for sample submission.
df_sample=select(df_new,-sequence_length,-nextstrain_clade)%>%mutate(type="sample",projects.code="SIU-SARS-CoV2")

#Fix County Names
df_sample_new$county=substr(df_sample_new$county,1,nchar(df_sample_new$county)-13)

#This splits apart the submitter_id with the auto generated information from the gen3-client for the samples.submitter_id.
df_vs_new=mutate(df_vs,samples.submitter_id=submitter_id)%>%separate(samples.submitter_id,c("Nothing","samples.submitter_id"),sep = "Walder-SIU-SARS-CoV2_")%>%select(-id,-project_id,-core_metadata_collections.id,-Nothing)

#This cuts off the end of the submitter_id that was made by the gen3-client for the sample submitter_id.
df_vs_new$samples.submitter_id=substr(df_vs_new$samples.submitter_id,1,nchar(df_vs_new$samples.submitter_id)-5)

#pull metadata from raw data for virus sequence node.
df_vs_clin=select(df_new,submitter_id,sequence_length,nextstrain_clade)%>%mutate(samples.submitter_id=submitter_id)%>%select(-submitter_id)

#join the existing virus sequence node in the commons with the metadata from the raw data.
df_vs_sub=left_join(df_vs_new,df_vs_clin)

#Write out both files to TSV format.
write_tsv(df_sample,paste(path_to_files,"sample_new_submission.tsv",sep=""), na="")
write_tsv(df_vs_sub,paste(path_to_files,"vs_new_submission.tsv",sep=""), na="")
