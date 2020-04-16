library(tidyverse)
library(readr)

df=tbl_df(read.csv(file = "COVID19/nCoV19_data_set/COVID19_line_list_data.csv"))

ttable=read_tsv("COVID19/nCoV19_data_set/nCoV2019_symptoms_harmonization/Symptoms Harmonization - Sheet1.tsv")

df <- df[,colSums(is.na(df))<nrow(df)]

colnames(df)=tolower(str_replace(string = colnames(df),pattern = "\\.",replacement = "_"))

df=mutate(df,submitter_id=paste(country,id,sep = "_"),type="subject",projects.code="nCoV2019",symptoms=symptom)

demo=select(df,submitter_id,gender)%>%mutate(type="demographic",subjects.submitter_id=submitter_id, submitter_id=paste(submitter_id,"_demo",sep=""))
  
subjects=select(df,-gender,-id)

##################
#
#Replace symptoms with harmonized symptoms
#There is one case of "feve\" that throws an error and was manually fixed.
##################


for (y in 1:length(subjects$symptoms)){
  string_vec=unique(trimws(unlist(str_split(subjects$symptoms[y],pattern = ","))))
  
  for (s in 1:length(string_vec)){
    rep_nums=grep(pattern = string_vec[s],x = ttable$property)[1]
    
    string_vec[s]=str_replace(string = string_vec[s],pattern = ttable$property[rep_nums],replacement = ttable$`harmonized property`[rep_nums])

    
  }
  string_vec=unique(string_vec)
  string_final=string_vec[1]
  if (length(string_vec)>1){
    for (l in 2:length(string_vec)){
      string_final=paste(string_final,string_vec[l],sep = ",")
    }
  }else{
    print("skip")
  }
  subjects$symptoms[y]=string_final
}

###########
#
#Harmonize visiting_wuhan, from_wuhan, death, If_onset_approximated, converting 1/0 to T/F
#
###########

subjects$visiting_wuhan[grep(pattern = 0,x = subjects$visiting_wuhan)]<-"False"
subjects$visiting_wuhan[grep(pattern = 1,x = subjects$visiting_wuhan)]<-"True"

subjects$from_wuhan[grep(pattern = 0,x = subjects$from_wuhan)]<-"False"
subjects$from_wuhan[grep(pattern = 1,x = subjects$from_wuhan)]<-"True"

# There are a few instances of dates instead of 1/0 and these were assigned to a 1, for True, by manual means
subjects$death[grep(pattern = 0,x = subjects$death)]<-"False"
subjects$death[grep(pattern = 1,x = subjects$death)]<-"True"

subjects$if_onset_approximated[grep(pattern = 0,x = subjects$if_onset_approximated)]<-"False"
subjects$if_onset_approximated[grep(pattern = 1,x = subjects$if_onset_approximated)]<-"True"


write_tsv(x = subject, path = "COVID19/nCoV19_data_set/COVID19_line_list_subject_submission.tsv",na="")

write_tsv(x = demo, path = "COVID19/nCoV19_data_set/COVID19_line_list_demo_submission.tsv",na="")
