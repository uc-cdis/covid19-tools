library(tidyverse)
library(readr)
library(zipcodeR)
library(usdata)

#Read in file
df=read_csv(file = "COVID19/COV-824/UCMC_PSR_QUARERLY_2021-03-18.csv", guess_max = 100000)


#Double check that there are no subjects with two values for a race field
#df_race_check=mutate(df,race_num=race_asian+race_aian+race_black+race_nhpi+race_white+race_other+race_unk)%>%select(person_id, race_num)
#unique(df_race_check$race_num)

#Add columns that do not exist
df=mutate(df,race=NA,
          gender=NA,
          ethnicity=NA,
          age_unit=NA,
          acuterespdistress=NA,
          diagother=NA,
          symp_res=NA, 
          hospitalized_status=NA,
          icu_status=NA,
          ventilator_status=NA,
          ventilator_duration=mechvent_dur,
          ecmo=NA,
          vital_status=NA,
          symptoms=NA,
          diabetes=NA,
          liverdis=NA,
          smoking_status=NA,
          smoke_former=NA,
          res_zip=zip,
          symp_onset_dt=onset_dt,
          )


#Transform numeric values to enumerated values for the DD 
#change race from multiple columns to one "race" column
df$race[grep(pattern = "1",x = df$race_aian)]<-"American Indian or Alaskan Native"
df$race[grep(pattern = "1",x = df$race_black)]<-"Black"
df$race[grep(pattern = "1",x = df$race_nhpi)]<-"Native Hawaiian Other Pacific Islander"
df$race[grep(pattern = "1",x = df$race_white)]<-"White"
df$race[grep(pattern = "1",x = df$race_other)]<-"Other"
df$race[grep(pattern = "1",x = df$race_unk)]<-"Unknown"
df$race[grep(pattern = "1",x = df$race_asian)]<-"Asian"

#transform sex to gender property
df$gender[grep(pattern = "1", x = df$sex)]<-"Male"
df$gender[grep(pattern = "2", x = df$sex)]<-"Female"
df$gender[grep(pattern = "3", x = df$sex)]<-"Other"
df$gender[grep(pattern = "9", x = df$sex)]<-"Unknown"

#transform Ethnicity to ethnicity property
df$ethnicity[grep(pattern = "1",x = df$ethnicity)]<-"Hispanic/Latino"
df$ethnicity[grep(pattern = "0",x = df$ethnicity)]<-"Non-Hispanic/Latino"
df$ethnicity[grep(pattern = "9",x = df$ethnicity)]<-"Not specified"

#convert days to years
for (x in 1:dim(df)[1]){
  if ((df$ageunit[x]==3)==TRUE){
    if ((df$age[x]<365)==TRUE){
      df$age[x]=0
    }else{
      df$age[x]=round(df$age[x]/365,digits=0)
    }
    df$ageunit[x]=1
  }else{
    }
}

#transform AgeUnit to age_unit property
df$age_unit[grep(pattern = "1",x = df$ageunit)]<-"Years"
df$age_unit[grep(pattern = "2",x = df$ageunit)]<-"Months"
df$age_unit[grep(pattern = "3",x = df$ageunit)]<-"Days"

#transform age (>89 to 89)
#df$age[grep(pattern = "> 89",x = df$age)]<-"89"

#change enum values for pna_yn property
df$pna_yn[grep(pattern = "1",x = df$pna_yn)]<-"True"
df$pna_yn[grep(pattern = "0",x = df$pna_yn)]<-"False"
df$pna_yn[grep(pattern = "9",x = df$pna_yn)]<-"Unknown"

#change enum values for cvd_yn property
df$cvd_yn[grep(pattern = "1",x = df$cvd_yn)]<-"True"
df$cvd_yn[grep(pattern = "0",x = df$cvd_yn)]<-"False"
df$cvd_yn[grep(pattern = "9",x = df$cvd_yn)]<-"Unknown"

#transform acuterespdistress_yn to acuterespdistress property
df$acuterespdistress[grep(pattern = "1",x = df$acuterespdistress_yn)]<-"True"
df$acuterespdistress[grep(pattern = "0",x = df$acuterespdistress_yn)]<-"False"
df$acuterespdistress[grep(pattern = "9",x = df$acuterespdistress_yn)]<-"Unknown"

#transform diag_other_yn to diagother property
df$diagother[grep(pattern = "1",x = df$diagother)]<-"True"
df$diagother[grep(pattern = "0",x = df$diagother)]<-"False"
df$diagother[grep(pattern = "9",x = df$diagother)]<-"Unknown"

#change enum values for sympstatus property
df$sympstatus[grep(pattern = "1",x = df$sympstatus)]<-"Symptomatic"
df$sympstatus[grep(pattern = "0",x = df$sympstatus)]<-"Asymptomatic"
df$sympstatus[grep(pattern = "9",x = df$sympstatus)]<-"Unknown"

#change enum values for onset_unk property to fit data dictionary setup
df$onset_unk[grep(pattern = "NULL",x = df$onset_unk)]<-NA
df$onset_unk[grep(pattern = "1",x = df$onset_unk)]<-"Unknown"

#transform symp_res_yn to symp_res property
df$symp_res[grep(pattern = "1",x = df$symp_res_yn)]<-"Still symptomatic"
df$symp_res[grep(pattern = "0",x = df$symp_res_yn)]<-"Symptoms resolved, unknown date"
df$symp_res[grep(pattern = "9",x = df$symp_res_yn)]<-"Unknown symptom status"

#transform hosp_yn to hospitalized_status
df$hospitalized_status[grep(pattern = "1",x = df$hosp_yn)]<-"True"
df$hospitalized_status[grep(pattern = "0",x = df$hosp_yn)]<-"False"
df$hospitalized_status[grep(pattern = "9",x = df$hosp_yn)]<-"Unknown"

#transform icu_yn to icu_status
df$icu_status[grep(pattern = "1",x = df$icu_yn)]<-"True"
df$icu_status[grep(pattern = "0",x = df$icu_yn)]<-"False"
df$icu_status[grep(pattern = "9",x = df$icu_yn)]<-"Unknown"

#transform mechvent_yn to ventilator_status
df$ventilator_status[grep(pattern = "1",x = df$mechvent_yn)]<-"True"
df$ventilator_status[grep(pattern = "0",x = df$mechvent_yn)]<-"False"
df$ventilator_status[grep(pattern = "9",x = df$mechvent_yn)]<-"Unknown"

#change NULL values in ventilator_duration to ""
df$ventilator_duration[grep(pattern = "NULL",x = df$ventilator_duration)]<-NA

#transform mechvent_yn to ventilator_status
df$ecmo[grep(pattern = "1",x = df$ecmo_yn)]<-"True"
df$ecmo[grep(pattern = "0",x = df$ecmo_yn)]<-"False"
df$ecmo[grep(pattern = "9",x = df$ecmo_yn)]<-"Unknown"

#transform death_yn to vital_status in the subject node
df$vital_status[grep(pattern = "1",x = df$death_yn)]<-"Dead"
df$vital_status[grep(pattern = "0",x = df$death_yn)]<-"Alive"
df$vital_status[grep(pattern = "9",x = df$death_yn)]<-"Unknown"

#change enum value for death_dt to be "" instead of NULL
df$death_dt[grep(pattern = "NULL",x = df$death_dt)]<-NA
df$death_dt[grep(pattern = "1",x = df$death_unk)]<-"Unknown"

#change enum value for symp_onset_dt to be "" instead of NULL
df$symp_onset_dt[grep(pattern = "NULL",x = df$symp_onset_dt)]<-NA

#rework all the symptom properties (fever_yn, sfever_yn, chills_yn, myalgia_yn, runnose_yn, sthroat_yn, cough_yn, sob_yn, nauseavomit_yn, headache_yn, abdom_yn, diarrhea_yn) into one symptom property

#treat both fever_yn and sfever_yn the same and combine to create one fever symptom based on the DD.
df$fever_yn[grep(pattern = "0",x = df$fever_yn)]<-NA
df$fever_yn[grep(pattern = "9",x = df$fever_yn)]<-NA

df$sfever_yn[grep(pattern = "0",x = df$sfever_yn)]<-NA
df$sfever_yn[grep(pattern = "9",x = df$sfever_yn)]<-NA

df$chills_yn[grep(pattern = "0",x = df$chills_yn)]<-NA
df$chills_yn[grep(pattern = "9",x = df$chills_yn)]<-NA

df=mutate(df, fever=paste(fever_yn,sfever_yn,chills_yn,sep=""))

df$fever[grep(pattern = "1",x = df$fever)]<-"Fever or feeling feverish/chills"
df$fever[grep(pattern = "NA",x = df$fever)]<-NA

#myalgia_yn for symptom property
df$myalgia_yn[grep(pattern = "0",x = df$myalgia_yn)]<-NA
df$myalgia_yn[grep(pattern = "9",x = df$myalgia_yn)]<-NA
df$myalgia_yn[grep(pattern = "1",x = df$myalgia_yn)]<-"Myalgia"

#runnose_yn for symptom property
df$runnose_yn[grep(pattern = "0",x = df$runnose_yn)]<-NA
df$runnose_yn[grep(pattern = "9",x = df$runnose_yn)]<-NA
df$runnose_yn[grep(pattern = "NULL",x = df$runnose_yn)]<-NA
df$runnose_yn[grep(pattern = "1",x = df$runnose_yn)]<-"Runny or stuffy nose"

#sthroat_yn for symptom property
df$sthroat_yn[grep(pattern = "0",x = df$sthroat_yn)]<-NA
df$sthroat_yn[grep(pattern = "9",x = df$sthroat_yn)]<-NA
df$sthroat_yn[grep(pattern = "1",x = df$sthroat_yn)]<-"Sore throat"

#cough_yn for symptom property
df$cough_yn[grep(pattern = "0",x = df$cough_yn)]<-NA
df$cough_yn[grep(pattern = "9",x = df$cough_yn)]<-NA
df$cough_yn[grep(pattern = "1",x = df$cough_yn)]<-"Cough"

#sob_yn for symptom property
df$sob_yn[grep(pattern = "0",x = df$sob_yn)]<-NA
df$sob_yn[grep(pattern = "9",x = df$sob_yn)]<-NA
df$sob_yn[grep(pattern = "1",x = df$sob_yn)]<-"Shortness of breath"

#nauseavomit_yn for symptom property
df$nauseavomit_yn[grep(pattern = "0",x = df$nauseavomit_yn)]<-NA
df$nauseavomit_yn[grep(pattern = "9",x = df$nauseavomit_yn)]<-NA
df$nauseavomit_yn[grep(pattern = "1",x = df$nauseavomit_yn)]<-"Nausea and vomiting"

#headache_yn for symptom property
df$headache_yn[grep(pattern = "0",x = df$headache_yn)]<-NA
df$headache_yn[grep(pattern = "9",x = df$headache_yn)]<-NA
df$headache_yn[grep(pattern = "1",x = df$headache_yn)]<-"Headaches"

#abdom_yn for symptom property
df$abdom_yn[grep(pattern = "0",x = df$abdom_yn)]<-NA
df$abdom_yn[grep(pattern = "9",x = df$abdom_yn)]<-NA
df$abdom_yn[grep(pattern = "1",x = df$abdom_yn)]<-"Abdominal pain"

#diarrhea_yn for symptom property
df$diarrhea_yn[grep(pattern = "0",x = df$diarrhea_yn)]<-NA
df$diarrhea_yn[grep(pattern = "9",x = df$diarrhea_yn)]<-NA
df$diarrhea_yn[grep(pattern = "1",x = df$diarrhea_yn)]<-"Diarrhea"

df=mutate(df,symptoms=paste(fever, myalgia_yn, runnose_yn, sthroat_yn, cough_yn, sob_yn, nauseavomit_yn, headache_yn, abdom_yn, diarrhea_yn, sep=","))

df$symptoms=str_replace_all(string = df$symptoms, pattern ="NA,",replacement = "" )
df$symptoms=str_replace_all(string = df$symptoms, pattern =",NA",replacement = "" )
df$symptoms=str_replace_all(string = df$symptoms, pattern ="NA",replacement = "" )

# cld_yn enum change
df$cld_yn[grep(pattern = "1",x = df$cld_yn)]<-"True"
df$cld_yn[grep(pattern = "0",x = df$cld_yn)]<-"False"
df$cld_yn[grep(pattern = "9",x = df$cld_yn)]<-"Unknown"

#diabetes_yn to diabetes property
df$diabetes[grep(pattern = "1",x = df$diabetes_yn)]<-"True"
df$diabetes[grep(pattern = "0",x = df$diabetes_yn)]<-"False"
df$diabetes[grep(pattern = "9",x = df$diabetes_yn)]<-"Unknown"

#renaldis_yn enum change
df$renaldis_yn[grep(pattern = "1",x = df$renaldis_yn)]<-"True"
df$renaldis_yn[grep(pattern = "0",x = df$renaldis_yn)]<-"False"
df$renaldis_yn[grep(pattern = "9",x = df$renaldis_yn)]<-"Unknown"

#liverdis_yn to liverdis property
df$liverdis[grep(pattern = "1",x = df$liverdis_yn)]<-"True"
df$liverdis[grep(pattern = "0",x = df$liverdis_yn)]<-"False"
df$liverdis[grep(pattern = "9",x = df$liverdis_yn)]<-"Unknown"

#immsup_yn enum change
df$immsupp_yn[grep(pattern = "1",x = df$immsupp_yn)]<-"True"
df$immsupp_yn[grep(pattern = "0",x = df$immsupp_yn)]<-"False"
df$immsupp_yn[grep(pattern = "9",x = df$immsupp_yn)]<-"Unknown"

#neuro_yn enum change
df$neuro_yn[grep(pattern = "1",x = df$neuro_yn)]<-"True"
df$neuro_yn[grep(pattern = "0",x = df$neuro_yn)]<-"False"
df$neuro_yn[grep(pattern = "9",x = df$neuro_yn)]<-"Unknown"

# #pregnant_yn to pregnancy_status property
# df$pregnancy_status[grep(pattern = "1",x = df$pregnancy_status)]<-"True"
# df$pregnancy_status[grep(pattern = "0",x = df$pregnancy_status)]<-"False"
# df$pregnancy_status[grep(pattern = "9",x = df$pregnancy_status)]<-"Unknown"

#smok_current_yn to smoking_status property
df$smoking_status[grep(pattern = "1",x = df$smoke_curr_yn)]<-"True"
df$smoking_status[grep(pattern = "0",x = df$smoke_curr_yn)]<-"False"
df$smoking_status[grep(pattern = "9",x = df$smoke_curr_yn)]<-"Unknown"

#smok_former_yn to smoke_former property
df$smoke_former[grep(pattern = "1",x = df$smoke_former_yn)]<-"True"
df$smoke_former[grep(pattern = "0",x = df$smoke_former_yn)]<-"False"
df$smoke_former[grep(pattern = "9",x = df$smoke_former_yn)]<-"Unknown"

#remove NULL from pos_spec_dt
df$pos_spec_dt[grep(pattern = "NULL",x = df$pos_spec_dt)]<-""

#change enum values for pos_spec_unk to fit data dictionary setup
df$pos_spec_unk[grep(pattern = "NULL",x = df$pos_spec_unk)]<-""
df$pos_spec_unk[grep(pattern = "1",x = df$pos_spec_unk)]<-"Unknown"

#resp_flua_ag enum change
df$resp_flua_ag[grep(pattern = "1",x = df$resp_flua_ag)]<-"Positive"
df$resp_flua_ag[grep(pattern = "2",x = df$resp_flua_ag)]<-"Negative"
df$resp_flua_ag[grep(pattern = "3",x = df$resp_flua_ag)]<-"Pending"
df$resp_flua_ag[grep(pattern = "4",x = df$resp_flua_ag)]<-"Not Done"

#resp_flub_ag enum change
df$resp_flub_ag[grep(pattern = "1",x = df$resp_flub_ag)]<-"Positive"
df$resp_flub_ag[grep(pattern = "2",x = df$resp_flub_ag)]<-"Negative"
df$resp_flub_ag[grep(pattern = "3",x = df$resp_flub_ag)]<-"Pending"
df$resp_flub_ag[grep(pattern = "4",x = df$resp_flub_ag)]<-"Not Done"

#resp_flua_pcr enum change
df$resp_flua_pcr[grep(pattern = "1",x = df$resp_flua_pcr)]<-"Positive"
df$resp_flua_pcr[grep(pattern = "2",x = df$resp_flua_pcr)]<-"Negative"
df$resp_flua_pcr[grep(pattern = "3",x = df$resp_flua_pcr)]<-"Pending"
df$resp_flua_pcr[grep(pattern = "4",x = df$resp_flua_pcr)]<-"Not Done"

#resp_flub_pcr enum change
df$resp_flub_pcr[grep(pattern = "1",x = df$resp_flub_pcr)]<-"Positive"
df$resp_flub_pcr[grep(pattern = "2",x = df$resp_flub_pcr)]<-"Negative"
df$resp_flub_pcr[grep(pattern = "3",x = df$resp_flub_pcr)]<-"Pending"
df$resp_flub_pcr[grep(pattern = "4",x = df$resp_flub_pcr)]<-"Not Done"

#resp_rsv enum change
df$resp_rsv[grep(pattern = "1",x = df$resp_rsv)]<-"Positive"
df$resp_rsv[grep(pattern = "2",x = df$resp_rsv)]<-"Negative"
df$resp_rsv[grep(pattern = "3",x = df$resp_rsv)]<-"Pending"
df$resp_rsv[grep(pattern = "4",x = df$resp_rsv)]<-"Not Done"

#resp_hm enum change
df$resp_hm[grep(pattern = "1",x = df$resp_hm)]<-"Positive"
df$resp_hm[grep(pattern = "2",x = df$resp_hm)]<-"Negative"
df$resp_hm[grep(pattern = "3",x = df$resp_hm)]<-"Pending"
df$resp_hm[grep(pattern = "4",x = df$resp_hm)]<-"Not Done"

#resp_pi enum change
df$resp_pi[grep(pattern = "1",x = df$resp_pi)]<-"Positive"
df$resp_pi[grep(pattern = "2",x = df$resp_pi)]<-"Negative"
df$resp_pi[grep(pattern = "3",x = df$resp_pi)]<-"Pending"
df$resp_pi[grep(pattern = "4",x = df$resp_pi)]<-"Not Done"

#resp_adv enum change
df$resp_adv[grep(pattern = "1",x = df$resp_adv)]<-"Positive"
df$resp_adv[grep(pattern = "2",x = df$resp_adv)]<-"Negative"
df$resp_adv[grep(pattern = "3",x = df$resp_adv)]<-"Pending"
df$resp_adv[grep(pattern = "4",x = df$resp_adv)]<-"Not Done"

#resp_rhino enum change
df$resp_rhino[grep(pattern = "1",x = df$resp_rhino)]<-"Positive"
df$resp_rhino[grep(pattern = "2",x = df$resp_rhino)]<-"Negative"
df$resp_rhino[grep(pattern = "3",x = df$resp_rhino)]<-"Pending"
df$resp_rhino[grep(pattern = "4",x = df$resp_rhino)]<-"Not Done"

#resp_cov enum change
df$resp_cov[grep(pattern = "1",x = df$resp_cov)]<-"Positive"
df$resp_cov[grep(pattern = "2",x = df$resp_cov)]<-"Negative"
df$resp_cov[grep(pattern = "3",x = df$resp_cov)]<-"Pending"
df$resp_cov[grep(pattern = "4",x = df$resp_cov)]<-"Not Done"

#resp_rcp enum change
df$resp_rcp[grep(pattern = "1",x = df$resp_rcp)]<-"Positive"
df$resp_rcp[grep(pattern = "2",x = df$resp_rcp)]<-"Negative"
df$resp_rcp[grep(pattern = "3",x = df$resp_rcp)]<-"Pending"
df$resp_rcp[grep(pattern = "4",x = df$resp_rcp)]<-"Not Done"

#resp_mp enum change
df$resp_mp[grep(pattern = "1",x = df$resp_mp)]<-"Positive"
df$resp_mp[grep(pattern = "2",x = df$resp_mp)]<-"Negative"
df$resp_mp[grep(pattern = "3",x = df$resp_mp)]<-"Pending"
df$resp_mp[grep(pattern = "4",x = df$resp_mp)]<-"Not Done"

#othrp enum change
df$othrp[grep(pattern = "1",x = df$othrp)]<-"Positive"
df$othrp[grep(pattern = "2",x = df$othrp)]<-"Negative"
df$othrp[grep(pattern = "3",x = df$othrp)]<-"Pending"
df$othrp[grep(pattern = "4",x = df$othrp)]<-"Not Done"

#remove NULL from res_zip property
df$res_zip[grep(pattern = "NULL",x = df$res_zip)]<-NA


#remove unneeded columns
df=select(df, 
          -race_aian,
          -race_black,
          -race_nhpi,
          -race_white,
          -race_other,
          -race_unk,
          -race_asian,
          -acuterespdistress_yn,
          -symp_res_yn,
          -symp_res_dt,
          -hosp_yn,
          -icu_yn,
          -mechvent_yn,
          -mechvent_dur,
          -ecmo_yn,
          -death_yn,
          -sfever_yn,
          -fever_yn,
          -fever,
          -chills_yn, 
          -myalgia_yn, 
          -runnose_yn, 
          -sthroat_yn, 
          -cough_yn, 
          -sob_yn, 
          -nauseavomit_yn, 
          -headache_yn, 
          -abdom_yn, 
          -diarrhea_yn,
          -diabetes_yn,
          -liverdis_yn,
          -zip,
          -smoke_curr_yn,
          -smoke_former_yn,
          -sex,
          -ageunit,
          -death_unk,
          -race_spec,
          -losssmelltaste_yn,
          -chestpain_yn,
          -medcond_yn,
          -onset_dt,
          -onset_unk
          )

x=zip_code_db
x$res_zip=substr(x$zipcode,1,3)
y=unique(select(x,res_zip,state))
y$state=abbr2state(y$state)
y=mutate(y, province_state=state, country_region="United States")%>%select(-state)

df=left_join(df,y)

df_subject=select(df,person_id,vital_status,res_zip)%>%mutate(submitter_id=person_id,type="subject", projects.code="Chicagoland_COVID-19_Subjects")%>%select(-person_id)

df_demo=select(df,person_id,race,gender,ethnicity,age,age_unit,province_state,country_region)%>%mutate(submitter_id=paste(person_id,"demo",sep = "_"),subjects.submitter_id=person_id,type="demographic")%>%select(-person_id)

df_obs=select(df,-race,-gender,-ethnicity,-age,-age_unit,-vital_status,-res_zip,-province_state,-country_region)%>%mutate(submitter_id=paste(person_id,"obs",sep = "_"),subjects.submitter_id=person_id,type="observation")%>%select(-person_id)


df_subject=df_subject[colSums(!is.na(df_subject))>0]
df_demo=df_demo[colSums(!is.na(df_demo))>0]
df_obs=df_obs[colSums(!is.na(df_obs))>0]


df_subject=unique(df_subject)
df_demo=unique(df_demo)
df_obs=unique(df_obs)

write_tsv(x = df_subject,file = "COVID19/COV-824/submission/UChicago_Clinical_2021_03_18_subject_submission.tsv",na="")
write_tsv(x = df_demo,file = "COVID19/COV-824/submission/UChicago_Clinical_2021_03_18_demographic_submission.tsv",na="")
write_tsv(x = df_obs,file = "COVID19/COV-824/submission/UChicago_Clinical_2021_03_18_observation_submission.tsv",na="")