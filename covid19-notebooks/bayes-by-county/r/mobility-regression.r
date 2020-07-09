library(lubridate)
library(modelr)
library(broom)
library(tidyverse)
library(visdat)
library(mlr3verse)

source("./read-mobility.r")

forecast_googleMob<-function(codeToName){
  # read data
  
  # google
  google_data <- read_google_mobility(codeToName=codeToName, regression=TRUE)
  il <- as.data.frame("Illinois")
  colnames(il) <- "sub_region_1"
  google_data <- left_join(google_data, il, by = c("sub_region_1"))

  # new foursquare data 
  new_foursquare = read_csv("../modelInput/mobility/visit-data/visitdata-grouped.csv")
  new_foursquare$categoryname=as.factor(new_foursquare$categoryname)

  # c("Airport" ,  "Arts & Entertainment"        "Banks"                      
  # [5] "Beach"                       "Big Box Stores"              "Bus"                         "Colleges & Universities"    
  # [9] "Convenience Store"           "Discount Stores"             "Drug Store"                  "Events"                     
  # [13] "Fast Food Restaurants"       "Fitness Center"              "Food"                        "Gas Stations"               
  # [17] "Government"                  "Grocery"                     "Gun Shops"                   "Hardware Stores"            
  # [21] "Hotel"                       "Light Rail Stations"         "Medical"                     "Metro Stations"             
  # [25] "Nightlife Spots"             "Office"                      "Outdoors & Recreation"       "Professional & Other Places"
  # [29] "Residences"                  "School"                      "Shops & Services"            "Skiing"                     
  # [33] "Spiritual Center"            "Sports"                      "Travel & Transport"    

  sfsq <- new_foursquare %>% 
          filter(categoryname!="Skiing",demo=="All") %>% 
          select(c(-demo,-county,-p50Duration)) # revisit p50

  # notice: visit data is only by state, not by county -> this is okay
  # just apply the same state-level signal to every county
  # just need to generate a functional table at this point
  google_cleaned <- google_data %>% 
          select(date,state=sub_region_1,retail.recreation,grocery.pharmacy,parks,transitstations,workplace,residential) %>% 
          filter(state!="")

  print("google last obs:")
  print(tail(google_cleaned$date, 1))
  print("visitdata last obs:")
  print(tail(sfsq$date, 1))

  mobility_data <- left_join(sfsq,google_cleaned, by = c("state" = "state", "date" = "date"))
  
  #mobility_data <- mobility_data %>% #  filter(!(state %in% c("Alaska", "District of Columbia", "Hawaii")))
  mobility_data <- mobility_data %>%     
          pivot_wider(names_from = categoryname , values_from = c(avgDuration,visits))
  mobility_data$state <- as.factor(mobility_data$state)
  names(mobility_data)<- make.names(names(mobility_data),unique = TRUE)

  # check fit on last week
  last_google = ymd((mobility_data %>% filter(!is.na(retail.recreation)) %>% summarise(last(date)))[[1,1]])
  train_stop = last_google-6
  
  # foursquare broken then # should be fine
  mobility_data = mobility_data %>% filter(date!=ymd("2020-04-19"))
  
  # need to remove date for mlr 
  mobility_data = mobility_data %>% arrange(date)
  mobility_data$id = c(1:nrow(mobility_data))
  
  train_end_id = (mobility_data %>% filter(date==train_stop) %>% summarise(last(id)))[[1,1]]
  google_end_id = (mobility_data %>% filter(date==last_google) %>% summarise(last(id)))[[1,1]]
  dates = mobility_data$date
  mobility_data = mobility_data %>% select(c(-date,-categoryid))

  # revisit imputation
  mobility_data=mobility_data %>% fill(names(mobility_data),.direction = 'updown')

  # convert single-item list cols to numeric  
  tmp <- sapply(mobility_data, class)
  mobility_data[tmp == "list"] <- sapply(mobility_data[tmp == "list"], function(l) l[1])

  # retail.recreation
  task = TaskRegr$new(id = "retail.recreation", backend = as.data.frame(mobility_data %>%
          select(c(-"grocery.pharmacy",-"parks",-"transitstations",-"workplace",-"residential"))), target = "retail.recreation")
  learner = lrn("regr.ranger")
  train_set = c(1:train_end_id)
  test_set = c((train_end_id+1):google_end_id)
  learner$train(task, row_ids = train_set)
  prediction = learner$predict(task, row_ids = test_set)
  abs(prediction$truth-prediction$response)
  df_lastweek = data.frame(id=prediction$row_ids,retail.recreation=(abs(prediction$truth-prediction$response)))
  train_set = c(1:google_end_id)
  test_set = setdiff(seq_len(task$nrow), train_set)
  learner$train(task, row_ids = train_set)
  prediction = learner$predict(task, row_ids = test_set)
  mobility_data[test_set,"retail.recreation"] = prediction$response

  # grocery.pharmacy    
  task = TaskRegr$new(id = "grocery.pharmacy", backend = as.data.frame(mobility_data %>%
                                                                          select(c(-"retail.recreation",-"parks",-"transitstations",-"workplace",-"residential"))),
                      target ="grocery.pharmacy")
  learner=lrn("regr.ranger")
  train_set = c(1:train_end_id)
  test_set = c((train_end_id+1):google_end_id)
  learner$train(task, row_ids = train_set)
  prediction = learner$predict(task, row_ids = test_set)
  abs(prediction$truth-prediction$response)
  df_lastweek=cbind(df_lastweek,data.frame(grocery.pharmacy=(abs(prediction$truth-prediction$response)) ))
  train_set = c(1:google_end_id)
  test_set = setdiff(seq_len(task$nrow), train_set)
  learner$train(task, row_ids = train_set)
  prediction = learner$predict(task, row_ids = test_set)
  mobility_data[test_set,"grocery.pharmacy"]=prediction$response
  
  # parks  
  task = TaskRegr$new(id = "parks", backend = as.data.frame(mobility_data %>%
                                                              select(c(-"retail.recreation",-"grocery.pharmacy",-"transitstations",-"workplace",-"residential"))),
                      target ="parks")
  learner=lrn("regr.ranger")
  train_set = c(1:train_end_id)
  test_set = c((train_end_id+1):google_end_id)
  learner$train(task, row_ids = train_set)
  prediction = learner$predict(task, row_ids = test_set)
  abs(prediction$truth-prediction$response)
  df_lastweek=cbind(df_lastweek,data.frame(parks=(abs(prediction$truth-prediction$response)) ))
  train_set = c(1:google_end_id)
  test_set = setdiff(seq_len(task$nrow), train_set)
  learner$train(task, row_ids = train_set)
  prediction = learner$predict(task, row_ids = test_set)
  mobility_data[test_set,"parks"]=prediction$response
  
  # transitstations
  task = TaskRegr$new(id = "transitstations", backend = as.data.frame(mobility_data %>%
                                                                        select(c(-"retail.recreation",-"grocery.pharmacy",-"parks",-"workplace",-"residential"))),
                      target ="transitstations")
  learner=lrn("regr.ranger")
  train_set = c(1:train_end_id)
  test_set = c((train_end_id+1):google_end_id)
  learner$train(task, row_ids = train_set)
  prediction = learner$predict(task, row_ids = test_set)
  abs(prediction$truth-prediction$response)
  df_lastweek=cbind(df_lastweek,data.frame(transitstations=(abs(prediction$truth-prediction$response)) ))
  train_set = c(1:google_end_id)
  test_set = setdiff(seq_len(task$nrow), train_set)
  learner$train(task, row_ids = train_set)
  prediction = learner$predict(task, row_ids = test_set)
  
  mobility_data[test_set,"transitstations"]=prediction$response

  # workplace  
  task = TaskRegr$new(id = "workplace", backend = as.data.frame(mobility_data %>%
                                                                  select(c(-"retail.recreation",-"grocery.pharmacy",-"parks",-"transitstations",-"residential"))),
                      target ="workplace")
  learner=lrn("regr.ranger")
  train_set = c(1:train_end_id)
  test_set = c((train_end_id+1):google_end_id)
  learner$train(task, row_ids = train_set)
  prediction = learner$predict(task, row_ids = test_set)
  abs(prediction$truth-prediction$response)
  df_lastweek=cbind(df_lastweek,data.frame(workplace=(abs(prediction$truth-prediction$response)) ))
  train_set = c(1:google_end_id)
  test_set = setdiff(seq_len(task$nrow), train_set)
  learner$train(task, row_ids = train_set)
  prediction = learner$predict(task, row_ids = test_set)
  mobility_data[test_set,"workplace"]=prediction$response

  # residential
  task = TaskRegr$new(id = "residential", backend = as.data.frame(mobility_data %>%
                                                                  select(c(-"retail.recreation",-"grocery.pharmacy",-"parks",-"transitstations",-"workplace"))),
                      target ="residential")
  learner=lrn("regr.ranger")
  train_set = c(1:train_end_id)
  test_set = c((train_end_id+1):google_end_id)
  learner$train(task, row_ids = train_set)
  prediction = learner$predict(task, row_ids = test_set)
  abs(prediction$truth-prediction$response)
  df_lastweek=cbind(df_lastweek,data.frame(residential=(abs(prediction$truth-prediction$response)) ))
  train_set = c(1:google_end_id)
  test_set = setdiff(seq_len(task$nrow), train_set)
  learner$train(task, row_ids = train_set)
  prediction = learner$predict(task, row_ids = test_set)
  mobility_data[test_set,"residential"]=prediction$response

  mobility_data$date=dates
  
  # error analysis
  df_raw_error=copy(df_lastweek)
  print("Error last week")
  print(df_raw_error %>% summarise(retail.recreation=mean(retail.recreation),
                            grocery.pharmacy=mean(grocery.pharmacy),
                            parks=mean(parks),transitstations=mean(transitstations),
                            workplace=mean(workplace),residential=mean(residential)
                            ))
  df_lastweek=df_lastweek %>% pivot_longer(  c("retail.recreation","grocery.pharmacy","parks","transitstations","workplace","residential")
,names_to = "cat",values_to = "error")
  
  return(mobility_data)
}
  
# case-mortality table
d <- read.csv("../modelInput/ILCaseAndMortalityV1.csv", stringsAsFactors = FALSE)
d$countryterritoryCode <- sapply(d$countryterritoryCode, as.character)
d$countryterritoryCode <- sub("840", "", d$countryterritoryCode)
codeToName <- unique(data.frame("countyCode" = d$countryterritoryCode, "countyName" = d$countriesAndTerritories))

f <- forecast_googleMob(codeToName=codeToName)
google_forecast <- f %>% select(state,retail.recreation,grocery.pharmacy,parks,transitstations,workplace,residential,date)
write.csv(google_forecast, "../modelInput/mobility/google-mobility-forecast.csv", row.names = FALSE)