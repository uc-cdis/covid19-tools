# save(fit,
# prediction,
# dates,
# reported_cases,
# deaths_by_country,
# countries,
# estimated.deaths,
# estimated.deaths.cf,
# out,
# covariates,file=paste0('../modelOutput/results/',StanModel,'-',JOBID,'-stanfit.Rdata'))

# ../modelOutput/results/nine_county_big/us_base-606037-stanfit.Rdata

load("../modelOutput/results/nine_county_big/us_base-606037.Rdata")
obs <- read.csv("../modelInput/ILCaseAndMortalityV1.csv")

l <- list()

for(i in 1:length(countries)){

    # county index
    county <- countries[[i]]
    N <- length(dates[[i]])
    countyDates <- dates[[i]]
    lastObs <- tail(dates[[i]], 1)

    # last index is county
    countyForecast <- colMeans(estimated.deaths[,(N+1):N2,i])

    countyObs <- obs[obs$countryterritoryCode==county,]

    # tail(as.Date(countyObs$dateRep, format = "%m/%d/%y"), 1) > lastObs
    validationObs <- countyObs[as.Date(countyObs$dateRep, format = "%m/%d/%y") > lastObs, ]

    # number of points for this county
    n <- min(length(countyForecast), nrow(validationObs))

    # here it is - for one county
    vdf <- data.frame("date"=validationObs$dateRep[1:n], "obs"=validationObs$deaths[1:n], "pred"=countyForecast[1:n])
    vdf$county <- county

    l[[i]] <- vdf
} 

fullSet <- do.call(rbind, l)

# number of points 
pts <- nrow(fullSet)

# compute the score
correlationScore <- cor(fullSet$pred, fullSet$obs)

# error here???
print(sprintf("correlation: %d", correlationScore))


print(sprintf("number of dates: %d", n))
print(sprintf("number of points: %d", pts))

# look at it
png(filename="../modelOutput/explorePlots/firstValidation.png", width=1600, height=1600, units="px", pointsize=36)
plot(fullSet$obs, fullSet$pred)
dev.off()

png(filename="../modelOutput/explorePlots/firstValidation_log.png", width=1600, height=1600, units="px", pointsize=36)
plot(log(fullSet$obs), log(fullSet$pred))
dev.off()