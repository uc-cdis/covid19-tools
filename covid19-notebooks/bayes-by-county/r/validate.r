# save(JOBID,
# nStanIterations,
# duration,
# fit,
# prediction,
# dates,
# reported_cases,
# deaths_by_country,
# countries,
# estimated.deaths,
# estimated.deaths.cf,
# out,
# lastObs,
# covariate_list_partial_county,
# file=paste0('../modelOutput/results/',StanModel,'-',JOBID,'-stanfit.Rdata'))

library(jsonlite)

args <- commandArgs(trailingOnly = TRUE)
filename2 <- args[1]
load(paste0("../modelOutput/results/", filename2))
# print(sprintf("loading: %s",paste0("../modelOutput/results/",filename2)))

obs <- read.csv("../modelInput/ILCaseAndMortalityV1.csv")
obs$date = as.Date(obs$dateRep,format='%m/%d/%y')
obs$countryterritoryCode <- sapply(obs$countryterritoryCode, as.character)
obs$countryterritoryCode <- sub("840", "", obs$countryterritoryCode)

l <- list()

for(i in 1:length(countries)){

    # county index
    county <- countries[[i]]
    N <- length(dates[[i]])
    countyDates <- dates[[i]]

    # last index is county
    countyForecast <- colMeans(estimated.deaths[,(N+1):N2,i])

    countyObs <- obs[obs$countryterritoryCode == county,]
    validationObs <- countyObs[countyObs$date > lastObs, ]

    # number of points for this county
    n <- min(length(countyForecast), nrow(validationObs))

    vdf <- data.frame("date"=validationObs$date[1:n], "obs"=validationObs$deaths[1:n], "pred"=countyForecast[1:n])
    vdf$county <- county

    l[[i]] <- vdf
}

# could save this df - not sure how necessary that is though
fullSet <- do.call(rbind, l)

# look at it
# print(fullSet)

# number of points
pts <- nrow(fullSet)

# compute the score
correlationScore <- cor(fullSet$pred, fullSet$obs)

## write results

outDir <- file.path("../modelOutput/validation", JOBID)
dir.create(outDir, showWarnings = FALSE)

# create summary
summary <- list(
    jobid=JOBID,
    time=duration, # in seconds
    nIter=nStanIterations,
    start=min(fullSet$date),
    end=max(fullSet$date),
    nDays=n,
    nCounties=length(countries),
    deathsCutoff=minimumReportedDeaths,
    nPoints=pts,
    correlation=correlationScore
)
exportJSON <- toJSON(summary, pretty=TRUE, auto_unbox=TRUE)
# sent to stdout
print("--- validation summary ---")
print(exportJSON)
# write summary to log
write(exportJSON, file.path(outDir, "log.json"))

# save plot
png(filename=file.path(outDir, "v.png"), width=1600, height=1600, units="px", pointsize=36)
plot(fullSet$obs, fullSet$pred, sub=sprintf("correlation: %f", correlationScore))
naught <- dev.off()

# save log plot (sometimes this is easier to look at)
png(filename=file.path(outDir, "v_logScale.png"), width=1600, height=1600, units="px", pointsize=36)
plot(log(fullSet$obs), log(fullSet$pred), sub=sprintf("correlation: %f", correlationScore))
naught <- dev.off()
