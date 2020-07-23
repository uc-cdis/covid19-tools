# read in soc-ec table
se <- read.csv("../modelInput/SocEc.csv", stringsAsFactors=FALSE)

# select what you want
se <- data.frame(
    fips = se$FIPS,
    state = se$State,
    area_name = se$Area_Name,
    pop = se$POP_ESTIMATE_2018,
    ## vars ##
    # income
    income = se$Median_Household_Income_2018,
    # density
    density = se$Density.per.square.mile.of.land.area...Population,
    # transit
    transit = se$transit_scores...population.weighted.averages.aggregated.from.town.city.level.to.county,
    # nICU beds
    icu_beds = se$ICU.Beds
)

# filter for IL
il <- se[se$state == "IL", ]

# remove all rows with any na
se <- se[complete.cases(se), ]

## normalize scores

# density
maxDensity <- max(se$density, na.rm=TRUE)
se$ndensity <- sapply(se$density, function(x) x/maxDensity)

# income
maxIncome <- max(se$income, na.rm=TRUE)
se$nincome <- sapply(se$income, function(x) x/maxIncome)

# transit
maxTransit <- max(se$transit, na.rm=TRUE)
se$ntransit <- sapply(se$transit, function(x) x/maxTransit)

# nICU beds
maxICU <-  max(se$icu_beds, na.rm=TRUE)
se$nicu_beds <- sapply(se$icu_beds, function(x) x/maxICU)

# normalized scores
ns <- data.frame(
    density=se$ndensity,
    income=se$nincome,
    transit=se$ntransit,
    icu=se$nicu_beds
)

# raw scores
rs <- data.frame(
    density=se$density,
    income=se$income,
    transity=se$transit,
    icu=se$icu_beds
)

##### mortality

# case-mortality table
d <- read.csv("../modelInput/ILCaseAndMortalityV1.csv", stringsAsFactors = FALSE)
d$date = as.Date(d$dateRep,format='%m/%d/%y')
d$countryterritoryCode <- sapply(d$countryterritoryCode, as.character)
d$countryterritoryCode <- sub("840", "", d$countryterritoryCode)

codeToName <- unique(data.frame("countyCode" = d$countryterritoryCode, "countyName" = d$countriesAndTerritories))
countries <- unique(d$countryterritoryCode)

##### mobility

# Read google mobility
source("./read-mobility.r")
mobility <- read_google_mobility(countries=countries, codeToName=codeToName)

# basic impute values for NA in google mobility
# see: https://github.com/ImperialCollegeLondon/covid19model/blob/v6.0/base-usa.r#L87-L88
for(i in 1:ncol(mobility)){
  if (is.numeric(mobility[,i])){
    mobility[is.na(mobility[,i]), i] <- mean(mobility[,i], na.rm = TRUE)
  }
}

#### regress mobility on soc-ec
#### mortality per-capita on soc-ec
