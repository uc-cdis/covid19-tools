### ETL script for generating input tables to model
### main point: ETL JHU covid-19 case and mortality data

# todo: refactor

## HERE! -> not handled here in python -> Serial Interval Table -> would be worthwhile to reproduce the R for that here
## "serial interval table" <--> that discretized gamma distribution 
## so that this script does indeed produce all the required input tables for the model
## alternatively, could just generate that discretized gamma distribution in the R code itself, pre-simulation
## I don't like that idea -> will try to reproduce results in python - but not now -> other more pressing tasks now

import os
import numpy as np
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning) # suppress pandas "future warning"
import pandas as pd

def makeCaseMortalityTable(dirPath):
        
    print("\n~ COVID-19 CASE-MORTALITY TABLE ~")

    # E
    print("--- extracting JHU covid-19 case and mortality data ---")

    # what's the issue here? sometimes stalls for some reason -> fetching data from git

    # fetch the JHU time-series data
    # see: https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/README.md
    jhu = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/"
    cases_csv = "time_series_covid19_confirmed_US.csv"
    deaths_csv = "time_series_covid19_deaths_US.csv"
    casesOrig = pd.read_csv(jhu + cases_csv)
    deathsOrig = pd.read_csv(jhu + deaths_csv)

    # T
    print("--- transforming case and mortality data ---")

    # 1. process cases df to match form of EU table
    cases = casesOrig

    # filter for IL
    cases = cases.loc[cases["Province_State"] == "Illinois"]

    ## get daily counts -> make this a fn

    # take this out, put it back after compute
    dc = cases.copy().iloc[:,11:].astype(np.int64)

    # fix monotone errors
    # rows
    for i in range(len(dc.iloc[:,0])):
        # cols
        for j in range(len(dc.columns)-1):
            # per row:
            # if the next value is bigger than current value -> set next val to current val
            # ensures monotonically increasing sequences
            # allows coherent compute of daily counts (increments -> prevents negative increments)
            if dc.iloc[i,j] > dc.iloc[i,j+1]:
                dc.iloc[i,j+1] = dc.iloc[i,j]

    # look, they're all monotone now!
    # print(dc.apply(lambda x: x.is_monotonic, axis=1).unique())

    dailyCounts = cases.copy()

    # replace cumulative counts with daily counts (i.e., increments)
    dailyCounts.iloc[:,12:] = dc.diff(axis=1, periods=1).iloc[:,1:]

    # treat the daily counts as our working table from here forward
    # note: need to do the same procedure for the death counts
    cases = dailyCounts

    # notice from data repo readme:
    # US counties: UID = 840 (country code3) + XXXXX (5-digit FIPS code)

    # drop redundant columns
    cases = cases.drop(cases.columns[[1,2,3,4,7,10]], axis=1)

    # "melt" df into desired form
    # i.e., rows correspond to dates
    idVars = ["UID", "Admin2", "Province_State", "Lat", "Long_"]
    cases = cases.melt(id_vars=idVars, var_name="Date", value_name="Cases")

    # define mapping from our df to theirs
    # first just map the data
    # then can fix column names and python etl code accordingly

    # now, process deaths data, see if we can take the same path to the same outcome

    # 2. process deaths data
    deaths = deathsOrig

    # filter for Illinois
    deaths = deaths.loc[deaths["Province_State"] == "Illinois"]

    # compute increments from cumulative counts - same steps as case table

    # take this out, put it back after compute
    # note: this table has "population" column - 1 additional column, so dates start at 12, not 11
    dc = deaths.copy().iloc[:,12:].astype(np.int64)

    ## fix monotone errors -> make this a fn
    # rows
    for i in range(len(dc.iloc[:,0])):
        # cols
        for j in range(len(dc.columns)-1):
            if dc.iloc[i,j] > dc.iloc[i,j+1]:
                dc.iloc[i,j+1] = dc.iloc[i,j]

    # look, they're all monotone now!
    # print(dc.apply(lambda x: x.is_monotonic, axis=1).unique())

    dailyCounts = deaths.copy()
    # replace cumulative counts with daily counts (i.e., increments)
    dailyCounts.iloc[:,13:] = dc.diff(axis=1, periods=1).iloc[:,1:]
    # take a look at cook (county)
    dailyCounts[dailyCounts["Admin2"] == "Cook"]

    # now working with daily deaths, not cumulative deaths
    deaths = dailyCounts

    # drop redundant columns -> keep population column
    deaths = deaths.drop(deaths.columns[[1,2,3,4,7,10]], axis=1)

    # "melt" df into desired form
    # i.e., rows correspond to dates
    idVars = ["UID", "Admin2", "Province_State", "Lat", "Long_", "Population"]
    deaths = deaths.melt(id_vars=idVars, var_name="Date", value_name="Deaths")

    # note: processing of the two tables is exactly the same except
    # for the deaths table having the extra ID column "Population"
    # so can refactor/streamline code to make it concise and much prettier

    # next task: "merge" the deaths and cases tables

    # danke: https://stackoverflow.com/questions/19125091/pandas-merge-how-to-avoid-duplicating-columns
    # merge df's
    # i.e., inject population and deaths data from deaths df into cases df
    cols_to_use = deaths.columns.difference(cases.columns)
    ILCaseAndMortality = pd.merge(cases, deaths[cols_to_use], left_index=True, right_index=True, how="outer")

    # cut out rows where Admin2 is "Out of IL" or "Unassigned" (both have population 0)
    ILCaseAndMortality = ILCaseAndMortality.loc[ILCaseAndMortality["Population"] > 0]

    # rename some columns; improve readability
    renameColsMap = {
        "UID": "CountyID", # fairly certain this is appropriate, though will double check
        "Admin2": "Town", # ? -> probably a better name for this
        "Province_State": "State",
        "Lat": "Latitude",
        "Long_": "Longitude"
    }
    ILCaseAndMortality = ILCaseAndMortality.rename(renameColsMap, axis=1)

    # now order the columns nicely
    columnOrder = [
        "Date",
        "Cases",
        "Deaths",
        "CountyID",
        "Town",
        "State",
        "Population",
        "Latitude",
        "Longitude"
    ]

    ILCaseAndMortality = ILCaseAndMortality[columnOrder]

    # looks good -> save it
    # suppressing this for now, so as not to create "unused" tables -> simplify output of this script
    # ILCaseAndMortality.to_csv(dirPath + "/ILCaseAndMortality.csv")

    # next: 
    # 1. preserving this table; modify this table to exactly match the scheme of the Euro table
    # 2. save that as a separate file
    # 3. run the model with that table as input
    # 6. refactor all this; sketch plan for actual script(s) (!)

    df = ILCaseAndMortality.copy()

    df["month"], df["day"], df["year"] = df["Date"].str.split("/").str

    # drop extra columns
    df = df.drop(["State", "Latitude", "Longitude"], axis=1)

    # rename remaining columns to match Euro table
    ToEuroColumnsMap = {
        "Date": "dateRep",
        "Cases": "cases",
        "Deaths": "deaths",
        "CountyID": "countryterritoryCode", # ?
        "Town": "countriesAndTerritories", # ?
        "Population": "popData2018",
    }

    df = df.rename(ToEuroColumnsMap, axis=1)

    # populate geoID with Town also, just to populate it
    df["geoId"] = df["countriesAndTerritories"]

    # reorder the columns to match Euro table # probably don't hardcode this -> make proper config file (?)
    CaseMortalityColumnOrder = [
        'dateRep', 
        'day', 
        'month', 
        'year', 
        'cases', 
        'deaths', 
        'countriesAndTerritories', 
        'geoId', 
        'countryterritoryCode', 
        'popData2018'
    ]
    df = df[CaseMortalityColumnOrder]

    print("--- saving transformed case and mortality data  ---")

    # okay, done, now save it
    p = dirPath + "/ILCaseAndMortalityV1.csv"
    df.to_csv(p)

    countyIDList = ILCaseAndMortality["CountyID"].unique()

    population_df = ILCaseAndMortality[["CountyID", "Population"]].copy().drop_duplicates()

    return(p, countyIDList, population_df)

def makeInterventionsTable(dirPath, countyIDList): 
    print("\n~ INTERVENTIONS TABLE ~")

    # -> should remove all their tables, comparisons to their tables etc.
    # self-contained ETL -> we can have our own config -> not the old EU tables

    # task: make a table for IL by county that looks like their covariates table
    # only column is lockdown
    # dates for all counties the same
    # admittedly a dumb table for now, but will get extended later

    print("--- constructing interventions table ---")

    # counties correspond to countries
    ourCovariates = pd.DataFrame(countyIDList, columns=["Country"])

    print("--- loading intervention: lockdown ---")

    # date of IL lockdown: Saturday, March 21st, 2020
    # source: https://www.chicagotribune.com/coronavirus/ct-coronavirus-illinois-shelter-in-place-lockdown-order-20200320-teedakbfw5gvdgmnaxlel54hau-story.html
    ourCovariates["lockdown"] = "2020-03-21"

    print("--- saving covariates table ---")

    # save this new table
    p = dirPath + "/ILInterventionsV1.csv"
    ourCovariates.to_csv(p)

    return(p)

def makeIFRTable(dirPath, population_df):

    print("\n~ IFR TABLE ~")
    print("--- constructing IFR table ---")
    
    ourIFR = population_df

    # now need:
    # 1. fatality ratio per stratum (?)
    # 2. relative frequency pre stratum
    # for now, all counties will be treated the same - same age strata rel freq, same fatality ratio

    # IL age distribution
    # actually here's IL age distribution by county according to US census:
    # https://censusreporter.org/data/table/?table=B01001&geo_ids=04000US17,01000US,050|04000US17&primary_geo_id=04000US17

    # for starters applying same distribution to each county, for sake of just running the model as soon as possible
    # later (tomorrow) can get the by-county resolution for age distribution -> HERE! fixme.
    # ILAgeDistr = pd.read_csv("./notebooks/IL-Age-Distr/ageDistr.csv")

    # for now, manually entering this
    # source:
    # https://censusreporter.org/profiles/04000US17-illinois/
    ILAgeDistr = {
        "0-9": [.119],
        "10-19": [.131],
        "20-29": [.136],
        "30-39": [.135],
        "40-49": [.127],
        "50-59": [.132],
        "60-69": [.114],
        "70-79": [.067],
        "80+"  : [.039]
    }

    ageDist = pd.DataFrame(ILAgeDistr)

    ourIFR[list(ageDist)] = pd.DataFrame(np.repeat(ageDist.values, len(ourIFR.index), axis=0))
    strata = list(ILAgeDistr.keys())
    ourIFR[strata] = ourIFR[strata].mul(ourIFR["Population"], axis=0)

    # okay great, so now we have IL population by-county by-age-bracket
    # note: again recall that this is just the IL state-wide distribution applied to each county's population -> HERE! fixme.
    # the by-county age distributions are available and can be easily worked in
    # will just take a bit of data transforming/mapping etc.
    # it's on the todo list

    # next: add the "weighted_fatality" column
    # right now the value for this column doesn't really matter
    # so will do the simplest thing for now and extend later

    # this is the paper ICL consulted for picking their ifr numbers: 
    # https://www.thelancet.com/journals/laninf/article/PIIS1473-3099(20)30243-7/fulltext
    ourIFR["weighted_fatality"] = .00657

    # reorder columns
    ourIFRColumnOrder = ["CountyID", "weighted_fatality", "Population"] + strata
    ourIFR = ourIFR[ourIFRColumnOrder]

    # save this
    # suppressing this for now -> simplify/minimize number of files generated by this script
    # ourIFR.to_csv(dirPath + "/ILWeightedFatality.csv")

    # now map to euro table for input to model
    mapILToEuroIFR = {
        "CountyID": "country",
        "10-19": "Oct-19", # extraordinarily painful to look at, but will be fixed soon enough
        "Population": "population"
    }

    ILInputIFR = ourIFR.copy()
    ILInputIFR = ILInputIFR.rename(mapILToEuroIFR, axis=1)
    # fill placeholder values for redundant columns, to match their df exactly ..
    ILInputIFR["Region, subregion, country or area *"] = ILInputIFR["country"]
    ILInputIFR["Unnamed: 0"] = ILInputIFR.index

    # reorder to match their order
    EUColumnOrder = [
        'Unnamed: 0', 
        'Region, subregion, country or area *', 
        '0-9', 
        'Oct-19', 
        '20-29', 
        '30-39', 
        '40-49', 
        '50-59', 
        '60-69', 
        '70-79', 
        '80+', 
        'weighted_fatality', 
        'population', 
        'country'
    ]
    ILInputIFR = ILInputIFR[EUColumnOrder]

    print("--- saving IFR table ---")

    # save this
    p = dirPath + "/ILWeightedFatalityV1.csv"
    ILInputIFR.to_csv(p)

    return(p)

# paper: https://arxiv.org/abs/2004.00756
# data: https://github.com/JieYingWu/COVID-19_US_County-level_Summaries
def fetchSocEc(dirPath):
    print("\n~ SOC-EC TABLE ~")

    print("--- fetching soc-ec table ---")
    path = 'https://raw.githubusercontent.com/JieYingWu/COVID-19_US_County-level_Summaries/master/data/counties.csv'
    df = pd.read_csv(path)

    print("--- saving soc-ec table ---")
    p = dirPath + "/SocEc.csv"
    df.to_csv(p)

    return(p)

# wow I want to really, thoroughly refactor all this so bad
# make a class - the whole thing -> not the most time pressing task though

if __name__ == "__main__":

    # put tables here
    dirPath = "../modelInput"
    os.makedirs(dirPath, exist_ok=True)

    p1, countyIDList, population_df = makeCaseMortalityTable(dirPath)
    p2 = makeInterventionsTable(dirPath, countyIDList)
    p3 = makeIFRTable(dirPath, population_df)
    p4 = fetchSocEc(dirPath)

    print("\n")
    print("tables successfully written to these paths:")
    print("\t", p1)
    print("\t", p2)
    print("\t", p3)
    print("\t", p4)    
    print("\n")
