# Tools to work with the COVID-19 Data Commons

## ETL tools

### Johns Hopkins

Parses the CSV files located [here](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series) and submits them to https://covid19.datacommons.io via Sheepdog.

### Illinois Department of Public Health

Parses the JSON file located [here](http://www.dph.illinois.gov/sitefiles/COVIDTestResults.json) and submits them to https://covid19.datacommons.io via Sheepdog.

Before April 1, 2020 the URL has daily format like this:

    https://www.dph.illinois.gov/sites/default/files/COVID19/COVID19CountyResults%date%.json

where `%date%` is in format `YYYYMMDD`, e.g. `20200330` for March 30, 2020.
