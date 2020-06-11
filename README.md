# Tools to work with the COVID-19 Data Commons

## ETL tools

### Johns Hopkins ([Jira](https://occ-data.atlassian.net/browse/COV-24))

Parses the CSV files located [here](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series) and submits them to https://chicagoland.pandemicresponsecommons.org via Sheepdog.

### Illinois Department of Public Health

#### County level data ([Jira](https://occ-data.atlassian.net/browse/COV-12))

Parses the JSON file located [here](http://www.dph.illinois.gov/sitefiles/COVIDTestResults.json) and submits them to https://chicagoland.pandemicresponsecommons.org via Sheepdog.

Before April 1, 2020 the URL has daily format like this:

    https://www.dph.illinois.gov/sites/default/files/COVID19/COVID19CountyResults%date%.json

where `%date%` is in format `YYYYMMDD`, e.g. `20200330` for March 30, 2020.

#### Zipcode-level data ([Jira](https://occ-data.atlassian.net/browse/COV-79))

Parses the JSON file located [here](http://dph.illinois.gov/sitefiles/COVIDZip.json?nocache=1).

#### Facility data ([Jira](https://occ-data.atlassian.net/browse/COV-273))

Parses the JSON file located [here](https://dph.illinois.gov/sitefiles/COVIDLTC.json).

### Covid Tracking Project

Parses CSV file from Github repository [here](https://raw.githubusercontent.com/COVID19Tracking/covid-tracking-data/master/data/states_daily_4pm_et.csv).

### Kaggle datasets

#### DS4C ([Jira](https://occ-data.atlassian.net/browse/COV-97))

The ETL for Kaggle dataset from [here](https://www.kaggle.com/kimjihoo/coronavirusdataset?select=PatientInfo.csv).

#### DSCI ([Jira](https://occ-data.atlassian.net/browse/COV-126))

The ETL for Kaggle dataset from [here](https://www.kaggle.com/ardisragen/indonesia-coronavirus-cases?select=patient.csv).

### DSFSI ([Jira](https://occ-data.atlassian.net/browse/COV-172))

The ETL for dataset from [here](https://github.com/dsfsi/covid19africa/tree/master/data/line_lists).

### OWID ([Jira](https://occ-data.atlassian.net/browse/COV-192))

The ETL for OWID dataset for number of testing from [here](https://github.com/owid/covid-19-data/tree/master/public/data/testing).

## Run ETL jobs

Setup in adminVM in `crontab`:

```
crontab -e
```

```
 0   1   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=jhu bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
 0  20   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=idph bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
 0  40   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=idph_zipcode bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
 0  50   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=idph_facility bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
30  20   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=ctp bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
45  20   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=owid bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
 0 */3   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/etl-cronjob.sh ]; then bash $HOME/cloud-automation/files/scripts/etl-cronjob.sh; else echo "no etl-cronjob.sh"; fi) > $HOME/etl-cronjob.log 2>&1
```

*Note*: The time in adminVM is in UTC.
