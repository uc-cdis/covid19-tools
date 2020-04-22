# Tools to work with the COVID-19 Data Commons

## ETL tools

### Johns Hopkins

Parses the CSV files located [here](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series) and submits them to https://chicagoland.pandemicresponsecommons.org via Sheepdog.

### Illinois Department of Public Health

#### County level data

Parses the JSON file located [here](http://www.dph.illinois.gov/sitefiles/COVIDTestResults.json) and submits them to https://chicagoland.pandemicresponsecommons.org via Sheepdog.

Before April 1, 2020 the URL has daily format like this:

    https://www.dph.illinois.gov/sites/default/files/COVID19/COVID19CountyResults%date%.json

where `%date%` is in format `YYYYMMDD`, e.g. `20200330` for March 30, 2020.

### Data Science for Korea dataset

The ETL for Kaggle dataset from [here](https://www.kaggle.com/kimjihoo/coronavirusdataset#PatientInfo.csv).

It requires `ACCESS_TOKEN` to run.

#### Zipcode-level data

Parses the JSON file located [here](http://dph.illinois.gov/sitefiles/COVIDZip.json?nocache=1).

### Covid Tracking Project

Parses CSV file from Github repository [here](https://raw.githubusercontent.com/COVID19Tracking/covid-tracking-data/master/data/states_daily_4pm_et.csv).

## Run ETL jobs

Setup in adminVM in `crontab`:

```
crontab -e
```

```
 0   1   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=jhu bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
 0  20   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=idph bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
30  20   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=ctp bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
 0 */3   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/etl-cronjob.sh ]; then bash $HOME/cloud-automation/files/scripts/etl-cronjob.sh; else echo "no etl-cronjob.sh"; fi) > $HOME/etl-cronjob.log 2>&1
```

*Note*: The time in adminVM is in UTC.
