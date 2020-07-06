# Tools to work with the COVID-19 Data Commons

| Jira | Dataset | Source |
| --- | --- | --- |
| [COV-237][cov-237] | [Chicago Neighborhoods Data](#chicago-neighborhoods-etl) | [here][chi-nbhd] ([JSON][chi-nbhd-json]) |
| [COV-24][cov-24] | John Hopkins Data | [here][jhu] |
| [COV-12][cov-12] | IDPH County-level data | [here][idph-county] |
| [COV-79][cov-79] | IDPH Zipcode data| [here][idph-zipcode] |
| [COV-273][cov-273] | IDPH Facility data | [here][idph-facility-json] ([JSON][idph-facility-json]) |
| [COV-97][cov-97] | DS4C | [here][ds4c] |
| [COV-126][cov-126] | DSCI | [here][dsci] |
| [COV-172][cov-172] | DSFSI | [here][dsfsi] |
| [COV-192][cov-192] | OWID | [here][owid] |
| [COV-170][cov-170] | CCMap | [here][ccmap] |
| [COV-220][cov-220] | COXRAY | [here][coxray] |

## ETL tools

### Chicago Neighborhoods ETL

This ETL will grab the data for Chicago Neighborhoods data from South Side Weekly from .
The data is located in JSON .

### Johns Hopkins

Parses the CSV files located [here](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series) and submits them to https://chicagoland.pandemicresponsecommons.org via Sheepdog.

### Illinois Department of Public Health

#### County level data ([Jira]())

Parses the JSON file located [here](http://www.dph.illinois.gov/sitefiles/COVIDTestResults.json) and submits them to https://chicagoland.pandemicresponsecommons.org via Sheepdog.

Before April 1, 2020 the URL has daily format like this:

    https://www.dph.illinois.gov/sites/default/files/COVID19/COVID19CountyResults%date%.json

where `%date%` is in format `YYYYMMDD`, e.g. `20200330` for March 30, 2020.

#### Zipcode-level data ([Jira]())

Parses the JSON file located [here](http://dph.illinois.gov/sitefiles/COVIDZip.json?nocache=1).

#### Facility data ([Jira]())

Parses the JSON file located [here](https://dph.illinois.gov/sitefiles/COVIDLTC.json).

### Covid Tracking Project

Parses CSV file from Github repository [here](https://raw.githubusercontent.com/COVID19Tracking/covid-tracking-data/master/data/states_daily_4pm_et.csv).

### Kaggle datasets

#### DS4C ([Jira]())

The ETL for Kaggle dataset from [here](https://www.kaggle.com/kimjihoo/coronavirusdataset?select=PatientInfo.csv).

#### DSCI ([Jira]())

The ETL for Kaggle dataset from [here](https://www.kaggle.com/ardisragen/indonesia-coronavirus-cases?select=patient.csv).

### DSFSI ([Jira]())

The ETL for dataset from [here](https://github.com/dsfsi/covid19africa/tree/master/data/line_lists).

### OWID ([Jira]())

The ETL for OWID dataset for number of testing from [here](https://github.com/owid/covid-19-data/tree/master/public/data/testing).

### COXRAY ([Jira]())

The ETL is consist of two parts: `COXRAY_FILE` - for file upload and `COXRAY` for metadata submission.

The data is available [here](https://www.kaggle.com/bachrr/covid-chest-xray) (requires Kaggle account).
The content of archive should go into the folder `./data` (this can be changed via `COXRAY_DATA_PATH` in `coxray.py` and `coxray_file.py`) resulting in the following structure:

```
covid19-tools
...
├── data
│   ├── annotations
│   │   └── ...
│   └── images
│   │   └── ...
│   └── metadata.csv
...
```

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
50  20   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=chi_nbhd bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
 0 */3   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/etl-cronjob.sh ]; then bash $HOME/cloud-automation/files/scripts/etl-cronjob.sh; else echo "no etl-cronjob.sh"; fi) > $HOME/etl-cronjob.log 2>&1
```

*Note*: The time in adminVM is in UTC.

  [chi-nbhd]: https://covid19neighborhoods.southsideweekly.com/
  [chi-nbhd-json]: https://covid19neighborhoods.southsideweekly.com/page-data/index/page-data.json
  [jhu]: https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series
  [idph-county]: http://www.dph.illinois.gov/sitefiles/COVIDTestResults.json?nocache=1
  [idph-zipcode]: http://dph.illinois.gov/sitefiles/COVIDZip.json?nocache=1
  [idph-facility]: https://dph.illinois.gov/covid19/long-term-care-facility-outbreaks-covid-19
  [idph-facility-json]: https://dph.illinois.gov/sitefiles/COVIDLTC.json?nocache=1
  [ds4c]: https://www.kaggle.com/kimjihoo/coronavirusdataset#PatientInfo.csv
  [dsci]: https://www.kaggle.com/ardisragen/indonesia-coronavirus-cases
  [dsfsi]: https://github.com/dsfsi/covid19africa/tree/master/data/line_lists
  [owid]: https://github.com/owid/covid-19-data/blob/master/public/data/testing/covid-testing-latest-data-source-details.csv
  [coxray]: https://www.kaggle.com/bachrr/covid-chest-xray
  [ccmap]: https://github.com/covidcaremap/covid19-healthsystemcapacity/tree/master/data/published
  [cov-12]: https://occ-data.atlassian.net/browse/COV-12
  [cov-24]: https://occ-data.atlassian.net/browse/COV-24
  [cov-79]: https://occ-data.atlassian.net/browse/COV-79
  [cov-97]: https://occ-data.atlassian.net/browse/COV-97
  [cov-126]: https://occ-data.atlassian.net/browse/COV-126
  [cov-170]: https://occ-data.atlassian.net/browse/COV-170
  [cov-172]: https://occ-data.atlassian.net/browse/COV-172
  [cov-192]: https://occ-data.atlassian.net/browse/COV-192
  [cov-220]: https://occ-data.atlassian.net/browse/COV-220
  [cov-237]: https://occ-data.atlassian.net/browse/COV-237
  [cov-273]: https://occ-data.atlassian.net/browse/COV-273
