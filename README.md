# Tools to work with the COVID-19 Data Commons

| Jira | Dataset | Source | Scheduled / One-time |
| --- | --- | --- | --- |
| [COV-24][cov-24] | John Hopkins Data | [here][jhu] | Scheduled |
| [COV-12][cov-12] | IDPH County-level data | ([JSON][idph-county-json]) | Scheduled |
| [COV-79][cov-79] | IDPH Zipcode data| ([JSON][idph-zipcode-json]) | Scheduled |
| [COV-273][cov-273] | IDPH Facility data | [here][idph-facility] ([JSON][idph-facility-json]) | Scheduled |
| [COV-345][cov-345] | IDPH Hospital data | [here][idph-hospital] ([JSON][idph-hospital-json]) | Scheduled |
| [COV-18][cov-18] | nCOV2019 | [here][ncov2019] | One-time |
| [COV-34][cov-34] | CTP | [here][ctp] | Scheduled |
| [COV-97][cov-97] | DS4C | [Kaggle][ds4c] | One-time |
| [COV-126][cov-126] | DSCI | [Kaggle][dsci] | One-time |
| [COV-172][cov-172] | DSFSI | [here][dsfsi] | One-time |
| [COV-170][cov-170] | CCMap | [here][ccmap] | One-time |
| [COV-192][cov-192] | OWID | [here][owid] | Scheduled |
| [COV-237][cov-237] | Chicago Neighborhoods Data | [here][chi-nbhd] ([JSON][chi-nbhd-json]) | Scheduled |
| [COV-361][cov-361] | NPI-PRO | [here][npi-pro] | One-time |
| [COV-220][cov-220] | COXRAY | [Kaggle][coxray] | One-time |

## Deployment

To deploy the daily/weekly ETLs, use the following setup in adminVM in `crontab`:
```
crontab -e
```

And add the following:

```
USER=<username with submission access>
S3_BUCKET=<name of bucket to upload data to>

 0   6   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=jhu bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
 0   6   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=jhu_to_s3 bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/logs/covid19-etl-jhu_to_s3-cronjob.log 2>&1
30   6   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=jhu_country_codes bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/logs/covid19-etl-jhu_country_codes-cronjob.log 2>&1
 0  20   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=idph bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
10  20   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=idph_zipcode bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
20  20   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=idph_facility bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
30  20   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=idph_hospital bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/logs/covid19-etl-$JOB_NAME-cronjob.log 2>&1
40  20   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=ctp bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
50  20   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=owid bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
 0  21   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=chi_nbhd bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no codiv19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1
 0 */3   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/etl-cronjob.sh ]; then bash $HOME/cloud-automation/files/scripts/etl-cronjob.sh; else echo "no etl-cronjob.sh"; fi) > $HOME/etl-cronjob.log 2>&1
```

*Note*: The time in adminVM is in UTC.

## Special instructions

### COXRAY

*This is local-only ETL.*
It requires data available locally.
Before running the ETL, the data, which is available [here](https://www.kaggle.com/bachrr/covid-chest-xray) and requires Kaggle account.
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

The ETL is consist of two parts: `COXRAY_FILE` - for file upload and `COXRAY` for metadata submission.

`COXRAY_FILE` should run first. It will upload the files.
`COXRAY` should run after `COXRAY_FILE` and it will create clinical data and it will link it to files in indexd.


  [chi-nbhd]: https://covid19neighborhoods.southsideweekly.com/
  [chi-nbhd-json]: https://covid19neighborhoods.southsideweekly.com/page-data/index/page-data.json
  [jhu]: https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series
  [idph-county-json]: http://www.dph.illinois.gov/sitefiles/COVIDTestResults.json?nocache=1
  [idph-zipcode-json]: http://dph.illinois.gov/sitefiles/COVIDZip.json?nocache=1
  [idph-facility]: https://dph.illinois.gov/covid19/long-term-care-facility-outbreaks-covid-19
  [idph-facility-json]: https://dph.illinois.gov/sitefiles/COVIDLTC.json?nocache=1
  [idph-hospital]: http://www.dph.illinois.gov/covid19/hospitalization-utilization
  [idph-hospital-json]: https://dph.illinois.gov/sitefiles/COVIDHospitalRegions.json
  [ds4c]: https://www.kaggle.com/kimjihoo/coronavirusdataset#PatientInfo.csv
  [dsci]: https://www.kaggle.com/ardisragen/indonesia-coronavirus-cases
  [dsfsi]: https://github.com/dsfsi/covid19africa/tree/master/data/line_lists
  [owid]: https://github.com/owid/covid-19-data/blob/master/public/data/testing/covid-testing-latest-data-source-details.csv
  [coxray]: https://www.kaggle.com/bachrr/covid-chest-xray
  [ccmap]: https://github.com/covidcaremap/covid19-healthsystemcapacity/tree/master/data/published
  [ctp]: https://covidtracking.com/data
  [npi-pro]: https://www.arcgis.com/home/item.html?id=7e80baf1773e4fd9b44fe9fb054677db
  [ncov2019]: https://www.kaggle.com/sudalairajkumar/novel-corona-virus-2019-dataset?select=COVID19_line_list_data.csv
  [cov-12]: https://occ-data.atlassian.net/browse/COV-12
  [cov-18]: https://occ-data.atlassian.net/browse/COV-18
  [cov-24]: https://occ-data.atlassian.net/browse/COV-24
  [cov-34]: https://occ-data.atlassian.net/browse/COV-34
  [cov-79]: https://occ-data.atlassian.net/browse/COV-79
  [cov-97]: https://occ-data.atlassian.net/browse/COV-97
  [cov-126]: https://occ-data.atlassian.net/browse/COV-126
  [cov-170]: https://occ-data.atlassian.net/browse/COV-170
  [cov-172]: https://occ-data.atlassian.net/browse/COV-172
  [cov-192]: https://occ-data.atlassian.net/browse/COV-192
  [cov-220]: https://occ-data.atlassian.net/browse/COV-220
  [cov-237]: https://occ-data.atlassian.net/browse/COV-237
  [cov-273]: https://occ-data.atlassian.net/browse/COV-273
  [cov-345]: https://occ-data.atlassian.net/browse/COV-345
  [cov-361]: https://occ-data.atlassian.net/browse/COV-361
