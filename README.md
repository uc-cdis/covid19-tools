# Tools to work with the COVID-19 Data Commons

| Jira | Dataset | Source | Scheduled / One-time |
| --- | --- | --- | --- |
| [COV-24][cov-24] | John Hopkins Data | [here][jhu] | Scheduled |
| [COV-12][cov-12] | IDPH County-level data | ([JSON][idph-county-json]) | Scheduled |
| [COV-79][cov-79] | IDPH Zipcode data| ([JSON][idph-zipcode-json]) | Scheduled |
| [COV-273][cov-273] | IDPH Facility data | [here][idph-facility] ([JSON][idph-facility-json]) | Scheduled |
| ~~[COV-345][cov-345]~~ | ~~IDPH Hospital data~~ | ~~[here][idph-hospital] ([JSON][idph-hospital-json])~~ | ~~Scheduled~~ |
| [COV-1014](cov-1014) | IDPH Regional ICU Capacity | [here][idph-regional-icu-cap] ([JSON][idph-regional-icu-cap-json]) | Scheduled |
| [COV-720][cov-720] | IDPH Vaccine | [here][covid-19-vaccine-administration-data] | scheduled|
| [COV-925][cov-925] | IDPH Vaccine to S3 | IDPH Vaccine data | scheduled|
| [COV-18][cov-18] | nCOV2019 | [here][ncov2019] | One-time |
| ~~[COV-34][cov-34]~~, ~~[COV-454][cov-454]~~ | ~~CTP~~ | ~~[here][ctp]~~ and ~~[here][race]~~ | ~~Scheduled~~ |
| [COV-97][cov-97] | DS4C | [Kaggle][ds4c] | One-time |
| [COV-126][cov-126] | DSCI | [Kaggle][dsci] | One-time |
| [COV-172][cov-172] | DSFSI | [here][dsfsi] | One-time |
| ~~[COV-170][cov-170]~~ | ~~CCMap~~ | ~~[here][ccmap]~~ | ~~One-time~~|
| [COV-192][cov-192] | OWID2 | [here][owid] | Scheduled |
| [COV-237][cov-237] | Chicago Neighborhoods Data | [here][chi-nbhd] ([JSON][chi-nbhd-json]) | Scheduled |
| ~~[COV-361][cov-361]~~ | ~~NPI-PRO~~ | ~~[here][npi-pro]~~ | ~~One-time~~|
| [COV-220][cov-220] | COXRAY | [Kaggle][coxray] | One-time |
| [COV-422][cov-422] | SSR | Controlled data | One-time (for now) |
| ~~[COV-434][cov-434]~~ | ~~STOPLIGHT~~ | ~~[here][stoplight]~~ | ~~scheduled~~|
| [COV-450][cov-422] | VAC-TRACKER | [here][vac-tracker] | scheduled |
| [COV-453][cov-453] | CHESTX-RAY8 | [here][chestxray8] | One-time |
| [COV-521][cov-521] | ATLAS | [here][atlas] | One-time |
| [COV-465][cov-465] | NCBI-FILE | [bucket][ncbi-bucket] | scheduled|
| [COV-482][cov-482] | NCBI-MANIFEST | [bucket][ncbi-bucket] | scheduled|
| [COV-465][cov-465] | NCBI | [bucket][ncbi-bucket] | scheduled|
| [COV-532][cov-532] | COM-MOBILITY | [here](com-mobility) | scheduled|


## Deployment

To deploy the daily/weekly ETLs, use the following setup in adminVM in `crontab`:
```
crontab -e
```

And add the following:

```
USER=<username with submission access>
S3_BUCKET=<name of bucket to upload data to>

# Sample format for a new job is as follows. Please create a new job for an etl in the same format and make sure it's execution does not overlap with any other jobs ( Just to avoid causing overload )

 0   6   *   *   *    (if [ -f $HOME/cloud-automation/files/scripts/covid19-etl-job.sh ]; then JOB_NAME=jhu bash $HOME/cloud-automation/files/scripts/covid19-etl-job.sh; else echo "no covid19-etl-job.sh"; fi) > $HOME/covid19-etl-$JOB_NAME-cronjob.log 2>&1


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
│   ├── images
│   │   └── ...
│   └── metadata.csv
...
```

The ETL is consist of two parts: `COXRAY_FILE` - for file upload and `COXRAY` for metadata submission.

`COXRAY_FILE` should run first. It will upload the files.
`COXRAY` should run after `COXRAY_FILE` and it will create clinical data and it will link it to files in indexd.

### CHESTX-RAY8

*This is local-only ETL.*
It requires data available locally.
Before running the ETL, the data, which is available [here][chestxray8].
The repository should be cloned into the folder `./data` (this can be changed via `CHESTXRAY8_DATA_PATH` in `chestxray8.py`) resulting in the following structure:

```
covid19-tools
...
├── data
│   ├── COVID-19
│   │   ├── X-Ray Image DataSet
│   │   │   ├── No_findings
│   │   │   ├── Pneumonia
│   │   │   └── Pneumonia
...
```

### NCBI

There are 3 NCBI ETL processes:
- `NCBI_MANIFEST`: Index virus sequence object data in indexd.
- `NCBI_FILE`: Split the clinical metadata into multiple files by accession numbers, and index the files in indexd.
- `NCBI`: Submit NCBI clinical data to the graph by creating metadata records for the files indexed by NCBI_FILE.

While either NCBI_MANIFEST or NCBI_FILE can run first, NCBI needs to run last because it needs the indexd information from the other two. It is common for object data to become available before the associated metadata, so the `NCBI_MANIFEST` job might index files that we don't have metadata for yet, in that case the files are not linked to the graph.

The input data for NCBI_MANIFEST is available in public bucket `sra-pub-sars-cov2`.

The input data for NCBI and NCBI_FILE are available in public bucket `sra-pub-sars-cov2-metadata-us-east-1` with the following structure:

```
covid19-tools
...
├── sra-pub-sars-cov2-metadata-us-east-1"
│   |── contigs
│   │   │   ├── contigs.json
│   │   pipetide
├── ├── ├   │── pipetide.json
│   │  
...
```
*Deployment*: NCBI ETL needs a google cloud setup to access the biqquery public table. For Gen3, the credential needs to put in
`Gen3Secrets/g3auto/covid19-etl/default.json`

*Notes*:
- An accession number is supposed in the format of `[SDE]RR\d+`. SRR for data submitted to NCBI, ERR for EMBL-EBI (European Molecular Biology Laboratory), and DRR for DDBJ (DNA Data Bank of Japan)
- NCBI_MANIFEST ETL uses `last_submission_identifier` field of the project node to keep track the last submission datetime. That prevents the etl from checking and re-indexing the files which were already indexed.
- Virus sequence run taxonomy without a matching submitter id in virus sequence link to CMC only, otherwise link to both CMC and virus sequence


[chi-nbhd]: https://covid19neighborhoods.southsideweekly.com/
[chi-nbhd-json]: https://covid19neighborhoods.southsideweekly.com/page-data/index/page-data.json
[jhu]: https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series
[idph-county-json]: http://www.dph.illinois.gov/sitefiles/COVIDTestResults.json?nocache=1
[idph-zipcode-json]: http://dph.illinois.gov/sitefiles/COVIDZip.json?nocache=1
[idph-facility]: https://dph.illinois.gov/covid19/long-term-care-facility-outbreaks-covid-19
[idph-facility-json]: https://dph.illinois.gov/sitefiles/COVIDLTC.json?nocache=1
[idph-hospital]: http://www.dph.illinois.gov/covid19/hospitalization-utilization
[idph-hospital-json]: https://dph.illinois.gov/sitefiles/COVIDHospitalRegions.json
[idph-regional-icu-cap]: http://www.dph.illinois.gov/covid19/hospitalization-utilization
[idph-regional-icu-cap-json]: https://idph.illinois.gov/DPHPublicInformation/api/COVIDExport/GetHospitalizationResultsRegion
[covid-19-vaccine-administration-data]: http://www.dph.illinois.gov/content/covid-19-vaccine-administration-data
[ds4c]: https://www.kaggle.com/kimjihoo/coronavirusdataset#PatientInfo.csv
[dsci]: https://www.kaggle.com/ardisragen/indonesia-coronavirus-cases
[dsfsi]: https://github.com/dsfsi/covid19africa/tree/master/data/line_lists
[owid]: https://github.com/owid/covid-19-data/blob/master/public/data/testing/covid-testing-latest-data-source-details.csv
[coxray]: https://www.kaggle.com/bachrr/covid-chest-xray
[chestxray8]: https://github.com/muhammedtalo/COVID-19
[ccmap]: https://github.com/covidcaremap/covid19-healthsystemcapacity/tree/master/data/published
[ctp]: https://covidtracking.com/data
[race]: https://covidtracking.com/race
[npi-pro]: https://www.arcgis.com/home/item.html?id=7e80baf1773e4fd9b44fe9fb054677db
[ncov2019]: https://www.kaggle.com/sudalairajkumar/novel-corona-virus-2019-dataset?select=COVID19_line_list_data.csv
[vac-tracker]:https://biorender.com/page-data/covid-vaccine-tracker/page-data.json
[stoplight]: https://covidstoplight.org/api/v0/location/US
[atlas]: https://opportunityinsights.org/data/?geographic_level=0&topic=0&paper_id=1652#resource-listing
[com-mobility]: https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv
[ncbi-bucket]: https://github.com/uc-cdis/covid19-tools#ncbi
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
[cov-422]: https://occ-data.atlassian.net/browse/COV-422
[cov-434]: https://occ-data.atlassian.net/browse/COV-434
[cov-450]: https://occ-data.atlassian.net/browse/COV-450
[cov-453]: https://occ-data.atlassian.net/browse/COV-453
[cov-521]: https://occ-data.atlassian.net/browse/COV-521
[cov-465]: https://occ-data.atlassian.net/browse/COV-465
[cov-482]: https://occ-data.atlassian.net/browse/COV-482
[cov-454]: https://occ-data.atlassian.net/browse/COV-454
[cov-532]: https://occ-data.atlassian.net/browse/COV-532
[cov-720]: https://occ-data.atlassian.net/browse/COV-720
[cov-925]: https://occ-data.atlassian.net/browse/COV-925
[cov-1014]: https://occ-data.atlassian.net/browse/COV-1014
