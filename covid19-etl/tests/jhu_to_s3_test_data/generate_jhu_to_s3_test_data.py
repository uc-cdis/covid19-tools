"""
Script to generate test JHU data (CSV files). We shouldn't need to run it
again. The test data files are extracts of the real data files. This data is
public.
"""


from contextlib import closing
import csv
import os
import requests


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
DATES_STEP = 50


urls = [
    "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv",
    "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv",
    "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv",
    "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv",
    "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv",
]

for url in urls:
    file_name = os.path.basename(url)
    print(file_name)
    with closing(requests.get(url, stream=True)) as r:
        with open(os.path.join(CURRENT_DIR, file_name), "w") as fw:
            writer = csv.writer(fw, delimiter=",")
            f = (line.decode("utf-8") for line in r.iter_lines())
            reader = csv.reader(f, delimiter=",", quotechar='"')
            headers = next(reader)
            for first_date_i, e in enumerate(headers):
                if e.endswith("/20"):
                    break

            writer.writerow(headers[:first_date_i] + headers[first_date_i::DATES_STEP])
            for row in reader:
                writer.writerow(row[:first_date_i] + row[first_date_i::DATES_STEP])
