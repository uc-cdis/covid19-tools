from collections import defaultdict
from contextlib import closing
import csv
import enum
import requests


# TODO
# metadata helper
# logger
# format date for submission in submit_metadata()
# comments


def main():
    etl = JonhsHopkinsETL()
    etl.files_to_submissions()
    etl.submit_metadata()


class JonhsHopkinsETL:
    def __init__(self):
        self.location_data = {}
        self.time_series_data = defaultdict(lambda: defaultdict(dict))
        self.expected_headers = [
            "Province/State",
            "Country/Region",
            "Lat",
            "Long",
            "1/22/20",
        ]
        self.metadata_helper = MetadataHelper(base_url="blah")

    def files_to_submissions(self):
        urls = {
            "confirmed": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv",
            "deaths": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv",
            "recovered": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv",
        }
        for data_type, url in urls.items():
            self.parse_file(data_type, url)

    def parse_file(self, data_type, url):
        with closing(requests.get(url, stream=True)) as r:
            f = (line.decode("utf-8") for line in r.iter_lines())
            reader = csv.reader(f, delimiter=",", quotechar='"')

            headers = next(reader)

            assert (
                headers[:5] == self.expected_headers
            ), "CSV headers have changed (expected {}, got {}). We may need to update the ETL code".format(
                headers[:5], self.expected_headers
            )

            for row in reader:
                location, date_to_value = self.parse_row(headers, row)

                location_submitter_id = location["submitter_id"]
                if location_submitter_id not in self.location_data:
                    self.location_data[location_submitter_id] = location

                for date, value in date_to_value.items():
                    self.time_series_data[location_submitter_id][date][
                        data_type
                    ] = value

    def parse_row(self, headers, row):
        province = row[0]
        country = row[1]
        submitter_id = "location_{}".format(country.lower())
        if province:
            submitter_id += "_{}".format(province.lower())

        location = {
            "country": country,
            "lat": row[2],
            "long": row[3],
            "submitter_id": submitter_id,
        }
        if province:
            location["province"] = province

        date_to_value = {}
        for i in range(4, len(headers)):
            date_to_value[headers[i]] = row[i]

        return location, date_to_value

    def submit_metadata(self):
        existing_data = self.metadata_helper.get_existing_data()

        for location in self.location_data.values():
            if location["submitter_id"] in existing_data:
                # do not re-submit location data that already exist
                continue
            record = {"type": "location"}
            record.update(location)
            self.metadata_helper.submit_record(record)

        for location_submitter_id, time_series in self.time_series_data.items():
            for date, data in time_series.items():
                formatted_date = date.replace("/", "-")
                submitter_id = "{}_{}".format(location_submitter_id, formatted_date)
                if submitter_id in existing_data.get(location_submitter_id, []):
                    # do not re-submit time_series data that already exist
                    continue
                record = {
                    "type": "time_series",
                    "submitter_id": submitter_id,
                    "location": {"submitter_id": location_submitter_id},
                    "date": date,
                }
                for data_type, value in data.items():
                    record[data_type] = value
                self.metadata_helper.submit_record(record)


class MetadataHelper:
    def __init__(self, base_url):
        pass

    def get_existing_data(self):
        res = {"location1": ["date1", "date2"]}
        return res

    def submit_record(self, record):
        print(record)
        # exit(1)
        pass


if __name__ == "__main__":
    main()
