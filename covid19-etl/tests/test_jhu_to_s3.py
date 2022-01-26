import csv
import json
from mock import patch
import os

from etl.jhu_to_s3 import JHU_TO_S3
from utils.country_codes_utils import get_codes_dictionary, get_codes_for_country_name


DATA_DIR = os.path.join(os.path.dirname(__file__), "jhu_to_s3_test_data")


def get_test_etl():
    def mock_get(*args):
        url = args[0][0]
        with open(os.path.join(DATA_DIR, os.path.basename(url))) as f:
            data = f.read()

        class MockResponse(object):
            def iter_lines(self):
                return (line.encode() for line in data.split("\n"))

            def close(self):
                pass

        return MockResponse()

    class MockS3Client:
        def __init__(self):
            self.upload_file_calls = []

        def upload_file(self, abs_path, s3_bucket, s3_path):
            self.upload_file_calls.append(abs_path)

    etl = JHU_TO_S3("base_url", "access_token", "s3_bucket")
    etl.get = lambda *args, **kwargs: mock_get(args)
    etl.s3_client = MockS3Client()
    return etl


def test_jhu_to_s3():
    # check which locations we have data for
    codes_dict = get_codes_dictionary()
    all_locations = {
        "country": set(),
        "state": set(),
        "county": set(),
    }
    for filename in [
        "time_series_covid19_confirmed_global.csv",
        "time_series_covid19_deaths_global.csv",
        "time_series_covid19_recovered_global.csv",
        "time_series_covid19_confirmed_US.csv",
        "time_series_covid19_deaths_US.csv",
    ]:
        with open(os.path.join(DATA_DIR, filename), "r") as f:
            reader = csv.DictReader(f, delimiter=",", quotechar='"')
            for row in reader:
                country = None
                for key in ["Country/Region", "Country_Region"]:
                    if row.get(key):
                        country = row[key]
                        iso3 = get_codes_for_country_name(codes_dict, row[key])["iso3"]
                        all_locations["country"].add(iso3)
                if country == "US":
                    for key in ["Province/State", "Province_State"]:
                        if row.get(key):
                            all_locations["state"].add(row[key])
                    for key in ["FIPS"]:
                        if row.get(key):
                            all_locations["county"].add(row[key])

    etl = get_test_etl()

    # run the ETL.
    # do not delete files after uploading, so we can check what's in them
    with patch("etl.jhu_to_s3.os.remove"):
        etl.files_to_submissions()

    # check that the ETL generated and uploded data for all the locations we
    # have data for
    uploaded_locations = {
        "country": set(),
        "state": set(),
        "county": set(),
    }
    for uploaded_file in etl.s3_client.upload_file_calls:
        parts = uploaded_file.split("/")
        data_level = parts[-2]
        location_id = os.path.splitext(parts[-1])[0]
        uploaded_locations[data_level].add(location_id)
    uploaded_files_dir = "/".join(uploaded_file.split("/")[:-3])

    assert all_locations["country"] == uploaded_locations["country"]
    assert all_locations["state"] == uploaded_locations["state"]
    # compare the number of counties instead of the counties themselves
    # because it would take very long
    assert len(all_locations["county"]) == len(uploaded_locations["county"])

    # check that the ETL generated the expected data.
    # we only check a few locations: AFG (use case without state/county data),
    # CAN (use case with state data, without county data) and USA (use case
    # with state/county data) countries, and IL state and a few counties
    # (because IL is the only state whose data we currently display on the
    # frontend)
    for data_level in ["country", "state", "county"]:
        dir = os.path.join(DATA_DIR, "time_series", data_level)
        for file in os.listdir(dir):
            if not file.endswith(".json"):
                continue
            print(f"Checking time_series/{data_level}/{file}")
            with open(os.path.join(dir, file)) as f:
                expected_data = json.loads(f.read())
            with open(
                os.path.join(uploaded_files_dir, "time_series", data_level, file)
            ) as f:
                uploaded_data = json.loads(f.read())
            assert expected_data == uploaded_data

    # submt the rest of the files.
    # do not delete files after uploading, so we can check what's in them
    with patch("etl.jhu_to_s3.os.remove"):
        etl.submit_metadata()

    # check that the ETL generated and uploaded the expected data
    dir = os.path.join(DATA_DIR, "map_data")
    for file in os.listdir(dir):
        if not file.endswith(".json"):
            continue
        print(f"Checking map_data/{file}")
        with open(os.path.join(dir, file)) as f:
            expected_data = json.loads(f.read())
        with open(os.path.join(uploaded_files_dir, "map_data", file)) as f:
            uploaded_data = json.loads(f.read())
        assert expected_data == uploaded_data
