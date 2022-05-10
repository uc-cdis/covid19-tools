import csv
import json
from mock import patch
import pytest
import os

from etl.jhu_to_s3 import JHU_TO_S3
from etl.jhu_to_s3_global import JHU_TO_S3_GLOBAL
from utils.country_codes_utils import get_codes_dictionary, get_codes_for_country_name


INPUT_DATA_DIR = os.path.join(
    os.path.dirname(__file__), "test_data/test_jhu_to_s3_input"
)
OUTPUT_DATA_DIR = os.path.join(
    os.path.dirname(__file__), "test_data/test_jhu_to_s3_output"
)
FILES_TO_DELETE = set()


@pytest.fixture(scope="function")
def clean_up():
    yield
    global FILES_TO_DELETE
    for file in FILES_TO_DELETE:
        os.remove(file)
    FILES_TO_DELETE = set()


def get_test_etl(etl_class):
    def mock_get(*args):
        url = args[0][0]
        with open(os.path.join(INPUT_DATA_DIR, os.path.basename(url))) as f:
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

    etl = etl_class("base_url", "access_token", "s3_bucket")
    etl.get = lambda *args, **kwargs: mock_get(args)
    etl.s3_client = MockS3Client()
    return etl


def test_jhu_to_s3_global(clean_up):
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
        with open(os.path.join(INPUT_DATA_DIR, filename), "r") as f:
            reader = csv.DictReader(f, delimiter=",", quotechar='"')
            for row in reader:
                country = None
                for key in ["Country/Region", "Country_Region"]:
                    if row.get(key):
                        country = row[key]
                        iso3 = get_codes_for_country_name(codes_dict, row[key])["iso3"]
                        all_locations["country"].add(iso3)
                        break
                if country == "US":
                    for key in ["Province/State", "Province_State"]:
                        if row.get(key):
                            all_locations["state"].add(row[key])
                            break
                    if row.get("FIPS"):
                        fips = str(int(float(row["FIPS"])))
                        all_locations["county"].add(fips)

    etl = get_test_etl(JHU_TO_S3_GLOBAL)

    # run the ETL.
    # do not delete files after uploading, so we can check what's in them
    with patch("etl.jhu_to_s3.os.remove") as mock_remove:
        mock_remove.side_effect = lambda f: FILES_TO_DELETE.add(f)
        etl.files_to_submissions()

    # check that the ETL generated and uploded data for all the locations we
    # have data for
    uploaded_locations = {
        "country": set(),
        "state": set(),
        "county": set(),
    }
    uploaded_file = ""
    for uploaded_file in etl.s3_client.upload_file_calls:
        parts = uploaded_file.split("/")
        data_level = parts[-2]
        location_id = os.path.splitext(parts[-1])[0]
        uploaded_locations[data_level].add(location_id)
    uploaded_files_dir = "/".join(uploaded_file.split("/")[:-3])

    assert all_locations["country"] == uploaded_locations["country"]
    assert all_locations["state"] == uploaded_locations["state"]
    assert all_locations["county"] == uploaded_locations["county"]

    # check that the ETL generated the expected time_series data.
    # we only check a few locations: AFG (use case without state/county data),
    # CAN (use case with state data, without county data) and USA (use case
    # with state/county data) countries, and IL state and a few counties
    # (because IL is the only state whose data we currently display on the
    # frontend)
    for data_level in ["country", "state", "county"]:
        dir = os.path.join(OUTPUT_DATA_DIR, "time_series", data_level)
        for file in os.listdir(dir):
            if not file.endswith(".json"):
                continue
            print(f"Checking time_series/{data_level}/{file}")
            output_file = file + "_global" if data_level == "county" else file
            with open(os.path.join(dir, output_file)) as f:
                expected_data = json.loads(f.read())
            with open(
                os.path.join(uploaded_files_dir, "time_series", data_level, file)
            ) as f:
                uploaded_data = json.loads(f.read())
            assert expected_data == uploaded_data

    # submit the rest of the files.
    # do not delete files after uploading, so we can check what's in them
    with patch("etl.jhu_to_s3.os.remove") as mock_remove:
        mock_remove.side_effect = lambda f: FILES_TO_DELETE.add(f)
        etl.submit_metadata()

    # check that the ETL generated and uploaded the expected map_data
    dir = os.path.join(OUTPUT_DATA_DIR, "map_data")
    for file in os.listdir(dir):
        if not file.endswith(".json"):
            continue
        print(f"Checking map_data/{file}")
        with open(os.path.join(dir, file)) as f:
            expected_data = json.loads(f.read())
        with open(os.path.join(uploaded_files_dir, "map_data", file)) as f:
            uploaded_data = json.loads(f.read())
        assert expected_data == uploaded_data


def test_jhu_to_s3_illinois(clean_up):
    # check which locations we have data for
    all_illinois_counties = set()
    for filename in [
        "time_series_covid19_confirmed_global.csv",
        "time_series_covid19_deaths_global.csv",
        "time_series_covid19_recovered_global.csv",
        "time_series_covid19_confirmed_US.csv",
        "time_series_covid19_deaths_US.csv",
    ]:
        with open(os.path.join(INPUT_DATA_DIR, filename), "r") as f:
            reader = csv.DictReader(f, delimiter=",", quotechar='"')
            for row in reader:
                country = row.get("Country/Region", row.get("Country_Region"))
                state = row.get("Province/State", row.get("Province_State"))
                if country == "US" and state == "Illinois":
                    if row.get("FIPS"):
                        fips = str(int(float(row["FIPS"])))
                        all_illinois_counties.add(fips)

    # run the ETL.
    # do not delete files after uploading, so we can check what's in them
    etl = get_test_etl(JHU_TO_S3)
    with patch("etl.jhu_to_s3.os.remove") as mock_remove:
        mock_remove.side_effect = lambda f: FILES_TO_DELETE.add(f)
        etl.files_to_submissions()
        etl.submit_metadata()

    # check that the ETL generated and uploded data for all the locations we
    # have data for
    uploaded_counties = set()
    uploaded_file = ""
    for uploaded_file in etl.s3_client.upload_file_calls:
        parts = uploaded_file.split("/")
        location_id = os.path.splitext(parts[-1])[0]
        if not location_id.endswith("_latest"):
            uploaded_counties.add(location_id)
    uploaded_files_dir = "/".join(uploaded_file.split("/")[:-3])
    assert all_illinois_counties == uploaded_counties

    # check that the ETL generated the expected time_series data
    dir = os.path.join(OUTPUT_DATA_DIR, "time_series/county")
    for file in os.listdir(dir):
        if not file.endswith(".json"):
            continue
        print(f"Checking time_series/county/{file}")
        with open(os.path.join(dir, file)) as f:
            expected_data = json.loads(f.read())
        with open(os.path.join(uploaded_files_dir, "time_series/county", file)) as f:
            uploaded_data = json.loads(f.read())
        assert expected_data == uploaded_data

    # compile the totals from the deprecated `jhu_geojson_latest.json` file to
    # make sure the new version of the ETL comes up with the same numbers
    dir = os.path.join(OUTPUT_DATA_DIR, "map_data")
    file = "jhu_geojson_latest.json"
    expected_totals = {
        "C": 0,
        "D": 0,
    }
    margin = {
        "C": 0,
        "D": 0,
    }
    with open(os.path.join(dir, file)) as f:
        old_etl_data = json.loads(f.read())
        for feature in old_etl_data["features"]:
            props = feature["properties"]
            if (
                props["country_region"] == "US"
                and props["province_state"] == "Illinois"
            ):
                if type(props["confirmed"]) == int:
                    expected_totals["C"] += props["confirmed"]
                else:
                    margin["C"] += 4
                if type(props["deaths"]) == int:
                    expected_totals["D"] += props["deaths"]
                else:
                    margin["D"] += 4

    # check that the ETL generated and uploaded the expected map_data
    file = "jhu_il_json_by_time_latest.json"
    print(f"Checking map_data/{file}")
    with open(os.path.join(dir, file)) as f:
        expected_data = json.loads(f.read())
    with open(os.path.join(uploaded_files_dir, "map_data", file)) as f:
        uploaded_data = json.loads(f.read())
        uploaded_total = uploaded_data.pop("totals")
    assert expected_data == uploaded_data
    # totals must be compared with a margin to allow for "<5" strings that
    # could represent any real values between 0 and 4
    for data_type in expected_totals:
        assert (
            expected_totals[data_type] - margin[data_type]
            <= uploaded_total[data_type]
            <= expected_totals[data_type] + margin[data_type]
        )
