import os
import csv
from etl.cityofchicago import CITYOFCHICAGO
from utils.format_helper import (
    derived_submitter_id,
    format_submitter_id,
)

INPUT_DATA_PATH = os.path.join(
    os.path.dirname(__file__), "test_data/test_cityofchicago_input.csv"
)


def get_test_etl():
    def mock_get(*args):
        with open(INPUT_DATA_PATH) as f:
            data = f.read()

        class MockResponse(object):
            def iter_lines(self):
                return (line.encode() for line in data.split("\n"))

            def close(self):
                pass

        return MockResponse()

    class MockMetadataHelper:
        def get_existing_summary_locations(self):
            return []

        def get_latest_submitted_date_idph(self):
            return None

        def get_last_submission(self):
            # The following returns date for the first entry in dataset, which is `2020-03-01`
            return "2020-03-01"

    etl = CITYOFCHICAGO("base_url", "access_token", "s3_bucket")
    etl.get = lambda *args, **kwargs: mock_get(args)
    etl.metadata_helper = MockMetadataHelper()
    etl.summary_location_submitter_id = format_submitter_id(
        "summary_location",
        {"country": "US", "state": "IL", "city": "chicago"},
    )
    return etl


def test_cityofchicago():
    # run the ETL
    etl = get_test_etl()
    etl.files_to_submissions()

    assert etl.summary_locations == {
        "summary_location_us_il_chicago": {
            "submitter_id": "summary_location_us_il_chicago",
            "country_region": "US",
            "county": "Cook",
            "province_state": "IL",
            "projects": [{"code": "cityofchicago"}],
        }
    }

    assert (
        etl.summary_clinicals["summary_clinical_us_il_chicago_2022-04-22"]["count"]
        == 683
    )  # Matching cases_total count for date `2022-04-22`
    assert (
        len(etl.summary_group_demographics) == 180
    )  # 10 rows in original dataset with lab_report_date and each row can make 18 different summary_group_demographics rows
    assert (
        etl.last_submission_identifier == "2022-04-27"
    )  # according to dataset, used for testing, it doesnt have hopitalization data after `2022-04-24`
