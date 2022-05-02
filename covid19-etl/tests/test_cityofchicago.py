import os

from etl.cityofchicago import CITYOFCHICAGO


INPUT_DATA_PATH = os.path.join(os.path.dirname(__file__), "test_cityofchicago_input.csv")  # TODO use test data that makes sense, for example with edge cases


def get_test_etl():
    def mock_get(*args):
        url = args[0][0]
        with open(os.path.join(INPUT_DATA_PATH, os.path.basename(url))) as f:
            data = f.read()

        class MockResponse(object):
            def iter_lines(self):
                return (line.encode() for line in data.split("\n"))

            def close(self):
                pass

        return MockResponse()

    class MockMetadataHelper:
        # TODO the functions can return whatever makes sense for your tests,
        # you can pass values to `get_test_etl` depending on your test case
        def get_existing_summary_locations(self):
            return []
    
        def get_latest_submitted_date_idph(self):
            return None

    etl = CITYOFCHICAGO("base_url", "access_token", "s3_bucket")
    etl.get = lambda *args, **kwargs: mock_get(args)
    etl.metadata_helper = MockMetadataHelper()
    return etl


def test_no_existing_data():
    # run the ETL
    etl = get_test_etl()
    etl.files_to_submissions()

    assert etl.summary_locations == "TODO"
    assert etl.summary_clinicals == "TODO"
    assert etl.summary_group_demographics == "TODO"
