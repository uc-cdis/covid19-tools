from etl.idph_vaccine_to_s3 import IDPH_VACCINE_TO_S3
from utils.country_codes_utils import get_county_to_fips_dictionary


def test_no_existing_data():
    etl = IDPH_VACCINE_TO_S3("base_url", "access_token", "s3_bucket")
    county_to_fips_dict = get_county_to_fips_dictionary()
    county1, county2 = list(county_to_fips_dict)[:2]

    existing_data = {"il_county_list": {}, "last_updated": None}
    new_data = {
        "summary_location": [
            {
                "county": county1,
                "summary_clinicals": [
                    {"date": "2021-01-01", "vaccine_persons_fully_vaccinated": 1}
                ],
            },
            {
                "county": county2,
                "summary_clinicals": [
                    {"date": "2021-01-02", "vaccine_persons_fully_vaccinated": 2}
                ],
            },
        ]
    }
    etl.format_result(county_to_fips_dict, existing_data, new_data)

    assert existing_data["il_county_list"] == {
        county_to_fips_dict[county1]: {"by_date": {"2021-01-01": 1}, "county": county1},
        county_to_fips_dict[county2]: {"by_date": {"2021-01-02": 2}, "county": county2},
    }
    assert existing_data["last_updated"] == "2021-01-01"


def test_with_existing_data():
    etl = IDPH_VACCINE_TO_S3("base_url", "access_token", "s3_bucket")
    county_to_fips_dict = get_county_to_fips_dictionary()
    county = list(county_to_fips_dict)[0]

    existing_data = {
        "il_county_list": {
            county_to_fips_dict[county]: {
                "by_date": {"2021-01-01": 1},
                "county": county,
            }
        },
        "last_updated": "2021-01-01",
    }
    new_data = {
        "summary_location": [
            {
                "county": county,
                "summary_clinicals": [
                    {"date": "2021-01-02", "vaccine_persons_fully_vaccinated": 2}
                ],
            },
        ]
    }
    etl.format_result(county_to_fips_dict, existing_data, new_data)

    # both old data and new data are present
    assert existing_data["il_county_list"] == {
        county_to_fips_dict[county]: {
            "by_date": {"2021-01-01": 1, "2021-01-02": 2},
            "county": county,
        },
    }
    assert existing_data["last_updated"] == "2021-01-02"


def test_add_chicago_to_cook():
    etl = IDPH_VACCINE_TO_S3("base_url", "access_token", "s3_bucket")
    county_to_fips_dict = get_county_to_fips_dictionary()
    county = "Cook"

    existing_data = {"il_county_list": {}, "last_updated": None}
    new_data = {
        "summary_location": [
            {
                "county": county,
                "summary_clinicals": [
                    {"date": "2021-01-01", "vaccine_persons_fully_vaccinated": 1},
                    {"date": "2021-01-02", "vaccine_persons_fully_vaccinated": 2},
                ],
            },
            {
                "county": "Chicago",
                "summary_clinicals": [
                    {"date": "2021-01-01", "vaccine_persons_fully_vaccinated": 1},
                    {"date": "2021-01-03", "vaccine_persons_fully_vaccinated": 3},
                ],
            },
        ]
    }
    etl.format_result(county_to_fips_dict, existing_data, new_data)

    # when both Cook and Chicago have a value for a date, add them as "Cook".
    # only Cook: keep it.
    # only Chicago: ignore it.
    assert existing_data["il_county_list"] == {
        county_to_fips_dict[county]: {
            "by_date": {"2021-01-01": 2, "2021-01-02": 2},
            "county": county,
        },
    }
    assert existing_data["last_updated"] == "2021-01-02"
