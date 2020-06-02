from importlib import import_module
import os
from pathlib import Path
from requests.exceptions import RequestException


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


def test_import():
    """
    Makes sure all defined ETL jobs can be imported and started
    """
    etl_dir = os.path.join(CURRENT_DIR, "../etl/")
    all_etls = [
        f.stem
        for f in Path(etl_dir).glob("*.py")
        if "__" not in f.stem and f.stem != "base"
    ]

    for job_name in all_etls:
        job_module = job_name.lower()
        job_class = job_name.upper()

        etl_module = import_module(f"etl.{job_module}")
        etl = getattr(etl_module, job_class)

        try:
            job = etl(
                base_url="fake_url", access_token="fake_token", s3_bucket="fake_bucket"
            )
        except TypeError as e:
            # probably missing an argument
            raise TypeError(f"ETL {job_name}: {e}")
        except RequestException:
            # we could mock `requests` calls instead
            pass

        assert hasattr(
            job, "files_to_submissions"
        ), f"ETL {job_name} is missing files_to_submissions() method"
        assert hasattr(
            job, "submit_metadata"
        ), f"ETL {job_name} is missing submit_metadata() method"
