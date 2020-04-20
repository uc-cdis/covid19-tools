import os

from importlib import import_module

if __name__ == "__main__":
    base_url = "http://revproxy-service"

    job_name = os.environ.get("JOB_NAME")
    if not job_name:
        raise Exception(
            "Need JOB_NAME environment variable (specification on which ETL job to run)"
        )

    job_module = job_name.lower()
    job_class = job_name.upper()

    s3_etl_module = import_module(f"s3_etl.{job_module}")
    s3_etl = getattr(s3_etl_module, job_class)

    job = s3_etl(base_url)
    job.files_to_submissions()
