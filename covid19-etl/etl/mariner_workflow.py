from datetime import datetime
import requests
import time

from etl import base
from utils.fence_helper import get_api_key, get_access_token


class MARINER_WORKFLOW(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.headers = {"Authorization": f"Bearer {access_token}"}
        self.api_key = get_api_key(base_url, headers=self.headers)

        self.model_version = "v3.2"
        self.status_ping_minutes = 10

    def get_status(self, run_id):
        url = f"{self.base_url}/ga4gh/wes/v1/runs/{run_id}/status"
        r = requests.get(url, headers=self.headers)

        if r.status_code == 403:
            print(
                "Got a 403, token might have expired. Getting a new token and retrying"
            )
            new_access_token = get_access_token(self.base_url, self.api_key)
            self.headers = {"Authorization": f"Bearer {new_access_token}"}
            r = requests.get(url, headers=self.headers)

        assert (
            r.status_code == 200
        ), f"Could not get run status from Mariner ({r.status_code}):\n{r.text}"
        resp_data = r.json()
        if not resp_data or "status" not in resp_data:
            # Mariner did not return a status - that happens right after the
            # job is created. It might take a few seconds to start the run.
            # For now, assume the status is "not-started"
            return "not-started"
        return resp_data["status"]

    def files_to_submissions(self):
        print("Preparing request body")
        url = f"https://raw.githubusercontent.com/uc-cdis/covid19model/{self.model_version}/cwl/request_body.json"
        r = requests.get(url)
        assert r.status_code == 200, f"Could not get request body from {url}"
        request_body = r.json()
        request_body["input"]["s3_bucket"] = f"s3://{self.s3_bucket}"

        print("Starting workflow run")
        url = f"{self.base_url}/ga4gh/wes/v1/runs"
        r = requests.post(url, json=request_body, headers=self.headers)
        assert (
            r.status_code == 200
        ), f"Could not start Mariner workflow ({r.status_code}):\n{r.text}"
        resp_data = r.json()
        assert (
            resp_data and "runID" in resp_data
        ), f"Mariner did not return a runID:\n{resp_data}"
        run_id = resp_data["runID"]

        print(f"Monitoring workflow run (run ID: {run_id})")
        status = "running"
        while status in ["not-started", "running", "unknown"]:
            status = self.get_status(run_id)
            print(f"  [{datetime.now()}] status: {status}")
            time.sleep(60 * self.status_ping_minutes)
