from aiohttp import ClientSession
import datetime
import json
from math import ceil
from time import sleep

import requests

MAX_RETRIES = 5


class MetadataHelper:
    def __init__(self, base_url, program_name, project_code, access_token):
        # Note: if we end up having too much data, Sheepdog submissions may
        # time out. We'll have to use a smaller batch size and hope that's enough
        self.submit_batch_size = 100

        self.base_url = base_url
        self.program_name = program_name
        self.project_code = project_code

        self.headers = {"Authorization": "bearer " + access_token}
        self.project_id = "{}-{}".format(self.program_name, self.project_code)

        self.records_to_submit = []

    def get_existing_data_jhu(self):
        """
        Queries Guppy for the existing `summary_location` and
        `summary_clinical` data. Returns the list of all the existing
        summary_location submitter_ids, and the latest submitted date
        as a string:
        (
            [
                "summary_location_submitter_id1",
                "summary_location_submitter_id2",
                ...
            ],
            "2020-11-02"
        )
        """
        _filter = {"=": {"project_id": self.project_id}}

        print("Getting existing summary_location submitter_ids from Guppy...")
        res = self.download_from_guppy("location", ["submitter_id"], _filter)
        summary_locations = [r["submitter_id"] for r in res]

        print("Getting the latest summary_clinical date from Guppy...")
        query_string = """query ($filter: JSON) {
            location (
                filter: $filter,
                sort: [{date: "desc"}],
                first: 1,
                accessibility: accessible
            ) {
                date
            }
        }"""
        variables = {"filter": _filter}
        query_res = self.query_guppy(query_string, variables)
        if not query_res["data"]["location"]:
            raise Exception(
                "Did not receive any data from Guppy. Is the token expired?"
            )
        loc = query_res["data"]["location"][0]
        # from format: %Y-%m-%dT00:00:00
        latest_submitted_date = loc["date"].split("T")[0]

        return summary_locations, latest_submitted_date

    def get_latest_submitted_date_idph(self):
        """
        Queries Guppy for the existing `location` data.
        Returns the latest submitted date as Python "datetime.date"
        """
        print("Getting the latest summary_clinical date from Guppy...")
        query_string = """query ($filter: JSON) {
            location (
                filter: $filter,
                sort: [{date: "desc"}],
                first: 1,
                accessibility: accessible
            ) {
                date
            }
        }"""
        variables = {"filter": {"=": {"project_id": self.project_id}}}
        query_res = self.query_guppy(query_string, variables)
        loc = query_res["data"]["location"][0]
        latest_submitted_date = datetime.datetime.strptime(loc["date"], "%Y-%m-%d")
        return latest_submitted_date.date()

    def add_record_to_submit(self, record):
        self.records_to_submit.append(record)

    def add_records_to_submit(self, records):
        self.records_to_submit.extend(records)

    def batch_submit_records(self):
        """
        Submits Sheepdog records in batch
        """
        if not self.records_to_submit:
            print("  Nothing new to submit")
            return
        print(
            "  Submitting {} records in batches of {}".format(
                len(self.records_to_submit), self.submit_batch_size
            )
        )

        n_batches = ceil(len(self.records_to_submit) / self.submit_batch_size)
        for i in range(n_batches):
            records = self.records_to_submit[
                i * self.submit_batch_size : (i + 1) * self.submit_batch_size
            ]

            tries = 0
            while tries < MAX_RETRIES:
                response = requests.put(
                    "{}/api/v0/submission/{}/{}".format(
                        self.base_url, self.program_name, self.project_code
                    ),
                    headers=self.headers,
                    data=json.dumps(records),
                )
                if response.status_code != 200:
                    tries += 1
                    sleep(5)
                else:
                    print("Submission progress: {}/{}".format(i + 1, n_batches))
                    break
            if tries == MAX_RETRIES:
                raise Exception(
                    "Unable to submit to Sheepdog: {}\n{}".format(
                        response.status_code, response.text
                    )
                )

        self.records_to_submit = []

    def query_peregrine(self, query_string):
        url = f"{self.base_url}/api/v0/submission/graphql"
        response = requests.post(
            url,
            json={"query": query_string, "variables": None},
            headers=self.headers,
        )
        try:
            response.raise_for_status()
        except Exception:
            print(
                f"Unable to query Peregrine.\nQuery: {query_string}\nVariables: {variables}"
            )
            raise
        try:
            return response.json()
        except:
            print(f"Peregrine did not return JSON: {response.text}")
            raise

    async def async_query_peregrine(self, query_string):
        async def _post_request(headers, query_string):
            url = f"{self.base_url}/api/v0/submission/graphql"
            async with ClientSession() as session:
                async with session.post(
                    url,
                    json={"query": query_string, "variables": None},
                    headers=headers,
                ) as response:
                    try:
                        response.raise_for_status()
                    except Exception:
                        print(f"Unable to query Peregrine.\nQuery: {query_string}")
                        raise
                    try:
                        response = await response.json()
                    except:
                        print(f"Peregrine did not return JSON: {response.text}")
                        raise
                    return response

        return await _post_request(self.headers, query_string)

    def query_guppy(self, query_string, variables=None):
        url = f"{self.base_url}/guppy/graphql"
        response = requests.post(
            url,
            json={"query": query_string, "variables": variables},
            headers=self.headers,
        )
        try:
            response.raise_for_status()
        except Exception:
            print(
                f"Unable to query Guppy.\nQuery: {query_string}\nVariables: {variables}"
            )
            raise
        try:
            return response.json()
        except:
            print(f"Guppy did not return JSON: {response.text}")
            raise

    def download_from_guppy(self, _type, fields=None, filter=None):
        body = {"type": _type, "accessibility": "accessible"}
        if fields:
            body["fields"] = fields
        if filter:
            body["filter"] = filter

        url = f"{self.base_url}/guppy/download"
        response = requests.post(
            url,
            json=body,
            headers=self.headers,
        )
        try:
            response.raise_for_status()
        except Exception:
            print(f"Unable to download from Guppy.\nBody: {body}")
            raise
        try:
            return response.json()
        except:
            print(f"Guppy did not return JSON: {response.text}")
            raise
