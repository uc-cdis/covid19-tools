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
        Queries Peregrine for the existing `location` and `time_series` data. Returns a dict in format { "location1": [ "date1", "date2" ] }
        """
        print("Getting existing data from Peregrine...")
        print("  summary_location data...")
        query_string = (
            '{ summary_location (first: 0, project_id: "'
            + self.project_id
            + '") { submitter_id } }'
        )
        query_res = self.query_peregrine(query_string)
        json_res = {
            location["submitter_id"]: []
            for location in query_res["data"]["summary_location"]
        }

        print("  summary_clinical data...")

        summary_clinicals = []
        data = None
        offset = 0
        first = 50000
        max_retries = 3
        while data != []:  # don't change, it explicitly checks for empty list
            tries = 0
            while tries < max_retries:
                print(
                    "    Getting first {} records with offset: {}".format(first, offset)
                )
                query_string = (
                    "{ summary_clinical (first: "
                    + str(first)
                    + ", offset: "
                    + str(offset)
                    + ', project_id: "'
                    + self.project_id
                    + '") { submitter_id } }'
                )

                query_res = None
                try:
                    query_res = self.query_peregrine(query_string)
                except:
                    print(f"Peregrine did not return JSON: {response.text}")

                if query_res:
                    data = query_res["data"]["summary_clinical"]
                    summary_clinicals.extend(data)
                    offset += first
                    break
                else:
                    tries += 1
                    print("    Trying again (#{})".format(tries))
                    sleep(2)  # wait 2 seconds - can change to exponential backoff later
            assert (
                tries < max_retries
            ), "    Unable to query Peregrine for existing 'summary_clinical' data"

        for sc in summary_clinicals:
            sc_id = sc["submitter_id"]
            location_id = sc_id.replace("summary_clinical", "summary_location")
            location_id = "_".join(location_id.split("_")[:-1])  # remove the date
            json_res[location_id].append(sc_id)

        return json_res

    def get_latest_submitted_date_idph(self):
        """
        Queries Guppy for the existing `location` data.
        Returns the latest submitted date as Python "datetime.date"
        """
        print("Getting latest date from Guppy...")
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

    async def query_peregrine_async(self, query_string):
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
