import asyncio
from aiohttp import ClientSession
import datetime
import json
from math import ceil
from time import sleep
from retry import retry

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
        response = requests.post(
            "{}/api/v0/submission/graphql".format(self.base_url),
            json={"query": query_string, "variables": None},
            headers=self.headers,
        )
        assert (
            response.status_code == 200
        ), "Unable to query Peregrine for existing 'summary_location' data: {}\n{}".format(
            response.status_code, response.text
        )
        try:
            query_res = json.loads(response.text)
        except:
            print(f"Peregrine did not return JSON: {response.text}")
            raise
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
                response = requests.post(
                    "{}/api/v0/submission/graphql".format(self.base_url),
                    json={"query": query_string, "variables": None},
                    headers=self.headers,
                )

                query_res = None
                if response.status_code == 200:
                    try:
                        query_res = json.loads(response.text)
                    except:
                        print(f"Peregrine did not return JSON: {response.text}")
                else:
                    print(
                        "    Unable to query Peregrine for existing 'summary_clinical' data: {}\n{}".format(
                            response.status_code, response.text
                        )
                    )

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
        Queries Peregrine for the existing `summary_clinical` data.

        { summary_clinical (first: 1, project_id: <...>) { date } }

        Returns the latest submitted date as Python "datetime.date"
        """
        print("Getting latest date from Peregrine...")
        query_string = (
            '{ summary_clinical (first: 1, order_by_desc: "date", project_id: "'
            + self.project_id
            + '") { submitter_id date } }'
        )
        response = requests.post(
            "{}/api/v0/submission/graphql".format(self.base_url),
            json={"query": query_string, "variables": None},
            headers=self.headers,
        )
        assert (
            response.status_code == 200
        ), "Unable to query Peregrine for existing data: {}\n{}".format(
            response.status_code, response.text
        )
        try:
            query_res = json.loads(response.text)
        except:
            print(f"Peregrine did not return JSON: {response.text}")
            raise

        if len(query_res["data"]["summary_clinical"]) < 1:
            return None

        sc = query_res["data"]["summary_clinical"][0]
        latest_submitted_date = datetime.datetime.strptime(sc["date"], "%Y-%m-%d")
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

    async def query_node_data(self, query_string):
        async def _post_request(headers, query_string):
            url = f"{self.base_url}/api/v0/submission/graphql"
            async with ClientSession() as session:
                async with session.post(
                    url,
                    json={"query": query_string, "variables": None},
                    headers=headers,
                ) as response:
                    response.raise_for_status()
                    response = await response.json()
                    return response

        return await _post_request(self.headers, query_string)
