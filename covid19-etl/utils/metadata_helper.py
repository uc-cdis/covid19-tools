from aiohttp import ClientSession
import datetime
import json
from math import ceil
from time import sleep
from dateutil.parser import parse

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
            print(f"Unable to query Peregrine.\nQuery: {query_string}")
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

    def get_last_submission(self):
        query_string = (
            '{ project (first: 0, dbgap_accession_number: "'
            + self.project_code
            + '") { last_submission_identifier } }'
        )
        try:
            response = self.query_peregrine(query_string)
            if response["data"]["project"][0]["last_submission_identifier"] is None:
                return None
            return parse(response["data"]["project"][0]["last_submission_identifier"])
        except Exception as ex:
            print(
                f"Unable to query peregrine for last_submission_identifier. Detail {ex}"
            )
            raise

    def update_last_submission(self, last_submission_date_time):
        headers = {"content-type": "application/json"}
        headers["Authorization"] = self.headers["Authorization"]
        record = {
            "code": self.project_code,
            "dbgap_accession_number": self.project_code,
            "last_submission_identifier": last_submission_date_time,
        }
        try:
            res = requests.put(
                "{}/api/v0/submission/{}".format(self.base_url, self.program_name),
                headers=headers,
                data=json.dumps(record),
            )
        except Exception as ex:
            print(f"Unable to update last_submission_identifier. Detail {ex}")
            raise

    def delete_nodes(self, ordered_node_list):
        # copy of Gen3Submission.delete_nodes
        # TODO: once https://github.com/uc-cdis/gen3sdk-python/pull/72 is
        # merged, just use the SDK (need this PR so we can use an
        # access_token directly).
        import itertools

        batch_size = 200
        verbose = True
        project_id = f"{self.program_name}-{self.project_code}"
        for node in ordered_node_list:
            if verbose:
                print(node, end="", flush=True)
            first_uuid = ""
            while True:
                query_string = f"""{{
                    {node} (first: {batch_size}, project_id: "{project_id}") {{
                        id
                    }}
                }}"""
                res = self.query_peregrine(query_string)
                uuids = [x["id"] for x in res["data"][node]]
                if len(uuids) == 0:
                    break  # all done
                if first_uuid == uuids[0]:
                    raise Exception("Failed to delete. Exiting")
                first_uuid = uuids[0]
                if verbose:
                    print(".", end="", flush=True)

                    # copy of Gen3Submission.delete_records
                    api_url = "{}/api/v0/submission/{}/{}/entities".format(
                        self.base_url, self.program_name, self.project_code
                    )
                    for i in itertools.count():
                        uuids_to_delete = uuids[batch_size * i : batch_size * (i + 1)]
                        if len(uuids_to_delete) == 0:
                            break
                        output = requests.delete(
                            "{}/{}".format(api_url, ",".join(uuids_to_delete)),
                            headers=self.headers,
                        )
                        try:
                            output.raise_for_status()
                        except requests.exceptions.HTTPError:
                            print(
                                "\n{}\nFailed to delete uuids: {}".format(
                                    output.text, uuids_to_delete
                                )
                            )
                            raise

            if verbose:
                print()
