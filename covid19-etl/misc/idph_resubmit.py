"""
Queries Peregrine for all the "summary_report" records and re-submits
them as "summary_clinical" records.
"""


import json
import os
import requests
from time import sleep
from sys import path

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
path.insert(0, os.path.join(CURRENT_DIR, ".."))
from helper.metadata_helper import MetadataHelper


##########
# config #
##########

base_url = "https://qa-covid19.planx-pla.net"
# base_url = "https://chicagoland.pandemicresponsecommons.org"
access_token = ""

program = "open"
# project = "IDPH"
project = "IDPH-zipcode"

old_node = "summary_report"
new_node = "summary_clinical"


def main():
    headers = {"Authorization": f"bearer {access_token}"}
    records = get_existing_data(base_url, program, project, old_node, headers)

    metadata_helper = MetadataHelper(
        base_url=base_url,
        program_name=program,
        project_code=project,
        access_token=access_token,
    )
    print(f"Submitting {new_node} data")
    for old_rec in records:
        new_rec = {"type": new_node, "project_id": f"{program}-{project}"}
        for key, value in old_rec.items():
            if value:
                new_rec[key] = value
        metadata_helper.add_record_to_submit(new_rec)
    metadata_helper.batch_submit_records()


def get_existing_data(base_url, program, project, node, headers):
    project_id = f"{program}-{project}"
    first = 50000
    max_retries = 3

    print(f"Getting {node} data from Peregrine...")
    records = []
    data = None
    offset = 0
    while data != []:  # don't change, it explicitly checks for empty list
        tries = 0
        while tries < max_retries:
            print("    Getting first {} records with offset: {}".format(first, offset))
            query_string = (
                "{ "
                + node
                + " (first: "
                + str(first)
                + ", offset: "
                + str(offset)
                + ', project_id: "'
                + project_id
                + '") { summary_locations { submitter_id }, submitter_id, date, confirmed, deaths, testing } }'
            )
            response = requests.post(
                "{}/api/v0/submission/graphql".format(base_url),
                json={"query": query_string, "variables": None},
                headers=headers,
            )

            query_res = None
            if response.status_code == 200:
                try:
                    query_res = json.loads(response.text)
                except Exception as e:
                    print("Peregrine did not return JSON: {}".format(e))
            else:
                print(
                    "    Unable to query Peregrine for existing '{}' data: {}\n{}".format(
                        node, response.status_code, response.text
                    )
                )

            if query_res:
                data = query_res["data"][node]
                records.extend(data)
                offset += first
                break
            else:
                tries += 1
                print("    Trying again (#{})".format(tries))
                sleep(2)  # wait 2 seconds - can change to exponential backoff later
        assert (
            tries < max_retries
        ), f"    Unable to query Peregrine for existing '{node}' data"

    return records


if __name__ == "__main__":
    main()
