import json
import os
import requests
from time import sleep

from utils.metadata_helper import MetadataHelper

# base_url = "https://qa-covid19.planx-pla.net"
base_url = "https://chicagoland.pandemicresponsecommons.org"
access_token = os.environ.get("ACCESS_TOKEN")


def main():
    headers = {"Authorization": f"bearer {access_token}"}
    program = "open"
    project = "IDPH-Vaccine"
    node = "summary_group_demographics"
    records = get_existing_data(base_url, program, project, node, headers)

    metadata_helper = MetadataHelper(
        base_url=base_url,
        program_name=program,
        project_code=project,
        access_token=access_token,
    )
    print(f"Submitting {node} data")
    races = [
        "American Indian or Alaskan Native",
        "Asian",
        "Black",
        "Hispanic",
        "Left Blank",
        "Other",
        "Native Hawaiian or Other Pacific Islander",
        "White",
        "Unknown",
        "Multi-racial",
        "Unspecified",
        "Middle Eastern or North African",
        None,
    ]
    mapping = {
        "American Indian or Alaska Native": "American Indian or Alaskan Native",
    }
    for rec in records:
        if rec["race"] in races:
            continue
        rec["race"] = mapping[rec["race"]]
        rec["type"] = node
        metadata_helper.add_record_to_submit(rec)
    metadata_helper.batch_submit_records()


def get_existing_data(base_url, program, project, node, headers):
    project_id = f"{program}-{project}"
    first = 3000
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
                + '") { submitter_id, race, summary_clinicals { submitter_id } } }'
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
