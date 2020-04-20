import json
from math import ceil

import requests


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

    def add_record_to_submit(self, record):
        self.records_to_submit.append(record)

    def batch_submit_records(self):
        """
        Submits Sheepdog records in batch
        """
        if not self.records_to_submit:
            print("  Nothing new to submit")
            return

        n_batches = ceil(len(self.records_to_submit) / self.submit_batch_size)
        for i in range(n_batches):
            records = self.records_to_submit[
                      i * self.submit_batch_size: (i + 1) * self.submit_batch_size
                      ]
            print(
                "  Submitting {} records: {}".format(
                    len(records), [r["submitter_id"] for r in records]
                )
            )

            response = requests.put(
                "{}/api/v0/submission/{}/{}".format(
                    self.base_url, self.program_name, self.project_code
                ),
                headers=self.headers,
                data=json.dumps(records),
            )
            assert (
                    response.status_code == 200
            ), "Unable to submit to Sheepdog: {}\n{}".format(
                response.status_code, response.text
            )

        self.records_to_submit = []
