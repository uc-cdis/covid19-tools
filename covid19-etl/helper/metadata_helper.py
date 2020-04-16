import datetime
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

    def get_existing_data_jhu(self):
        """
        Queries Peregrine for the existing `location` and `time_series` data. Returns a dict in format { "location1": [ "date1", "date2" ] }

        Note: if we end up having too much data, the query may timeout. We
        could simplify this by assuming that any `time_series` date that
        already exists for one location also already exists for all other
        locations (historically not true), and use the following query to
        retrieve the dates we already have data for:
        { location (first: 1, project_id: <...>) { time_seriess (first: 0) { date } } }
        Or use the `first` and `offset` Peregrine parameters
        We could also query Guppy instead (assuming the Guppy ETL ran since
        last time this ETL ran), or get the existing data directly from the DB.
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
            print("Peregrine did not return JSON")
            raise
        json_res = {
            location["submitter_id"]: []
            for location in query_res["data"]["summary_location"]
        }

        print("  summary_report data...")

        summary_reports = []
        data = None
        offset = 0
        first = 10000
        while data != []:  # don't change, it's explicitly checks for empty list
            print("    Getting data with offset: " + str(offset))
            query_string = (
                '{ summary_report (first: ' + str(first) + ', offset: ' + str(offset) + ', order_by_desc: "date", project_id: "'
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
            ), "Unable to query Peregrine for existing 'summary_report' data: {}\n{}".format(
                response.status_code, response.text
            )
            try:
                query_res = json.loads(response.text)
            except:
                print("Peregrine did not return JSON")
                raise
            data = query_res["data"]["summary_report"]
            summary_reports.extend(data)
            offset += first

        for report in summary_reports:
            report_id = report["submitter_id"]
            location_id = report_id.replace("summary_report", "summary_location")
            location_id = "_".join(location_id.split("_")[:-1])  # remove the date
            json_res[location_id].append(report_id)

        return json_res

    def get_latest_submitted_data_idph(self):
        """
        Queries Peregrine for the existing `summary_report` data.

        { summary_report (first: 1, project_id: <...>) { date } }

        Returns the latest submitted date as Python "datetime.date"
        """
        print("Getting latest date from Peregrine...")
        query_string = (
            '{ summary_report (first: 1, order_by_desc: "date", project_id: "'
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
            print("Peregrine did not return JSON")
            raise

        report = query_res["data"]["summary_report"][0]
        latest_submitted_date = datetime.datetime.strptime(report["date"], "%Y-%m-%d")
        return latest_submitted_date.date()

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
                i * self.submit_batch_size : (i + 1) * self.submit_batch_size
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
