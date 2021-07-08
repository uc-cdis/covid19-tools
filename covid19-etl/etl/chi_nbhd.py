import datetime
from contextlib import closing

from etl import base
from utils.format_helper import derived_submitter_id, format_submitter_id
from utils.metadata_helper import MetadataHelper


class CHI_NBHD(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)
        self.summary_locations = []
        self.summary_clinicals = []

        self.program_name = "open"
        self.project_code = "CHI-NBHD"

        self.country = "US"
        self.state = "IL"

        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

    def files_to_submissions(self):
        """
        Reads JSON file and convert the data to Sheepdog records
        """
        url = "https://covid19neighborhoods.southsideweekly.com/page-data/index/page-data.json"
        self.parse_file(url)

    def parse_file(self, url):
        print("Getting data from {}".format(url))
        with closing(self.get(url, stream=True)) as r:
            data = r.json()
            data = data["result"]["data"]
            build_time_str = data["build_time"]["nodes"][0]["buildTime"]
            build_time = datetime.datetime.strptime(
                build_time_str, "%Y-%m-%dT%H:%M:%S.%fZ"
            )
            current_date = build_time.strftime("%Y-%m-%d")
            nbhd_stats = data["community_areas_all"]["nodes"][0]["childGeoJson"][
                "features"
            ]

            for nbhd_object in nbhd_stats:
                summary_location, summary_clinical = self.parse_nbhd(
                    nbhd_object, current_date
                )

                self.summary_locations.append(summary_location)
                self.summary_clinicals.append(summary_clinical)

                print(summary_location)
                print(summary_clinical)

    def parse_nbhd(self, nbhd_object, date):
        properties = nbhd_object["properties"]
        nbhd = properties["community"]
        deaths = properties["value"]
        population = properties["population"]

        summary_location_submitter_id = format_submitter_id(
            "summary_location",
            {"country": self.country, "state": self.state, "nbhd": nbhd},
        )

        summary_location = {
            "submitter_id": summary_location_submitter_id,
            "community_area": nbhd,
            "projects": [{"code": self.project_code}],
        }

        summary_clinical_submitter_id = derived_submitter_id(
            summary_location_submitter_id,
            "summary_location",
            "summary_clinical",
            {"date": date},
        )

        summary_clinical = {
            "submitter_id": summary_clinical_submitter_id,
            "date": date,
            "deaths_per_10000": round(10000 * deaths / population, 2),
            "deaths": deaths,
            "summary_locations": [{"submitter_id": summary_location_submitter_id}],
        }

        return summary_location, summary_clinical

    def submit_metadata(self):
        print("Submitting summary_location data")
        for loc in self.summary_locations:
            loc_record = {"type": "summary_location"}
            loc_record.update(loc)
            self.metadata_helper.add_record_to_submit(loc_record)
        self.metadata_helper.batch_submit_records()

        print("Submitting summary_clinical data")
        for sc in self.summary_clinicals:
            sc_record = {"type": "summary_clinical"}
            sc_record.update(sc)
            self.metadata_helper.add_record_to_submit(sc_record)
        self.metadata_helper.batch_submit_records()
