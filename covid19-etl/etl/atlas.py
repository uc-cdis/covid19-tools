"""
Note that this ETL cannot be run in the VM because it needs too much memory.
Specifically, county_outcomes.csv should not be read in memory. To run it in
the VM, we should (generate JSON submissions instead of TSV, and) read
county_outcomes.csv as below, which means removing uses of pandas. Since this
is a one-time ETL, keeping as is for now.
    with zipfile.ZipFile(<zip file>) as zf:
        with io.TextIOWrapper(<csv file>, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=",")
            <parse rows one by one>
"""

from gen3.submission import Gen3Submission
import os
import pandas as pd
import pathlib
import requests
import shutil
import time
import zipfile

from etl import base
from helper.metadata_helper import MetadataHelper


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
TEMP_DIR = os.path.join(CURRENT_DIR, "atlas_temp_files")


class TokenAuth(requests.auth.AuthBase):
    def __init__(self, access_token):
        self.access_token = access_token

    def __call__(self, request):
        request.headers["Authorization"] = "Bearer " + self.access_token
        return request


class ATLAS(base.BaseETL):
    def __init__(self, base_url, access_token, s3_bucket):
        super().__init__(base_url, access_token, s3_bucket)

        self.program_name = "open"
        self.project_code = "ATLAS"
        self.metadata_helper = MetadataHelper(
            base_url=self.base_url,
            program_name=self.program_name,
            project_code=self.project_code,
            access_token=access_token,
        )

        self.nodes = {
            "summary_location": [],
            "summary_socio_demographic": [],
        }

    def files_to_submissions(self):
        start = time.time()

        # metadata file locations
        outcome_url = "https://opportunityinsights.org/wp-content/uploads/2018/10/county_outcomes.zip"
        outcome_file_name = "county_outcomes"
        neighbor_url = "https://opportunityinsights.org/wp-content/uploads/2018/12/cty_covariates.csv"
        neighbor_file_path = os.path.join(TEMP_DIR, "cty_covariates.csv")
        fips_url = "https://github.com/GL-Li/totalcensus/blob/master/data_raw/all-geocodes-v2016%20.xlsx?raw=true"

        pathlib.Path(TEMP_DIR).mkdir(exist_ok=True)

        # obtain files
        print("Getting data from {}".format(outcome_url))
        outcome_response = requests.get(outcome_url)
        with open(os.path.join(TEMP_DIR, outcome_file_name) + ".zip", "wb") as outfile:
            outfile.write(outcome_response.content)

        print("Getting data from {}".format(neighbor_url))
        neighbor_response = requests.get(neighbor_url)
        with open(neighbor_file_path, "wb") as outfile:
            outfile.write(neighbor_response.content)

        # obtain FIPS data
        print("Getting data from {}".format(fips_url))
        fips_data = pd.read_excel(
            fips_url, dtype=object, keep_default_na=False, skiprows=4
        )

        # read in files
        print("Reading downloaded files...")
        zf = zipfile.ZipFile(os.path.join(TEMP_DIR, outcome_file_name) + ".zip")
        outcome_url_data = pd.read_csv(
            zf.open(outcome_file_name + ".csv"),
            low_memory=False,
            dtype=object,
            keep_default_na=False,
        )
        neighbor_url_data = pd.read_csv(
            neighbor_file_path, dtype=object, keep_default_na=False
        )

        # columns names and mappings to DD variables
        print("Converting to Sheepdog submissions...")
        outcome_data_col = {
            "coll_pooled_pooled_n": "college_degree",
            "comcoll_pooled_pooled_n": "community_college_degree",
            "county": "county",
            "grad_pooled_pooled_n": "graduate_degree",
            "has_dad_pooled_pooled_n": "has_dad",
            "has_mom_pooled_pooled_n": "has_mom",
            "hours_wk_pooled_pooled_n": "hours_weekly_worked_prior",
            "hs_pooled_pooled_n": "completed_high_school",
            "jail_pooled_pooled_n": "jail",
            "kfr_imm_pooled_pooled_n": "household_income_immigrated",
            "kfr_native_pooled_pooled_n": "household_income_native",
            "kfr_pooled_pooled_n": "household_income",
            "kfr_stycz_pooled_pooled_n": "household_income_childhood_commuting_zone",
            "kfr_top01_pooled_pooled_n": "household_income_probability_top01",
            "kfr_top20_pooled_pooled_n": "household_income_probability_top20",
            "kid_pooled_pooled_blw_p50_n": "kids_household_income_below_median",
            "kid_pooled_pooled_n": "kids_count",
            "lpov_nbh_pooled_pooled_n": "kids_poverty_below_10p",
            "married_pooled_pooled_n": "married",
            "pos_hours_pooled_pooled_n": "hours_positive_worked_prior",
            "proginc_pooled_pooled_n": "received_public_assistance_income",
            "somecoll_pooled_pooled_n": "some_college_experience",
            "spouse_rk_pooled_pooled_n": "spouse_income_rank",
            "state": "state",
            "staycz_pooled_pooled_n": "kids_stayed_in_commuting_zone",
            "stayhome_pooled_pooled_n": "kids_live_with_parents",
            "teenbrth_pooled_female_n": "teenbirths",
            "two_par_pooled_pooled_n": "has_two_parents",
            "wgflx_rk_pooled_pooled_n": "hourly_wage_rank",
            "working_pooled_pooled_n": "working",
        }
        neighbor_data_col = {
            "ann_avg_job_growth_2004_2013": "ann_avg_job_growth",
            "county": "county",
            "emp2000": "employment",
            "foreign_share2010": "foreign_share",
            "frac_coll_plus2010": "frac_coll_plus",
            "gsmn_math_g3_2013": "gsmn_math_g3",
            "hhinc_mean2000": "hhinc_mean",
            "job_density_2013": "job_density",
            "ln_wage_growth_hs_grad": "ln_wage_growth_hs_grad",
            "mail_return_rate2010": "mail_return_rate",
            "mean_commutetime2000": "mean_commutetime",
            "med_hhinc2016": "med_hhinc",
            "poor_share2010": "poor_share",
            "popdensity2010": "popdensity",
            "rent_twobed2015": "rent_twobed",
            "share_asian2010": "share_asian",
            "share_black2010": "share_black",
            "share_hisp2010": "share_hisp",
            "share_white2010": "share_white",
            "singleparent_share2010": "singleparent_share",
            "state": "state",
            "traveltime15_2010": "traveltime15",
        }

        # filter out columns from the data set and covnert to DD variables
        outcome_data = outcome_url_data[outcome_data_col.keys()]
        outcome_data.columns = list(outcome_data_col.values())

        neighbor_data = neighbor_url_data[neighbor_data_col.keys()]
        neighbor_data.columns = list(neighbor_data_col.values())

        # join data sets for submission
        atlas_data = pd.merge(outcome_data, neighbor_data)
        atlas_data["FIPS"] = ""

        for x in range(len(atlas_data)):
            # create FIPS column
            atlas_data["state"][x] = atlas_data["state"][x].zfill(2)
            atlas_data["county"][x] = atlas_data["county"][x].zfill(3)
            atlas_data["FIPS"][x] = atlas_data["state"][x] + atlas_data["county"][x]

            # change numbers to human readable
            state = atlas_data.loc[(x), "state"]
            county = atlas_data.loc[(x), "county"]
            df_sub = fips_data[fips_data["State Code (FIPS)"] == state]
            df_sub_state = df_sub[df_sub["Place Code (FIPS)"] == "00000"]
            df_sub_state = df_sub_state[df_sub_state["County Code (FIPS)"] == "000"]
            df_sub_county = df_sub[df_sub["County Code (FIPS)"] == county]
            df_sub_state = df_sub_state.reset_index(drop=True)
            df_sub_county = df_sub_county.reset_index(drop=True)
            atlas_data["state"][x] = df_sub_state[
                "Area Name (including legal/statistical area description)"
            ][0]
            atlas_data["county"][x] = df_sub_county[
                "Area Name (including legal/statistical area description)"
            ][0]

        # create submission for summary location node
        summary_loc_sub = atlas_data[{"state", "county", "FIPS"}]
        summary_loc_sub["country_region"] = "US"
        summary_loc_sub["projects.code"] = "ATLAS"
        summary_loc_sub["type"] = "summary_location"
        summary_loc_sub["county"] = summary_loc_sub["county"].str.replace(" County", "")
        summary_location_submitter_id = (
            "summary_location_"
            + summary_loc_sub["country_region"]
            + "_"
            + summary_loc_sub["state"]
            + "_"
            + summary_loc_sub["county"]
        ).str.replace(" ", "_")
        summary_loc_sub["submitter_id"] = summary_location_submitter_id
        summary_loc_sub["province_state"] = summary_loc_sub["state"]
        summary_loc_sub = summary_loc_sub.drop("state", axis=1)

        # create submission for summary socio-demographic node
        summary_sociodem_sub = atlas_data
        summary_sociodem_sub["country_region"] = "US"
        summary_sociodem_sub["county"] = summary_sociodem_sub["county"].str.replace(
            " County", ""
        )
        summary_location_submitter_id = (
            "summary_location_"
            + summary_sociodem_sub["country_region"]
            + "_"
            + summary_sociodem_sub["state"]
            + "_"
            + summary_sociodem_sub["county"]
        ).str.replace(" ", "_")
        summary_sociodem_sub[
            "summary_locations.submitter_id"
        ] = summary_location_submitter_id

        summary_sociodem_submitter_id = (
            "summary_sociodem_"
            + summary_sociodem_sub["country_region"]
            + "_"
            + summary_sociodem_sub["state"]
            + "_"
            + summary_sociodem_sub["county"]
        ).str.replace(" ", "_")
        summary_sociodem_sub["submitter_id"] = summary_sociodem_submitter_id

        summary_sociodem_sub["type"] = "summary_socio_demographic"
        summary_sociodem_sub = summary_sociodem_sub.drop(
            ["country_region", "FIPS", "state", "county"], axis=1
        )

        # create submission files
        summary_loc_sub.to_csv(
            os.path.join(TEMP_DIR, "summary_location_submission.tsv"),
            sep="\t",
            index=False,
        )
        summary_sociodem_sub.to_csv(
            os.path.join(TEMP_DIR, "summary_socio_demographic_submission.tsv"),
            sep="\t",
            index=False,
        )

        print("Done in {} secs".format(int(time.time() - start)))

    def submit_metadata(self):
        print("Submitting data...")

        # Gen3 submission via python SDK
        sub = Gen3Submission(self.base_url, TokenAuth(self.access_token))
        project_id = self.program_name + "-" + self.project_code
        sub.submit_file(
            project_id,
            os.path.join(TEMP_DIR, "summary_location_submission.tsv"),
            chunk_size=100,
        )
        sub.submit_file(
            project_id,
            os.path.join(TEMP_DIR, "summary_socio_demographic_submission.tsv"),
            chunk_size=100,
        )

        # clean up
        shutil.rmtree(TEMP_DIR)
