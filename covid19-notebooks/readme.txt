# Steps to run a mounted notebook in the Chicagoland PandemicResponse Data Commons

1. Login at https://chicagoland.pandemicresponsecommons.org using your username and password.

2. Navigate to the "Profile" page and click "Create API key". Then click "Download JSON" to save the "credential.json" file locally.

3. Navigate to the "Workspace" page, select the workspace you would like to work in (it will take several minutes to launch).

4. Upload your "credential.json" file in the "pd" (for "persistent drive") directory.

5. Navigate back to the home directory, and then to the "covid19-notebook" directory. Double click on the notebook you wish to run to open it. From the navigation bar on the top left, click "Run" and then select "Run All Cells" to execute the notebook. If you are running the notebook in our Workspace, you should not need to install dependencies.


# Notebooks overview

## "covid19_seir" notebook

In chicago-seir-forcast notebook, we construct an SEIR model for COVID-19 in Cook County, Illinois, using data sourced from Johns Hopkins University, but available within the Chicagoland COVID-19 Commons. We then perform an optimization of initial model parameter values and do simple validation. This notebook is intended to demonstrate real-life usage of data for epidemiological modeling and is not intended for rigorous scientific interpretation.

## "COVID-19-JHU_data_analysis_04072020" notebook

## "kaggle_data_analysis_04072020" notebook
