# COVID-19 Notebooks


## Steps to run a mounted notebook in the Chicagoland Pandemic Response Data Commons

1. Login at https://chicagoland.pandemicresponsecommons.org using your username and password.

2. Navigate to the `Workspace` page, select the workspace named `A collection of Jupyter notebooks to explore health outcomes for COVID-19` (it will take several minutes to launch).
3. Navigate to the `covid19-notebook` directory. Double click on the notebook you wish to run to open it. From the navigation bar on the top left, click "Run" and then select "Run All Cells" to execute the notebook. If you are running the notebook in our Workspace, you should not need to install dependencies.
4. If a notebook requires an API key, run [Gen3 Auth Helper](https://uc-cdis.github.io/gen3sdk-python/_build/html/auth.html) as follows:

```
# Import Gen3Auth
from gen3.auth import Gen3Auth
# Generate the Gen3Auth class pointed at the data commons the user is in
auth = Gen3Auth()
```


## Notebooks overview

### "covid19_seir" notebook:

In chicago-seir-forcast notebook, we construct an SEIR model for COVID-19 in Cook County, Illinois, using data sourced from Johns Hopkins University, but available within the Chicagoland COVID-19 Commons. We then perform an optimization of initial model parameter values and do simple validation. This notebook is intended to demonstrate real-life usage of data for epidemiological modeling and is not intended for rigorous scientific interpretation.

### "COVID-19-JHU_data_analysis_04072020" notebook:

In this notebook, we demonstrate the visualization of the Johns Hopkins COVID-19 data currently available in the Chicagoland Pandemic Response Commons. The results from this notebook are purely for demonstration purposes and should not be interpreted as scientifically rigorous. We plotted the trend of confirmed, deaths and recovered infected cases from January 22, 2020 and we focus on China, US, Italy, France, and Spain.

### "kaggle_data_analysis_04072020" notebook:

In this notebook, we explore some of the demographic data associated with COVID-19 cases in the Chicagoland Pandemic Response Commons. Specifically, we focus on the individual-level dataset from Kaggle stratified by age and gender, which allows us to explore the demographic composition of the infected population, specifically gender and age structure.

### "extended-seir" notebook:

In this notebook we implement an extended SEIR model of the COVID-19 outbreak,
fit probability distributions to model parameters, and apply monte carlo methods to the resulting stochastic model.

### "A live View of COVID-19's Global Presense" notebook:

In this R markdown, we track COVID-19 local (US) and global cases with active, confirmed, recovered and death toll on the map at the latest time point. The interactive maps indicate the concentration of coronavirus cases of across the US and around the world.

### "COVID-19 Testing Over Time in the US" notebook:

COVID-19 testing is very crucial to understand the spread of the pandemic. In this python notebook, we have focused our efforts on visualizing the COVID-19 testing data across the United States and U.S. Territories from the COVID Tracking Project. The animations will allow you to see how testing data has changed over time.

### "Cross Dataset Analysis on COVID-19 Symptoms and Fatality Rate" notebook:

In this notebook, we are performing visual analysis on the individual case reports that provide aggregated data on clinical states (eg., deceased, recovered, alive, etc) and symptoms. We are looking at whether the fatality rates and symptoms are similar across different datasets.

### "Demo: Exploring clinical data from multiple commons" notebook:

In this notebook, we obtain the clinical data for multiple projects from the Chicagoland Pandemic Response Commons and NCI Genomic Data Commons (GDC) and visualize some of the clinical attributes. This notebook is for demonstration purposes only and not biologically meaningful.

### "Visualize Statistical Summary Reports from Partnering Healthcare Providers" notebook:

This notebook provides an example of how to visualize the status of COVID-19 and general patient information from multiple regional healthcare providers using the demo Statistical Summary Report data from 03/10/2020 to 11/10/2020.

### "Google Mobility Data Visualization" notebook:

In this notebook, we demonstrate how to query the google mobility data using the BigQuery API client python library, and visualize the mobility changes over time across the United States since February 2020.


## "PFB_example" notebook:

Once a PFB is loaded into the workspace, either by uploading a PFB file or by exporting a PFB from explorer, the data contained in the PFB may be examined by converting to tsv files. This notebook provides a tutorial on how to export data contained in a PFB for analysis into a workspace environment using the PyPFB library via the Gen3 CLI and directly in python.

### "COVID-19 Data Visualization in Illinois" notebook:

This notebook provides a snapshot of publicly available data related to COVID-19 in Illinois. Graphics illustrate the age/race/gender demographics, total and daily administered vaccine doses, google mobility data and statistical model forecasts on cumulative and daily new COVID-19 cases in Illinois.
