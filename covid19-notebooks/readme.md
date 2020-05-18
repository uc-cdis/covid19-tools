# COVID-19 Notebooks


## Steps to run a mounted notebook in the Chicagoland Pandemic Response Data Commons

1. Login at https://chicagoland.pandemicresponsecommons.org using your username and password.

2. Navigate to the `Profile` page and click `Create API key`. Then click `Download JSON` to save the "credential.json" file locally.

3. Navigate to the `Workspace` page, select the workspace you would like to work in (it will take several minutes to launch).

4. Upload your `credential.json` file in the `pd` (for "persistent drive") directory.

5. Navigate back to the home directory, and then to the "covid19-notebook" directory. Double click on the notebook you wish to run to open it. From the navigation bar on the top left, click "Run" and then select "Run All Cells" to execute the notebook. If you are running the notebook in our Workspace, you should not need to install dependencies.


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
