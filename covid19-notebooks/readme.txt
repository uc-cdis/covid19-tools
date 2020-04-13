Steps to run mounted notebook in chicagoland pandemicresponse data common.

1. Login in https://chicagoland.pandemicresponsecommons.org using you user id and password.

2. Navigate to Profile and click Create API key. Then click Download json to save the credential.json file locally.

3. Navigate to Workspace, select the mounted notebook you would like to run (it will take several minutes to set up the notebook).

4. Upload your credential.json file under /home/jovyan/pd.

5. Double click to open the notebook. From the navigation bar on the top left, click Run and then select Run All Cells to execute the notebook.


Jupyter Notebook overview:

In chicago-seir-forcast notebook, we construct an SEIR model for COVID-19 in Cook County, Illinois, using data sourced from Johns Hopkins University, but available within the Chicagoland COVID-19 Commons. We then perform an optimization of initial model parameter values and do some simple validation. This notebook is intended to demonstrate real-life usage of data for epidemiological modeling and is not intended for rigorous scientific interpretation.
