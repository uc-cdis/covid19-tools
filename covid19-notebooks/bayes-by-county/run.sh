#!/bin/bash

# call form:
# sh run.sh <stan_model> <minimumDeaths> <nIterations>

# example call:
# sh run.sh us_base 150 4000 [--validate]

echo "\n--- running input ETL and model with these parameters ---"
echo 'stanModel = ' $1
echo 'minimumDeaths = ' $2
echo 'mcmcIterations = ' $3
echo 'validationFlag = ' $4

# run the etl to generate all input tables
echo "\n- Input ETL -"
cd py
python3 etl.py

## MOBILITY DATA
echo "\n- Fetch Mobility Data  -"
cd ../modelInput/mobility/
wget --secure-protocol=TLSv1 -O Global_Mobility_Report.csv https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv 
cd ./visit-data/
sh get-visit-data.sh
cd ../../../r
Rscript mobility-regression.r > /dev/null 2>&1

# run the model via R script
echo "\n- Model Run -"
# cd ../r
Rscript base.r $1 $2 $3 $4

cd ..

echo "\n- Routine Completed -\n"