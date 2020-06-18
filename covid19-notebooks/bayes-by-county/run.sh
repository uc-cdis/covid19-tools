#!/bin/bash

# call form:
# sh run.sh <stan_model> <minimumDeaths> <nIterations>

# example call:
# sh run.sh us_base 150 4000

echo "\n--- running input ETL and model with these parameters ---"
echo 'stanModel = ' $1
echo 'minimumDeaths = ' $2
echo 'mcmcIterations = ' $3

# run the etl to generate all input tables
echo "\n- Input ETL -"
cd py
python3 etl.py

# run the model via R script
echo "\n- Model Run -"
cd ../r
Rscript base.r $1 $2 $3

cd ..

echo "\n- Routine Completed -\n"