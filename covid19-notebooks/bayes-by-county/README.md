# COVID-19 Model for IL by-county (Version 2)

## V2: Mobility Data

Use google mobility data to generate real-time estimates of Rt.

## Visualizations

Get written to `modelOutput/figures/`. 

## List of Counties

Each time you run the model, the list of counties used in the simulation gets written to `modelOutput/figures/CountyCodeList.txt`, even though it's not a "figure".

## Running The Model

To run the model:

```sh run.sh <stan_model> <minimumDeaths> <nIterations>```

Example:

```sh run.sh us_mobility 150 4000```

Output gets written to `modelOutput/explorePlots/`, `modelOutput/figures/`, and `modelOutput/results`.

Some Rough Time Benchmarks:
- 9 counties for 4000 iterations: 9hrs

These times are from running the model on Matt's laptop, without any kind of extra optimization etc.

## A Few Notes On Version 1

- Input: reported deaths
- Output: Rt, modeled deaths, modeled true cases
- The model seems to be pretty good - correlation of 0.9478155 on 9 counties modeled deaths vs. reported deaths, validated over the time period June 2-5. So we input data up through June 1st, forecasted forward, and compared forecast to observed. 
- Rt is a step-function, drops at lockdown from initial R0, which is computed by-county based on early observed deaths
- Rt model "expires", so to speak, on June 1st, but there is a lag before the effect shows up - estimates will begin looking off in the final third of June (so replace the Rt model asap, if not sooner)
- More observations -> better estimates. Counties with small number of reported deaths tend to have bigger posterior credible intervals
- Not all counties have sufficient data to make halfway decent estimates, so set a minimum cumulative deaths cutoff (e.g., 10, 300, ...)
- A cutoff of 10 yields a set of ~26 counties
- Use the bash script to run the model, always.
- The bash script runs the etl (fetches latest JHU data) and then runs the model with the parameters you pass to it
- The IL age distribution is applied uniformly across all counties in IL -> this impacts the age-stratified IFR (the age distribution by-county is available, it just hasn't been incorporated into the model yet)
