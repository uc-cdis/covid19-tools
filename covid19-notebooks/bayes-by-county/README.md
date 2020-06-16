# COVID-19 Model for the US by-county

To run the model:

```sh run.sh <stan_model> <minimumDeaths> <nIterations>```

Example:

```sh run.sh us_base 150 4000```

Output gets written to `modelOutput/explorePlots/`, `modelOutput/figures/`, and `modelOutput/results`.

Some Rough Time Benchmarks:
- 5 counties for 8000 iterations: 20min
- 25 counties for 8000 iterations: 165min
- 5 counties for 24000 iterations: 66min
- 9 counties for 24000 iterations: 106min

These times are from running the model on Matt's laptop, without any kind of extra optimization etc.

## More Details and Comments

To come.