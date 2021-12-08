# Generative Logic to Predict COVID-19 Reproduction Factor and Daily Confirmed Incidents

#### Fan Wang
#### 12/08/2021
-------------------

$R_0$ ("R-naught") describes the reproduction factor of an epidemic disease, i.e. how many other people does one infected person pass the disease to. If this quantity is larger than 1 we have an epidemic on our hands, as is the case for COVID-19.

R0 assumes, however, that there are no counter-measures being implemented to curb spread of the virus. Thus, the more critical measure to track is Re(t), i.e. the time-changing effective reproduction factor, i.e. on a given day t, how many people does one person infect.

As lockdowns and social distancing measures are put in place we expect this quantity to drop, ideally below the critical quantity of 1 because then, over time, the disease would just wimper out.

Usually we'd extract Re(t) from something like a SIR (susceptible-infected-recovered) model or an SEIR (which adds an exposed term), which are classic epidemiological compartment models.

SIR model is in fact what Chicagoland Pandemic Response Commons used in the beginning. However, SIR or SEIR models are also just approximations of the real thing and come with quite a few assumptions baked in. The current model is simpler and makes fewer assumptions. In addition, the SIR model is described as an ODE (Ordinary Differential Equations) which causes various technical problems. Solving the ODE is quite time-intensive and while closed-form approximations exist and are faster, we found that they are quite unreliable.

Instead, the newly proposed model uses a simple generative logic to explain how an initial pool of infected people spreads the disease at each time point, according to the current reproduction factor.

## Objectives

* Build generative models that take into account knowledge we have about the daily new incident data generation process.

* Apply Bayes formula to reason backwards from observed data to unobserved hidden causes.

* Model time-varying processes.

* Model Infection delay (generation time).



## Model Design

1. Primary infection: $$ y_t = y_{t-1} \cdot R_0 $$

2. Time-varying reproduction rate: $$ y_t = y_{t-1} \cdot R_e(t) $$

3. Infection delay (generation time): $$ y_t = \sum_{i=1}^{M}y_{t-i} R_e(t-i) g_i  $$

4. Onset delay: Onset delay is defined as the delay between noticeable symptoms and reported as a positive case. To estimate onset delay distribution, we could use individual-level line list data from the [Open COVID-19 Data Working Group](https://github.com/beoutbreakprepared/nCoV2019/tree/master/latest_data ) which asked patients how long ago their symptoms started.


## Results and Exprected Outputs

* Reproduction rate prediction in Cook County:

![Alt text](images/cook_county_rt.svg?raw=true "Title")

* Daily confirmed cases prediction (with 15-days forecast) in Cook County:

![Alt text](images/cook_county_daily.svg?raw=true "Title")

## Directory layout
        .
        ├── model_ealuation               # Direcory to evaluate the model prediction power
        │   ├── model_evaluation.py       # Python scripts to evaluate model prediction power
        │   ├── images                    # Exprected output for model evaluation
        │   └── readme.md                 # How to use model_evaluation.py
        ├── Dockerfile                    # Dockerfile for running generative model and model evaluation
        ├── images                        # Exprected outputs for generative model
        ├── p_delay.csv                   # Onset delay data between noticeable symptoms and reported as a positive case
        ├── gbm-run.sh                    # Runs the model script (pymc3_generative_model.py) and uploads the results to S3
        ├── gbm-run-with-slack.sh         # Depending on whether gbm-run.sh runs successfully or not, send a success or failure Slack notification
        ├── pymc3_generative_model.py     # Build generative model and generate predictions
        └── readme.md                     # Current file


## Reference
1. https://www.ijidonline.com/article/S1201-9712%2820%2930119-3/pdf
2. https://staff.math.su.se/hoehle/blog/2020/04/15/effectiveR0.html
3. https://github.com/beoutbreakprepared/nCoV2019
4. https://github.com/CSSEGISandData/COVID-19
