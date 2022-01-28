# COVID-19 Forecast in Cook County with SIR-based Bayesian Model

#### Fan Wang
#### Jan 26 2022

## Overview
We are interested in capturing the trend of evolving situation and providing prognosis of the disease with a robust measure with mathematical model aided analysis. In this repository, we use a discrete SIR (Susceptible-Infected-Recovered) model as a disease model to infer the COVID-19 propagation (daily and cumulative confirmed cases) in Cook County in Illinois. This work is based upon Priesemann Group's work for inferring the parameters for COVID-19 and performing predictions.

**Document source :** <https://covid19-inference.readthedocs.io/en/latest/index.html>

**Python code source :** <https://github.com/Priesemann-Group/covid19_inference>

## Model and Methods
### 1. SIR (Susceptible-Infected-Recovered) Model
In short, we assume that the disease spreads at rate *λ* from an infected person *(I)* to a susceptible person *(S)* and that an infected person becomes a recovered person *(R)* at rate *μ*, i.e.

*S + I → <sup>λ</sup> I + I*


*I → <sup>μ</sup> R*

This well-established model for disease spreading can be described by the following set of (deterministic) ordinary differential equations [see, e.g., Wikipedia or recent works on the spread of covid-19]. Within a population of size *N*,

<img src="https://render.githubusercontent.com/render/math?math=dS/dt=-%5Clambda SI/N">

<img src="https://render.githubusercontent.com/render/math?math=dI/dt=%5Clambda SI/N -%5Cmu I">

<img src="https://render.githubusercontent.com/render/math?math=dR/dt=%5Cmu I">

Because our data set is discrete in time (*Δt*=1 day), we solve the above differential equations with a discrete time step (*dI/dt≈ΔI/Δt*), such that

<img src="https://render.githubusercontent.com/render/math?math=S_t - S_{t-1}=-%5Clambda %5Cdelta tS_t/NI_{t-1}=:-I^{new}_t">

<img src="https://render.githubusercontent.com/render/math?math=R_t-R_t-1=%5Cmu %5Cdelta tI_{t-1}=:R^{new}_t">

<img src="https://render.githubusercontent.com/render/math?math=I_t-I_{t-1}=(%5Clambda S_{t-1}/N-%5Cmu )%5Cdelta tI_{t-1}=I^{new}_t-R^{new}_t">


Importantly, <img src="https://render.githubusercontent.com/render/math?math=I_t"> models the number of all active, (currently) infected people, while <img src="https://render.githubusercontent.com/render/math?math=I^{new}_t"> is the number of new infections that is reported according to standard WHO convention. Furthermore, we explicitely include a reporting delay <img src="https://render.githubusercontent.com/render/math?math=D"> between new infections <img src="https://render.githubusercontent.com/render/math?math=I^{new}_t"> and reported cases when generating the forecast.

### 2. Exponential growth during outbreak onset

Note that in the onset phase, only a tiny fraction of the population is infected *(I)* or recovered *(R)*, and thus *S≈N≫I* such that *S/N≈1*. Therefore, the differential equation for the infected reduces to a simple linear equation, exhibiting an exponential growth

<img src="https://render.githubusercontent.com/render/math?math=dI/dt=(%5Clambda -%5Cmu )I"> solved by <img src="https://render.githubusercontent.com/render/math?math=I(t)=I(0) e^{(%5Clambda -%5Cmu )t}">.

### 3. Estimating model parameters

We estimate the set of model parameters *θ*={*λ,μ,σ,I0*} using Bayesian inference with Markov-chain Monte-Carlo (MCMC). Our implementation relies on the python package pymc3 with NUTS (No-U-Turn Sampling).

The structure of our approach is the following:

* **Choose random initial parameters and evolve according to model equations**. Initially, we choose paramters θ from prior distributions that we explicitly specify below. Then, time integration of the model equations generates a (fully deterministic) time series of new infected cases <img src="https://render.githubusercontent.com/render/math?math=I^{new}(%5Ctheta)={I^{new}_t(%5Ctheta)}"> of the same length as the observed real-world data <img src="https://render.githubusercontent.com/render/math?math=I^{new}={I^{new}_t} ">.

* **Recursively update the parameters using MCMC**. The drawing of new candidate parameters and the time integration is repeated in every MCMC step. The idea is to propose new parameters and to accept them in a way that overall reduces the deviation between the model outcome and the real-world data. We quantify the deviation between the model outcome <img src="https://render.githubusercontent.com/render/math?math=I^{new}_t(%5Ctheta)"> and the real-world data <img src="https://render.githubusercontent.com/render/math?math=I^{new}_t"> for each step t of the time series with the local likelihood.

 <img src="https://render.githubusercontent.com/render/math?math=p(I^{new}_t |%5Ctheta )~StudentT%5Cnu =4(mean=I^{new}_t(%5Ctheta),width=%5Csigma \sqrt{I^{new}_t(%5Ctheta)})">

  We chose the Student’s t-distribution because it approaches a Gaussian distribution but features heavy tails, which make the MCMC more robust with respect to outliers [Lange et al, J. Am. Stat. Assoc, 1989]. The square-root width models the demographic noise of typical mean-field solutions for epidemic spreading [see, e.g., di Santo et al. (2017)].

  For each MCMC step, the new parameters are drawn so that a set of parameters that minimizes the previous deviation is more likely to be chosen. In our case, this is done with an advanced gradient-based method (NUTS). Every time integration that is performed (with its own set of parameters) yields one Monte Carlo sample, and the MCMC step is repeated to create the full set of samples. Eventually, the majority of sampled parameters will describe the real-world data well, so that consistent forecasts are possible in the forecast phase.

* **Forecast using Monte Carlo samples**. For the forecast, we take all samples from the MCMC step and continue time integration according to different forecast scenarios explained below. Note that the overall procedure yields an ensemble of predictions — as opposed to a single prediction that would be solely based on one set of (previously optimized) parameters.

## Results and Exprected Outputs

* Daily confirmed cases prediction (45-days forecast) in Cook County:

![Alt text](cook_county_daily_sir.svg?raw=true "Title")

* Cumulative confirmed cases prediction (45-days forecast) in Cook County:

![Alt text](cook_county_total_sir.svg?raw=true "Title")

### Reference
Acknowledgement: The model we used is not our own model. The model is based on the model developed by Priesemann Group.
1. Zhang, Y., You, C., Cai, Z., Sun, J., Hu, W., & Zhou, X.-H. (2020). Prediction of the COVID-19 outbreak based on a realistic stochastic model [Preprint]. Infectious Diseases (except HIV/AIDS). https://doi.org/10.1101/2020.03.10.20033803
2. Liu, Y. (n.d.). Estimating the Case Fatality Rate for the COVID-19 virus: A Markov Model Application. 17.
3. Song, P. X., Wang, L., Zhou, Y., He, J., Zhu, B., Wang, F., Tang, L., & Eisenberg, M. (2020). An epidemiological forecast model and software assessing interventions on COVID-19 epidemic in China [Preprint]. Infectious Diseases (except HIV/AIDS). https://doi.org/10.1101/2020.02.29.20029421
4. Maier, B. F., & Brockmann, D. (2020). Effective containment explains sub-exponential growth in confirmed cases of recent COVID-19 outbreak in Mainland China. ArXiv:2002.07572 [Physics, q-Bio]. http://arxiv.org/abs/2002.07572
5. Allen, L. J. S. (2008). An Introduction to Stochastic Epidemic Models. In F. Brauer, P. van den Driessche, & J. Wu (Eds.), Mathematical Epidemiology (Vol. 1945, pp. 81–130). Springer Berlin Heidelberg. https://doi.org/10.1007/978-3-540-78911-6_3
6. Irene Li, Ziheng Cai, Zhuoran Zhang, Jiacheng Zhu. Lecture 14: Approximate Inference: Markov Chain Monte Carlo. https://sailinglab.github.io/pgm-spring-2019/notes/lecture-14/
7. https://pad.gwdg.de/s/ByQgsSP88
