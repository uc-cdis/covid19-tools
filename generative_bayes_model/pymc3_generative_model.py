import time
import os

t0 = time.time()
import pymc3 as pm
import arviz as az
import seaborn as sns

sns.set_context("talk")
import theano
import theano.tensor as tt
from scipy import stats
from sklearn.metrics import r2_score
from itertools import groupby
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter

plt.style.use("seaborn-whitegrid")
from IPython.display import display, Markdown
import warnings

warnings.simplefilter("ignore")
# sampler_kwargs = {"chains":4, "cores":4, "return_inferencedata":True}
#%config InlineBackend.figure_format = 'svg'
theano.config.gcc.cxxflags = "-Wno-c++11-narrowing"


def _random(self, sigma, mu, size, sample_shape):
    if size[len(sample_shape)] == sample_shape:
        axis = len(sample_shape)
    else:
        axis = len(size) - 1
    rv = stats.norm(mu, sigma)
    data = rv.rvs(size).cumsum(axis=axis)
    data = np.array(data)
    if len(data.shape) > 1:
        for i in range(data.shape[0]):
            data[i] = data[i] - data[i][0]
    else:
        data = data - data[0]
    return data


pm.GaussianRandomWalk._random = _random


def _get_generation_time_interval():
    """Create a discrete P (Generation Time Interval)
    Source: https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7128842/"""
    import scipy.stats as sps

    mean_si = 4.7
    std_si = 2.9
    mu_si = np.log(mean_si**2 / np.sqrt(std_si**2 + mean_si**2))
    sigma_si = np.sqrt(np.log(std_si**2 / mean_si**2 + 1))
    dist = sps.lognorm(scale=np.exp(mu_si), s=sigma_si)

    # Discretize the Generation Interval up to 20 days max
    g_range = np.arange(0, 20)
    gt = pd.Series(dist.cdf(g_range), index=g_range)
    gt = gt.diff().fillna(0)
    gt /= gt.sum()
    gt = gt.values
    return gt


def _get_convolution_ready_gt(len_observed):
    gt = _get_generation_time_interval()
    convolution_ready_gt = np.zeros((len_observed - 1, len_observed))
    for t in range(1, len_observed):
        begin = np.maximum(0, t - len(gt) + 1)
        slice_update = gt[1 : t - begin + 1][::-1]
        convolution_ready_gt[t - 1, begin : begin + len(slice_update)] = slice_update
    convolution_ready_gt = theano.shared(convolution_ready_gt)
    return convolution_ready_gt


def maximum_zeros_length(a):
    """Count consecutive 0s and return the biggest number.
    This number pls onw will be served as the window size."""
    all_length = []
    for i, g in groupby(a):
        if i == 0:
            all_length.append(len(list(g)))
    return max(all_length)


jh_data = pd.read_csv(
    "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
)
il_data = jh_data.loc[
    (jh_data.Province_State == "Illinois") & (jh_data.Admin2 == "Cook")
]
il_data1 = il_data.set_index("Admin2").T
il_data2 = il_data1.iloc[10:]
data_sum = pd.DataFrame(il_data2.sum(axis=1), columns=["cases"])
data_sum = data_sum.loc[lambda x: (x.cases >= 100)]
daily_data = data_sum.diff().iloc[1:]
data_sum.insert(0, "days_since_100", range(len(data_sum)))
daily_data.insert(0, "days_since_100", range(1, len(daily_data) + 1))
update_date = str(data_sum.index[-1])
daily_data["date"] = pd.to_datetime(daily_data.index)
# Take past 9 month data as sampling data
daily_data_all = daily_data[-180:]
daily_data = daily_data[-90:]
max_zero_length = maximum_zeros_length(daily_data.cases.values)
window_size = max_zero_length + 2


def average_missing_data(numbers, window_size):
    """JHU doesn't update the data during holidays and weekends.
    And all the cases during the holidays and weekends will add up onto the next business day.
    This function is to get the average case number for those days."""

    i = 0
    moving_averages = []
    while i < len(numbers) - window_size + 1:
        this_window = numbers[i : i + window_size]
        window_average = sum(this_window) / window_size
        moving_averages.append(window_average)
        i += 1

    return moving_averages


len_observed = len(daily_data_all[window_size // 2 : -window_size // 2 + 1])
convolution_ready_gt = _get_convolution_ready_gt(len_observed)

with pm.Model() as model_r_t_infection_delay:
    log_r_t = pm.GaussianRandomWalk("log_r_t", sigma=0.035, shape=len_observed)
    r_t = pm.Deterministic("r_t", pm.math.exp(log_r_t))

    # Define a seed population
    seed = pm.Exponential("seed", 0.01)
    y0 = tt.zeros(len_observed)
    y0 = tt.set_subtensor(y0[0], seed)

    # Apply the recursive algorithm from above
    outputs, _ = theano.scan(
        fn=lambda t, gt, y, r_t: tt.set_subtensor(y[t], tt.sum(r_t * y * gt)),
        sequences=[tt.arange(1, len_observed), convolution_ready_gt],
        outputs_info=y0,
        non_sequences=r_t,
        n_steps=len_observed - 1,
    )
    infections = pm.Deterministic("infections", outputs[-1])

    # Stop infections from taking on unresonably large values that break the NegativeBinomial
    infections = tt.clip(infections, 0, 13_000_000)

    eps = pm.HalfNormal("eps", 10)  # Error term
    pm.Lognormal(
        "obs",
        pm.math.log(infections),
        eps,
        observed=average_missing_data(daily_data_all.cases.values, window_size),
    )

with model_r_t_infection_delay:
    trace_r_t_infection_delay = pm.sample(
        tune=500, chains=2, cores=8, target_accept=0.9
    )
    pm.save_trace(
        trace=trace_r_t_infection_delay,
        directory="./trace_r_t_infection_delay",
        overwrite=True,
    )


def conv(a, b, len_observed):
    """Perform 1D convolution of a and b"""
    from theano.tensor.signal.conv import conv2d

    return conv2d(
        tt.reshape(infections, (1, len_observed)),
        tt.reshape(p_delay, (1, len(p_delay))),
        border_mode="full",
    )[0, :len_observed]


def get_delay_distribution():
    """Returns the delay distribution between symptom onset
    and confirmed case."""
    # The literature suggests roughly 5 days of incubation before becoming
    # having symptoms. See:
    # https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7081172/
    INCUBATION_DAYS = 5

    try:
        p_delay = pd.read_csv(
            "https://raw.githubusercontent.com/uc-cdis/covid19-tools/master/generative_bayes_model/p_delay.csv",
            squeeze=True,
        )  # The delay distribution was calculated from the data in https://github.com/beoutbreakprepared/nCoV2019/tree/master/latest_data
    except FileNotFoundError:
        delays = get_delays_from_patient_data()
        p_delay = delays.value_counts().sort_index()
        new_range = np.arange(0, p_delay.index.max() + 1)
        p_delay = p_delay.reindex(new_range, fill_value=0)
        p_delay /= p_delay.sum()
        p_delay = (
            pd.Series(np.zeros(INCUBATION_DAYS))
            .append(p_delay, ignore_index=True)
            .rename("p_delay")
        )
        p_delay.to_csv("p_delay.csv", index=False)

    return p_delay


len_observed = len(daily_data_all[window_size // 2 : -window_size // 2 + 1])
convolution_ready_gt = _get_convolution_ready_gt(len_observed)
p_delay = get_delay_distribution()
p_delay.iloc[:5] = 1e-5

with pm.Model() as model_r_t_onset:
    log_r_t = pm.GaussianRandomWalk("log_r_t", sigma=0.035, shape=len_observed)
    r_t = pm.Deterministic("r_t", pm.math.exp(log_r_t))

    # Define a seed population
    seed = pm.Exponential("seed", 0.01)
    y0 = tt.zeros(len_observed)
    y0 = tt.set_subtensor(y0[0], seed)

    # Apply the recursive algorithm from above
    outputs, _ = theano.scan(
        fn=lambda t, gt, y, r_t: tt.set_subtensor(y[t], tt.sum(r_t * y * gt)),
        sequences=[tt.arange(1, len_observed), convolution_ready_gt],
        outputs_info=y0,
        non_sequences=r_t,
        n_steps=len_observed - 1,
    )
    infections = pm.Deterministic("infections", outputs[-1])

    test_adjusted_positive = pm.Deterministic(
        "test_adjusted_positive", conv(infections, p_delay, len_observed)
    )

    eps = pm.HalfNormal("eps", 10)  # Error term
    pm.Lognormal(
        "obs",
        pm.math.log(infections),
        eps,
        observed=average_missing_data(daily_data_all.cases.values, window_size),
    )

    prior_pred = pm.sample_prior_predictive()

with model_r_t_onset:
    trace_r_t_onset = pm.sample(tune=500, chains=2, cores=8, target_accept=0.9)
    pm.save_trace(trace=trace_r_t_onset, directory="./trace_r_t_onset", overwrite=True)

start_date = daily_data_all.date[0]
fig, ax = plt.subplots(figsize=(10, 6))
plt.plot(
    daily_data_all.date[window_size // 2 : -window_size // 2 + 1],
    trace_r_t_onset["r_t"].T,
    color="0.5",
    alpha=0.05,
)
# plt.plot(pd.date_range(start=start_date, periods=len(daily_data.cases.values), freq='D'), trace_r_t_infection_delay['r_t'].T, color='r', alpha=0.1)
ax.set(
    xlabel="Time",
    ylabel="$R_e(t)$",
)
ax.set_title(
    "Estimated $R_e(t)$ as of {}".format(update_date),
    size=15,
    y=1.08,
)
ax.axhline(1.0, c="k", lw=1, linestyle="--")
fig.autofmt_xdate()
os.makedirs("results/17031/", exist_ok=True)
fig.savefig("results/17031/rt.svg", dpi=30, bbox_inches="tight")

with model_r_t_onset:
    post_pred_r_t_onset = pm.sample_posterior_predictive(trace_r_t_onset, samples=100)
r2 = az.r2_score(
    average_missing_data(daily_data_all.cases.values, window_size),
    post_pred_r_t_onset["obs"],
)[0]
start_date = daily_data_all.date[0]


num_days = len(daily_data) + 1
num_days_to_predict = 45
np.random.seed(0)


def SIR_model(λ, μ, S_begin, I_begin, N):
    new_I_0 = tt.zeros_like(I_begin)

    def next_day(λ, S_t, I_t, _):
        new_I_t = (λ / N) * I_t * S_t
        S_t = S_t - new_I_t
        I_t = I_t + new_I_t - μ * I_t
        return S_t, I_t, new_I_t

    outputs, _ = theano.scan(
        fn=next_day, sequences=[λ], outputs_info=[S_begin, I_begin, new_I_0]
    )
    S_all, I_all, new_I_all = outputs
    return S_all, I_all, new_I_all


with pm.Model() as model:

    # true cases at begin of loaded data but we do not know the real number (sigma is 90%)
    I_begin = pm.Lognormal("I_begin", mu=np.log(data_sum.cases[-90]), sigma=0.9)

    # fraction of people that are newly infected each day (sigma is 50%)
    λ = pm.Lognormal("λ", mu=np.log(0.4), sigma=0.5)

    # fraction of people that recover each day, recovery rate mu (sigma is 20%)
    μ = pm.Lognormal("μ", mu=np.log(1 / 8), sigma=0.2)

    # prior of the error of observed cases
    σ_obs = pm.HalfCauchy("σ_obs", beta=1)

    N_cook = 5.15e6  # cook population in 2020

    # Initail state
    S_begin = N_cook - I_begin
    # calculate S_past, I_past, new_I_past usign sir model
    S_past, I_past, new_I_past = SIR_model(
        λ=λ * tt.ones(num_days - 1), μ=μ, S_begin=S_begin, I_begin=I_begin, N=N_cook
    )
    new_infections_obs = daily_data.cases.values

    # Approximates Poisson
    # calculate the likelihood of the model:
    # observed cases are distributed following studentT around the model
    # Recursively update the parameters using MCMC
    # syntax of pymc3=> https://docs.pymc.io/api/distributions/continuous.html
    pm.StudentT(  # Student’s T log-likelihood eg: pm.StudentT(nu[, mu, lam, sigma, sd])
        "obs",
        nu=4,
        mu=new_I_past,
        sigma=new_I_past**0.5 * σ_obs,
        observed=new_infections_obs,
    )
    # saves the variables for later retrieval
    S_past = pm.Deterministic("S_past", S_past)
    I_past = pm.Deterministic("I_past", I_past)
    new_I_past = pm.Deterministic("new_I_past", new_I_past)

    # delay in days between contracting the disease and being recorded
    # assuming a median delay of 8 days.
    delay = pm.Lognormal("delay", mu=np.log(8), sigma=0.1)  #
    # Initail state
    S_begin = S_past[-1]
    I_begin = I_past[-1]
    # Forecast in no change condition
    forecast_no_change = SIR_model(
        λ=λ * tt.ones(num_days_to_predict),
        μ=μ,
        S_begin=S_begin,
        I_begin=I_begin,
        N=N_cook,
    )
    S_no_change, I_no_change, new_I_no_change = forecast_no_change

    # saves the variables for later retrieval
    pm.Deterministic("S_no_change", S_no_change)
    pm.Deterministic("I_no_change", I_no_change)
    pm.Deterministic("new_I_no_change", new_I_no_change)

    trace = pm.sample(draws=100, tune=200, chains=1)
    pm.save_trace(trace=trace, directory="./trace", overwrite=True)

low = np.percentile(trace["new_I_no_change"], q=10.0, axis=0)
high = np.percentile(trace["new_I_no_change"], q=90.0, axis=0)
median = np.percentile(trace["new_I_no_change"], q=50.0, axis=0)

fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(
    daily_data_all.date[window_size // 2 : -window_size // 2 + 1],
    post_pred_r_t_onset["obs"].T,
    color="0.5",
    alpha=0.05,
)
ax.plot(
    daily_data_all.date[window_size // 2 : -window_size // 2 + 1],
    average_missing_data(daily_data_all.cases.values, window_size),
    color="r",
    linewidth=1,
    markersize=5,
)

ax.set(xlabel="Time", ylabel="Daily confirmed cases", yscale="log")
plt.suptitle(
    "With Reported Data From Past 6 Months and Generative Model Predictions (R-squared = {:.4f})".format(
        r2
    ),
    fontsize=12,
    y=0.94,
)
ax.set_title(
    "Daily Confirmed Cases in Cook County as of {}".format(update_date),
    size=15,
    y=1.1,
)


fig.autofmt_xdate()
legend_elements = [
    Line2D([0], [0], color="red", lw=2, label="Reported cases"),
    Line2D([0], [0], color="black", label="45-days forecast (median)", linestyle="--"),
    Patch(facecolor="silver", edgecolor="silver", label="Posterior predicted cases"),
    Patch(
        facecolor="lightskyblue",
        edgecolor="lightskyblue",
        label="45-days forecast (90% prediciton intervals)",
    ),
]
x_future = np.arange(1, num_days_to_predict + 1)
ax.plot(
    pd.date_range(
        start=daily_data.date[-window_size // 2], periods=num_days_to_predict, freq="D"
    ),
    median,
    color="black",
    lw=1,
    linestyle="--",
)

plt.fill_between(
    pd.date_range(
        start=daily_data.date[-window_size // 2], periods=num_days_to_predict, freq="D"
    ),
    low,
    high,
    alpha=0.6,
    color="lightskyblue",
    linewidth=0,
)
ax.legend(handles=legend_elements, loc="best", fontsize=12)
ax.grid(False)
fig.savefig("results/17031/cases.svg", dpi=60, bbox_inches="tight")
t1 = time.time()
totaltime = (t1 - t0) / 3600
print("total run time is {:.4f} hours".format(totaltime))
