import time
import os
import logging

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

import logging
import sys

logger = logging.getLogger(__name__)


def setup_logger():
    """
    Sets up the logger.
    """
    logger_format = "[%(levelname)s] [%(asctime)s] [%(name)s] - %(message)s"
    logger.setLevel(level=logging.INFO)
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(logger_format, datefmt="%Y%m%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


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


setup_logger()

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
daily_data = daily_data[-180:]
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


len_observed = len(daily_data[window_size // 2 : -window_size // 2 + 1])
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

    # Likelihood
    #     pm.NegativeBinomial(
    #         'obs',
    #         infections,
    #         alpha = pm.Gamma('alpha', mu=6, sigma=1),
    #         observed=daily_data.cases.values
    #     )
    eps = pm.HalfNormal("eps", 10)  # Error term
    pm.Lognormal(
        "obs",
        pm.math.log(infections),
        eps,
        observed=average_missing_data(daily_data.cases.values, window_size),
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


len_observed = len(daily_data[window_size // 2 : -window_size // 2 + 1])
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
        observed=average_missing_data(daily_data.cases.values, window_size),
    )

    prior_pred = pm.sample_prior_predictive()

with model_r_t_onset:
    trace_r_t_onset = pm.sample(tune=500, chains=2, cores=8, target_accept=0.9)
    pm.save_trace(trace=trace_r_t_onset, directory="./trace_r_t_onset", overwrite=True)

start_date = daily_data.date[0]
fig, ax = plt.subplots(figsize=(10, 6))
plt.plot(
    daily_data.date[window_size // 2 : -window_size // 2 + 1],
    trace_r_t_onset["r_t"].T,
    color="0.5",
    alpha=0.05,
)
ax.set(
    xlabel="Time",
    ylabel="$R_e(t)$",
    Title="Estimated $R_e(t)$ as of {}".format(update_date),
)
ax.axhline(1.0, c="k", lw=1, linestyle="--")
fig.autofmt_xdate()
os.makedirs("results/17031/", exist_ok=True)
fig.savefig("results/17031/rt.svg", dpi=30, bbox_inches="tight")

with model_r_t_onset:
    post_pred_r_t_onset = pm.sample_posterior_predictive(trace_r_t_onset, samples=100)
r2 = az.r2_score(
    average_missing_data(daily_data.cases.values, window_size),
    post_pred_r_t_onset["obs"],
)[0]
start_date = daily_data.date[0]

y = average_missing_data(daily_data.cases.values, window_size)
T = len(y)
F = 15
t = np.arange(T + F)[:, None]

with pm.Model() as model:
    c = pm.TruncatedNormal("mean", mu=4, sigma=2, lower=0)
    mean_func = pm.gp.mean.Constant(c=c)

    a = pm.HalfNormal("amplitude", sigma=2)
    l = pm.TruncatedNormal("time-scale", mu=10, sigma=2, lower=0)
    cov_func = a**2 * pm.gp.cov.ExpQuad(input_dim=1, ls=l)

    gp = pm.gp.Latent(mean_func=mean_func, cov_func=cov_func)

    f = gp.prior("f", X=t)

    y_past = pm.Poisson("y_past", mu=tt.exp(f[:T]), observed=y)
    y_logp = pm.Deterministic("y_logp", y_past.logpt)

with model:
    trace = pm.sample(
        500, tune=800, chains=1, target_accept=0.95, random_seed=42, cores=8, init="adapt_diag"
    )
    pm.save_trace(trace=trace, directory="./trace", overwrite=True)

with model:
    y_future = pm.Poisson("y_future", mu=tt.exp(f[-F:]), shape=F)
    forecasts = pm.sample_posterior_predictive(trace, vars=[y_future], random_seed=42)

samples = forecasts["y_future"]

low = np.zeros(F)
high = np.zeros(F)
mean = np.zeros(F)
median = np.zeros(F)

for i in range(F):
    # low[i] = np.min(samples[:,i])
    low[i] = np.percentile(samples[:, i], 10)
    high[i] = np.percentile(samples[:, i], 90)
    # high[i] = np.max(samples[:,i])
    median[i] = np.percentile(samples[:, i], 50)
    mean[i] = np.mean(samples[:, i])

fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(
    daily_data.date[window_size // 2 : -window_size // 2 + 1],
    post_pred_r_t_onset["obs"].T,
    color="0.5",
    alpha=0.05,
)
ax.plot(
    daily_data.date[window_size // 2 : -window_size // 2 + 1],
    average_missing_data(daily_data.cases.values, window_size),
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
    Line2D([0], [0], color="black", label="15-days forecast (median)", linestyle="--"),
    Line2D([0], [0], color="orange", label="15-days forecast (mean)", linestyle="--"),
    Patch(facecolor="silver", edgecolor="silver", label="Posterior predicted cases"),
    Patch(
        facecolor="lightskyblue",
        edgecolor="lightskyblue",
        label="15-days forecast (90% prediciton intervals)",
    ),
]
x_future = np.arange(1, F + 1)
ax.plot(
    pd.date_range(start=daily_data.date[-window_size // 2], periods=15, freq="D"),
    median,
    color="black",
    lw=1,
    linestyle="--",
)
ax.plot(
    pd.date_range(start=daily_data.date[-window_size // 2], periods=15, freq="D"),
    mean,
    color="orange",
    lw=1,
    linestyle="--",
)
plt.fill_between(
    pd.date_range(start=daily_data.date[-window_size // 2], periods=15, freq="D"),
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
logger.info("total run time is {:.4f} hours".format(totaltime))
