import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
import scipy.stats
import statistics
import pymc3 as pm
import theano.tensor as tt
import theano
import datetime
from datetime import date
import time
import matplotlib

# ------------------------------------------------------------------------------ #
# Step 1: load data
# ------------------------------------------------------------------------------ #
confirmed_cases_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
confirmed_cases = pd.read_csv(confirmed_cases_url)
path_to_save = ""

num_days_to_predict = 45

date_data_end = confirmed_cases.loc[
    (confirmed_cases["Province_State"] == "Illinois")
    & (confirmed_cases["Admin2"] == "Cook"),
    :,
].columns[-1]
date_data_begin = confirmed_cases.loc[
    (confirmed_cases["Province_State"] == "Illinois")
    & (confirmed_cases["Admin2"] == "Cook"),
    :,
].columns[-90]
month, day, year = map(int, date_data_end.split("/"))

data_begin = date_data_begin
data_end = date_data_end

cases_obs = np.array(
    confirmed_cases.loc[
        (confirmed_cases["Province_State"] == "Illinois")
        & (confirmed_cases["Admin2"] == "Cook"),
        data_begin:data_end,
    ]
)[0]

cases_all = np.array(
    confirmed_cases.loc[
        (confirmed_cases["Province_State"] == "Illinois")
        & (confirmed_cases["Admin2"] == "Cook"),
        "1/22/20":,
    ]
)[0]

cases_part_all = np.array(
    confirmed_cases.loc[
        (confirmed_cases["Province_State"] == "Illinois")
        & (confirmed_cases["Admin2"] == "Cook"),
        "12/7/21":,
    ]
)[0]

date_data_end = date(year + 2000, month, day)
date_today = date_data_end + datetime.timedelta(days=1)
print(
    "Cases yesterday ({}): {} and day before yesterday: {}".format(
        date_data_end.isoformat(), *cases_obs[:-3:-1]
    )
)
num_days = len(cases_obs)
np.random.seed(0)

# ------------------------------------------------------------------------------ #
# Step 2: calculate averaged daily data
# ------------------------------------------------------------------------------ #
window_size = 7


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


averaged_total = average_missing_data(
    np.diff(cases_all),
    window_size,
)

base = datetime.datetime.strptime("1/22/20", "%m/%d/%y").date() + datetime.timedelta(
    window_size // 2
)
date_list = [base + datetime.timedelta(days=x) for x in range(len(averaged_total))]

index_begin = date_list.index(datetime.datetime.strptime(data_begin, "%m/%d/%y").date())

# ------------------------------------------------------------------------------ #
# Step 3: model setup and training
# ------------------------------------------------------------------------------ #
# -------------------------------------------------------------------------------
# Step 3.1. write Fuction of SIR Model with population
# -------------------------------------------------------------------------------
# λ is spreading rate, μ is recovery rate
# S_begin is susceptible at time 0, I_begin is the infacted at time 0, N is population
# S_t is susceptible at time t, I_t is the infacted at time t
# new_I_t is new infacted at time t


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


# -------------------------------------------------------------------------------
# Step 3.2. Estimating Model parameters
# -------------------------------------------------------------------------------
with pm.Model() as model:
    # -----------------------------------------------------------------------------
    # Step 3.2.1: theta is the set of estimation parameters {λ, μ, σ_obs, I_begin}
    # -----------------------------------------------------------------------------
    # true cases at begin of loaded data but we do not know the real number (sigma is 90%)
    I_begin = pm.Lognormal("I_begin", mu=np.log(cases_obs[0]), sigma=0.9)

    # fraction of people that are newly infected each day (sigma is 50%)
    λ = pm.Lognormal("λ", mu=np.log(0.4), sigma=0.5)

    # fraction of people that recover each day, recovery rate mu (sigma is 20%)
    μ = pm.Lognormal("μ", mu=np.log(1 / 8), sigma=0.2)

    # prior of the error of observed cases
    σ_obs = pm.HalfCauchy("σ_obs", beta=1)

    N_cook = 5.15e6  # cook population in 2020

    # -------------------------------------------------------------------------- #
    # Step 3.2.2 training the model with loaded data
    # -------------------------------------------------------------------------- #
    # Initail state
    S_begin = N_cook - I_begin
    # calculate S_past, I_past, new_I_past usign sir model
    S_past, I_past, new_I_past = SIR_model(
        λ=λ * tt.ones(num_days - 1), μ=μ, S_begin=S_begin, I_begin=I_begin, N=N_cook
    )
    new_infections_obs = np.diff(cases_obs)

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

    # -------------------------------------------------------------------------- #
    # Step 3.2.3 prediction, start with no changes in policy
    # -------------------------------------------------------------------------- #

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

    # -------------------------------------------------------------------------- #
    # Step 3.2.4 social distancing, m reduced by about 50 percent
    # -------------------------------------------------------------------------- #
    # For all following predictions:
    length_transient = 7  # days

    # λ is decreased by 50%
    reduc_factor_mild = 0.5
    days_offset = 0  # start the decrease in spreading rate after this
    # time arangement
    time_arr = np.arange(num_days_to_predict)

    # change in m along time
    λ_correction = tt.clip(
        (time_arr - delay - days_offset + 1) / length_transient, 0, 1
    )
    λ_t_soc_dist = λ * (1 - λ_correction * reduc_factor_mild)
    # Initail state
    S_begin = S_past[-1]
    I_begin = I_past[-1]
    # Forecast in social distancing
    forecast_soc_dist = SIR_model(
        λ=λ_t_soc_dist, μ=μ, S_begin=S_begin, I_begin=I_begin, N=N_cook
    )
    S_soc_dist, I_soc_dist, new_I_soc_dist = forecast_soc_dist
    # saves the variables for later retrieval
    pm.Deterministic("S_soc_dist", S_soc_dist)
    pm.Deterministic("I_soc_dist", I_soc_dist)
    pm.Deterministic("new_I_soc_dist", new_I_soc_dist)

    # -------------------------------------------------------------------------- #
    # Step 3.2.5 isolation, almost no new infections besides baseline after transient phase
    # -------------------------------------------------------------------------- #

    # λ is decreased by 90%
    reduc_factor_strong = 0.9
    days_offset = 0  # start the decrease in spreading rate after this

    # spreading of people who transmit although they are isolated
    time_arr = np.arange(num_days_to_predict)

    # change in λ along time
    λ_correction = tt.clip(
        (time_arr - delay - days_offset + 1) / length_transient, 0, 1
    )
    λ_t_isol = λ * (1 - λ_correction * reduc_factor_strong)
    # Initail state
    S_begin = S_past[-1]
    I_begin = I_past[-1]
    # Forecast in Strong social distancing
    forecast_isol = SIR_model(
        λ=λ_t_isol, μ=μ, S_begin=S_begin, I_begin=I_begin, N=N_cook
    )
    S_isol, I_isol, new_I_isol = forecast_isol
    # saves the variables for later retrieval
    pm.Deterministic("S_isol", S_isol)
    pm.Deterministic("I_isol", I_isol)
    pm.Deterministic("new_I_isol", new_I_isol)

    # -------------------------------------------------------------------------- #
    # Step 3.2.6 isolation 5 days later, almost no new infections besides baseline after transient phase
    # -------------------------------------------------------------------------- #

    # λ is decreased by 90%
    reduc_factor_strong = 0.9
    days_offset = 5  # start the decrease in spreading rate after this

    # spreading of people who transmit although they are isolated
    time_arr = np.arange(num_days_to_predict)

    # change in λ along time
    λ_correction = tt.clip(
        (time_arr - delay - days_offset + 1) / length_transient, 0, 1
    )
    λ_t_isol_later = λ * (1 - λ_correction * reduc_factor_strong)
    # Initail state
    S_begin = S_past[-1]
    I_S_beginbegin = I_past[-1]
    # Forecast in strong social distancing at 5 days later
    forecast_isol_later = SIR_model(
        λ=λ_t_isol_later, μ=μ, S_begin=S_begin, I_begin=I_begin, N=N_cook
    )
    S_isol_later, I_isol_later, new_I_isol_later = forecast_isol_later
    # saves the variables for later retrieval
    pm.Deterministic("S_isol_later", S_isol_later)
    pm.Deterministic("I_isol_later", I_isol_later)
    pm.Deterministic("new_I_isol_later", new_I_isol_later)

    # -------------------------------------------------------------------------- #
    # Step 3.2.7 isolation 7 days earlyier, almost no new infections besides baseline after transient phase
    # -------------------------------------------------------------------------- #

    # λ is decreased by 90%
    reduc_factor_strong = 0.9
    days_offset = -5  # start the decrease in spreading rate after this

    # spreading of people who transmit although they are isolated
    time_arr = np.arange(num_days_to_predict)

    # change in λ along time
    λ_correction = tt.clip(
        (time_arr - delay - days_offset + 1) / length_transient, 0, 1
    )
    λ_t_isol_earlyier = λ * (1 - λ_correction * reduc_factor_strong)
    # Initail state
    S_begin = S_past[-1]
    I_S_begin = I_past[-1]
    # Forecast in storng social distancing at 7 days earlyier
    forecast_isol_earlyier = SIR_model(
        λ=λ_t_isol_earlyier, μ=μ, S_begin=S_begin, I_begin=I_begin, N=N_cook
    )
    S_isol_earlyier, I_isol_earlyier, new_I_isol_earlyier = forecast_isol_earlyier
    # saves the variables for later retrieval
    pm.Deterministic("S_isol_earlyier", S_isol_earlyier)
    pm.Deterministic("I_isol_earlyier", I_isol_earlyier)
    pm.Deterministic("new_I_isol_earlyier", new_I_isol_earlyier)

    # -------------------------------------------------------------------------- #
    # Step 3.2.8 run model, pm trains and predicts when calling this
    # -------------------------------------------------------------------------- #
    time_beg = time.time()
    trace = pm.sample(draws=500, tune=800, chains=2)
    print("Model run in {:.2f} s".format(time.time() - time_beg))

# -------------------------------------------------------------------------------
# Step 4 Plot data of new infections
# -------------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 7))
ax.plot(
    pd.date_range(
        start=datetime.datetime.strptime(data_begin, "%m/%d/%y").date()
        + datetime.timedelta(1),
        end=datetime.datetime.strptime(data_end, "%m/%d/%y").date(),
        freq="D",
    ),
    averaged_total[index_begin - window_size // 2 :],
    color="red",
    label="Reported daily confirmed cases (7-day moving average)",
    linewidth=1,
)
percentiles = (
    np.percentile(trace.new_I_past, q=5.0, axis=0),
    np.percentile(trace.new_I_past, q=95.0, axis=0),
)
ax.fill_between(
    pd.date_range(
        start=datetime.datetime.strptime(data_begin, "%m/%d/%y").date()
        + datetime.timedelta(days=1),
        end=datetime.datetime.strptime(data_end, "%m/%d/%y").date(),
        freq="D",
    ),
    percentiles[0],
    percentiles[1],
    alpha=0.2,
    color="red",
    linewidth=0,
)
legends_lang = [
    "Uncontrolled spreading\n(median and 95% prediction intervals)",
    "Mild social distancing\n(median and 95% prediction intervals)",
    "Strong social distancing\n(median and 95% prediction intervals)",
    "Strong social distancing since {}\n(median and 95% prediction intervals)".format(
        datetime.datetime.strptime(data_end, "%m/%d/%y").date()
        - datetime.timedelta(days=5)
    ),
]

obs_cases_labels = [
    "new_I_no_change",
    "new_I_isol",
    "new_I_isol_later",
    "new_I_isol_earlyier",
]
colors = ["tab:purple", "tab:orange", "tab:green", "black"]
for label, color, legend in zip(obs_cases_labels, colors, legends_lang):
    percentiles_future = (
        np.percentile(trace[label], q=5.0, axis=0),
        np.percentile(trace[label], q=95.0, axis=0),
    )
    ax.fill_between(
        pd.date_range(
            start=datetime.datetime.strptime(data_end, "%m/%d/%y").date(),
            periods=num_days_to_predict,
            freq="D",
        ),
        percentiles_future[0],
        percentiles_future[1],
        alpha=0.2,
        color=color,
        linewidth=0,
    )
    ax.plot(
        pd.date_range(
            start=datetime.datetime.strptime(data_end, "%m/%d/%y").date(),
            periods=num_days_to_predict,
            freq="D",
        ),
        np.median(trace[label], axis=0),
        label=legend,
        linestyle="--",
        color=color,
        linewidth=1,
    )

ax.legend(loc="best", fontsize=10)
ax.set_title(
    "Daily Confirmed Cases in Cook County as of {} to {}".format(data_begin, data_end),
    size=15,
    y=1.08,
)
ax.set(
    xlabel="Time",
    ylabel="Daily confirmed cases",
)
plt.suptitle(
    "With Reported Data From Past 3 Months and SIR Model Predictions",
    fontsize=12,
    y=0.92,
)
plt.yscale("log")
fig.autofmt_xdate()
fig.savefig(
    "IL_tab_charts_cumulative_logistic_last180days.svg", dpi=60, bbox_inches="tight"
)

# -------------------------------------------------------------------------------
# Step 5 Plot Total confirm case
# -------------------------------------------------------------------------------
# Labels
legends_lang = {
    "english": [
        "Reported confirmed cases",
        [
            "Uncontrolled spreading\n(median and 90% prediction intervals)",
            "Mild social distancing\n(median and 90% prediction intervals)",
            "Strong social distancing\n(median and 90% prediction intervals)",
            "Strong social distancing since {}\n(median and 90% prediction intervals)".format(
                datetime.datetime.strptime(data_end, "%m/%d/%y").date()
                - datetime.timedelta(days=5)
            ),
        ],
        "Time",
        "Cumulative confirmed cases",
    ],
}
obs_cases_labels = [
    "new_I_no_change",
    "new_I_soc_dist",
    "new_I_isol",
    "new_I_isol_later",
    "new_I_isol_earlyier",
]
# formate date
date_today_formatted = "{}/{}/{}".format(
    date_today.month, date_today.day, str(date_today.year)[2:4]
)
# past and present plot data
cases_obs_to_plot = np.array(
    confirmed_cases.loc[
        (confirmed_cases["Province_State"] == "Illinois")
        & (confirmed_cases["Admin2"] == "Cook"),
        data_begin:data_end,
    ]
)[0]

# Observe case future
def return_obs_cases_future(trace):
    obs_cases_future = dict()
    for label in obs_cases_labels:
        obs_cases_future[label] = (
            np.cumsum(trace[label], axis=1)
            + np.sum(trace.new_I_past, axis=1)[:, None]
            + trace.I_begin[:, None]
        )
        obs_cases_future[label] = obs_cases_future[label].T
    return obs_cases_future


obs_cases_labels_local = obs_cases_labels[:]
obs_cases_labels_local.pop(3)

# plotting data of total confirmed cases
for lang, legends_list in legends_lang.items():
    fig, ax = plt.subplots(figsize=(10, 7))
    # bottom left
    colors = ["tab:purple", "tab:orange", "tab:green", "black"]
    dict_obsc_cases = return_obs_cases_future(trace)
    # time arangement for plot
    time = np.arange(-len(cases_obs_to_plot) + 1, 1)
    ax.plot(
        pd.date_range(
            start=datetime.datetime.strptime(data_begin, "%m/%d/%y").date(),
            end=datetime.datetime.strptime(data_end, "%m/%d/%y").date(),
            freq="D",
        ),
        cases_obs_to_plot,
        label=legends_list[0],
        linewidth=1,
        color="red",
        zorder=0,
    )

    for label, color, legend in zip(obs_cases_labels_local, colors, legends_list[1]):
        time = np.arange(0, num_days_to_predict)
        cases = dict_obsc_cases[label]
        cases = cases + cases_part_all[0]
        # find median
        median = np.median(cases, axis=-1)
        percentiles = (
            np.percentile(cases, q=10, axis=-1),
            np.percentile(cases, q=90, axis=-1),
        )
        ax.plot(
            pd.date_range(
                start=datetime.datetime.strptime(data_end, "%m/%d/%y").date(),
                periods=num_days_to_predict,
                freq="D",
            ),
            median,
            color,
            linewidth=1,
            label=legend,
            linestyle="--",
        )
        ax.fill_between(
            pd.date_range(
                start=datetime.datetime.strptime(data_end, "%m/%d/%y").date(),
                periods=num_days_to_predict,
                freq="D",
            ),
            percentiles[0],
            percentiles[1],
            alpha=0.2,
            color=color,
            linewidth=0,
        )

    ax.set_yscale("linear")
    ax.legend(loc="lower right", fontsize=10)
    ax.set_title(
        "Cumulative Confirmed Cases in Cook County as of {} to {}".format(
            data_begin, data_end
        ),
        size=15,
        y=1.08,
    )
    ax.set(
        xlabel="Time",
        ylabel="Cumulative confirmed cases",
    )
    # function format
    func_format = lambda num, _: "${:,.0f}$".format(num).replace(",", "\,")
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(func_format))

    plt.suptitle(
        "With Reported Data From Past 2 Months and SIR Model Predictions",
        fontsize=12,
        y=0.92,
    )
    plt.yscale("log")
    # fig.tight_layout()
    fig.autofmt_xdate()
    fig.savefig(
        "IL_tab_charts_cumulative_logistic_last360.svg", dpi=60, bbox_inches="tight"
    )

print(
    "effective m: {:.3f} +- {:.3f}".format(
        1 + np.median(trace.λ - trace.μ), np.std(trace.λ - trace.μ)
    )
)
