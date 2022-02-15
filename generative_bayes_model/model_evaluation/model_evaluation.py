import time
import os
import datetime
from datetime import timedelta
from dateutil.parser import parse

t0 = time.time()
import pymc3 as pm
import arviz as az
import seaborn as sns
import argparse

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
from matplotlib.dates import AutoDateLocator, AutoDateFormatter, date2num
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


def timestamps(datetimes, format="%Y-%m-%d"):
    if isinstance(datetimes, (list, tuple)):
        datetimes = type(datetimes)(dt.strftime(format) for dt in datetimes)
    return datetimes


def maximum_zeros_length(a):
    """Count consecutive 0s and return the biggest number.
    This number pls onw will be served as the window size.
    If there is no missing data, return 3 to calculate 5-days moving average."""
    all_length = []
    for i, g in groupby(a):
        if i == 0:
            all_length.append(len(list(g)))
    if len(all_length) != 0:
        return max(all_length)
    if len(all_length) == 0:
        return 3


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--start_date",
        required=True,
        help="Provides start date for training dataset.",
    )
    parser.add_argument(
        "-e",
        "--end_date",
        required=True,
        help="Provides end date for training dataset.",
    )
    parser.add_argument(
        "-w",
        "--prediction_window",
        required=True,
        type=int,
        help="Provides prediciton window for evaluating the prediciton power.",
    )
    args = parser.parse_args()
    start_date = args.start_date
    end_date = args.end_date
    prediction_window = args.prediction_window
    start_date = timestamps(start_date)
    end_date = timestamps(end_date)

    print("Start date is " + str(start_date))
    print("End date is " + str(end_date))
    print("Prediction window is " + str(prediction_window))

    print("Reading JHU data.")
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
    daily_data_all = data_sum.diff().iloc[1:]
    data_sum.insert(0, "days_since_100", range(len(data_sum)))
    daily_data_all.insert(0, "days_since_100", range(1, len(daily_data_all) + 1))
    daily_data_all["date"] = pd.to_datetime(daily_data_all.index)
    mask = (daily_data_all["date"] >= start_date) & (daily_data_all["date"] <= end_date)
    daily_data = daily_data_all.loc[mask]
    max_zero_length = maximum_zeros_length(daily_data.cases.values)
    window_size = max_zero_length + 2

    y = average_missing_data(daily_data.cases.values, window_size)
    T = len(y)
    F = prediction_window
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
            500, tune=800, chains=1, target_accept=0.95, random_seed=42, cores=8
        )

    with model:
        y_future = pm.Poisson("y_future", mu=tt.exp(f[-F:]), shape=F)
        forecasts = pm.sample_posterior_predictive(
            trace, vars=[y_future], random_seed=42
        )
    start_date_index = (
        daily_data_all[daily_data_all["date"] == start_date].days_since_100[0] - 1
    )
    end_date_index = (
        daily_data_all[daily_data_all["date"] == end_date].days_since_100[0] - 1
    )
    r2 = az.r2_score(
        average_missing_data(daily_data_all.cases.values, window_size)[
            end_date_index : end_date_index + prediction_window
        ],
        forecasts["y_future"],
    )[0]

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
    # ax.plot(
    #     daily_data.date[window_size // 2 : -window_size // 2 + 1],
    #     post_pred_r_t_onset["obs"].T,
    #     color="0.5",
    #     alpha=0.05,
    # )
    #     ax.plot(
    #         daily_data.date[window_size // 2 : -window_size // 2 + 1],
    #         average_missing_data(daily_data.cases.values, window_size),
    #         color="r",
    #         linewidth=1,
    #         markersize=5,
    #     )

    ax.plot(
        pd.date_range(
            start=start_date,
            periods=prediction_window + len(daily_data) - 1,
            freq="D",
        ),
        average_missing_data(daily_data_all.cases.values, window_size)[
            start_date_index : end_date_index + prediction_window
        ],
        color="r",
        linewidth=1,
        markersize=5,
    )

    ax.set(xlabel="Time", ylabel="Daily confirmed cases", yscale="log")
    plt.suptitle(
        "{}-days Forecast using Generative Model (R-squared = {:.4f})".format(
            prediction_window, r2
        ),
        fontsize=10,
        y=0.94,
    )
    ax.set_title(
        "Daily Confirmed Cases in Cook County from {} to {}".format(
            start_date, end_date
        ),
        size=14,
        y=1.1,
    )

    fig.autofmt_xdate()
    legend_elements = [
        Line2D([0], [0], color="red", lw=2, label="Reported cases"),
        Line2D(
            [0],
            [0],
            color="black",
            label="{}-days forecast (median)".format(prediction_window),
            linestyle="--",
        ),
        Line2D(
            [0],
            [0],
            color="orange",
            label="{}-days forecast (mean)".format(prediction_window),
            linestyle="--",
        ),
        Patch(
            facecolor="lightskyblue",
            edgecolor="lightskyblue",
            label="{}-days forecast (90% prediciton intervals)".format(
                prediction_window
            ),
        ),
        Patch(
            facecolor="bisque",
            edgecolor="bisque",
            label="Training data",
        ),
        Patch(
            facecolor="lightgreen",
            edgecolor="lightgreen",
            label="Testing data",
        ),
    ]
    x_future = np.arange(1, F + 1)
    ax.plot(
        pd.date_range(
            start=end_date,
            periods=prediction_window,
            freq="D",
        ),
        median,
        color="black",
        lw=1,
        linestyle="--",
    )
    ax.plot(
        pd.date_range(
            start=end_date,
            periods=prediction_window,
            freq="D",
        ),
        mean,
        color="orange",
        lw=1,
        linestyle="--",
    )
    plt.fill_between(
        pd.date_range(
            start=end_date,
            periods=prediction_window,
            freq="D",
        ),
        low,
        high,
        alpha=0.6,
        color="lightskyblue",
        linewidth=0,
    )
    ax.legend(handles=legend_elements, loc="best", fontsize=9)
    plt.grid(linestyle="--", alpha=0.3)
    ax.axvspan(
        date2num(datetime.datetime.strptime(start_date, "%Y-%m-%d")),
        date2num(datetime.datetime.strptime(end_date, "%Y-%m-%d")),
        label="Training data",
        color="bisque",
        alpha=0.3,
    )
    ax.axvspan(
        date2num(datetime.datetime.strptime(end_date, "%Y-%m-%d")),
        date2num(
            datetime.datetime.strptime(end_date, "%Y-%m-%d")
            + timedelta(days=prediction_window - 1)
        ),
        label="Testing data",
        color="lightgreen",
        alpha=0.1,
    )

    fig.savefig("Daily_cases.svg", dpi=100, bbox_inches="tight")
    t1 = time.time()
    totaltime = (t1 - t0) / 3600
    print("total run time is {:.4f} hours".format(totaltime))


if __name__ == "__main__":
    main()
