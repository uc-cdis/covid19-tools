# import time
# t0 = time.time()
# import pymc3 as pm
# import arviz as az
# import seaborn as sns

# sns.set_context("talk")
# import theano
# import theano.tensor as tt
# from scipy import stats
# from sklearn.metrics import r2_score
# import pandas as pd
# import numpy as np
# import matplotlib
# import matplotlib.pyplot as plt
# from matplotlib.ticker import FuncFormatter

# plt.style.use("seaborn-whitegrid")
# from IPython.display import display, Markdown
# import warnings

# warnings.simplefilter("ignore")
# # sampler_kwargs = {"chains":4, "cores":4, "return_inferencedata":True}
# sampler_kwargs = {"chains": 4, "cores": 4, "tune": 1000, "draws": 500}
# #%config InlineBackend.figure_format = 'svg'
# theano.config.gcc.cxxflags = "-Wno-c++11-narrowing"


# def _random(self, sigma, mu, size, sample_shape):
#     if size[len(sample_shape)] == sample_shape:
#         axis = len(sample_shape)
#     else:
#         axis = len(size) - 1
#     rv = stats.norm(mu, sigma)
#     data = rv.rvs(size).cumsum(axis=axis)
#     data = np.array(data)
#     if len(data.shape) > 1:
#         for i in range(data.shape[0]):
#             data[i] = data[i] - data[i][0]
#     else:
#         data = data - data[0]
#     return data


# pm.GaussianRandomWalk._random = _random


# def _get_generation_time_interval():
#     """Create a discrete P(Generation Interval)
#     Source: https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7128842/"""
#     import scipy.stats as sps

#     mean_si = 4.7
#     std_si = 2.9
#     mu_si = np.log(mean_si ** 2 / np.sqrt(std_si ** 2 + mean_si ** 2))
#     sigma_si = np.sqrt(np.log(std_si ** 2 / mean_si ** 2 + 1))
#     dist = sps.lognorm(scale=np.exp(mu_si), s=sigma_si)

#     # Discretize the Generation Interval up to 20 days max
#     g_range = np.arange(0, 20)
#     gt = pd.Series(dist.cdf(g_range), index=g_range)
#     gt = gt.diff().fillna(0)
#     gt /= gt.sum()
#     gt = gt.values
#     return gt


# def _get_convolution_ready_gt(len_observed):
#     gt = _get_generation_time_interval()
#     convolution_ready_gt = np.zeros((len_observed - 1, len_observed))
#     for t in range(1, len_observed):
#         begin = np.maximum(0, t - len(gt) + 1)
#         slice_update = gt[1 : t - begin + 1][::-1]
#         convolution_ready_gt[t - 1, begin : begin + len(slice_update)] = slice_update
#     convolution_ready_gt = theano.shared(convolution_ready_gt)
#     return convolution_ready_gt


# jh_data = pd.read_csv(
#     "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
# )
# il_data = jh_data.loc[
#     (jh_data.Province_State == "Illinois") & (jh_data.Admin2 == "Cook")
# ]
# il_data1 = il_data.set_index("Admin2").T
# il_data2 = il_data1.iloc[10:]
# data_sum = pd.DataFrame(il_data2.sum(axis=1), columns=["cases"])
# data_sum = data_sum.loc[lambda x: (x.cases >= 100)]
# daily_data = data_sum.diff().iloc[1:]
# data_sum.insert(0, "days_since_100", range(len(data_sum)))
# daily_data.insert(0, "days_since_100", range(1, len(daily_data) + 1))
# update_date = str(data_sum.index[-1])
# daily_data["date"] = pd.to_datetime(daily_data.index)
# daily_data = daily_data.loc[daily_data["cases"] != 0]


# def average_missing_data(numbers):
#     average_numbers = []

#     for i in range(len(numbers) - 1):
#         if numbers[i] == 0 and numbers[i + 1] != 0:
#             numbers[i] = numbers[i + 1] / 2
#             numbers[i + 1] = numbers[i + 1] / 2
#         if (
#             i + 3 <= len(numbers)
#             and numbers[i] == 0
#             and numbers[i + 1] == 0
#             and numbers[i + 2] != 0
#         ):
#             numbers[i] = numbers[i + 2] / 3
#             numbers[i + 1] = numbers[i + 2] / 3
#             numbers[i + 2] = numbers[i + 2] / 3
#         if (
#             i + 3 < len(numbers)
#             and numbers[i] == 0
#             and numbers[i + 1] == 0
#             and numbers[i + 2] == 0
#             and numbers[i + 3] != 0
#         ):
#             numbers[i] = numbers[i + 3] / 4
#             numbers[i + 1] = numbers[i + 3] / 4
#             numbers[i + 2] = numbers[i + 3] / 4
#             numbers[i + 3] = numbers[i + 3] / 4
#         if (
#             i + 3 == len(numbers)
#             and numbers[i] == 0
#             and numbers[i + 1] == 0
#             and numbers[i + 2] == 0
#         ):
#             numbers[i] = numbers[i - 1]
#             numbers[i + 1] = numbers[i - 1]
#             numbers[i + 2] = numbers[i - 1]
#         if i + 2 == len(numbers) and numbers[i] == 0 and numbers[i + 1] == 0:
#             numbers[i] = numbers[i - 1]
#             numbers[i + 1] = numbers[i - 1]

#     return numbers


# len_observed = len(daily_data)
# convolution_ready_gt = _get_convolution_ready_gt(len_observed)

# with pm.Model() as model_r_t_infection_delay:
#     log_r_t = pm.GaussianRandomWalk("log_r_t", sigma=0.035, shape=len_observed)
#     r_t = pm.Deterministic("r_t", pm.math.exp(log_r_t))

#     # Define a seed population
#     seed = pm.Exponential("seed", 0.01)
#     y0 = tt.zeros(len_observed)
#     y0 = tt.set_subtensor(y0[0], seed)

#     # Apply the recursive algorithm from above
#     outputs, _ = theano.scan(
#         fn=lambda t, gt, y, r_t: tt.set_subtensor(y[t], tt.sum(r_t * y * gt)),
#         sequences=[tt.arange(1, len_observed), convolution_ready_gt],
#         outputs_info=y0,
#         non_sequences=r_t,
#         n_steps=len_observed - 1,
#     )
#     infections = pm.Deterministic("infections", outputs[-1])

#     # Stop infections from taking on unresonably large values that break the NegativeBinomial
#     infections = tt.clip(infections, 0, 13_000_000)

#     # Likelihood
#     #     pm.NegativeBinomial(
#     #         'obs',
#     #         infections,
#     #         alpha = pm.Gamma('alpha', mu=6, sigma=1),
#     #         observed=daily_data.cases.values
#     #     )
#     eps = pm.HalfNormal("eps", 10)  # Error term
#     pm.Lognormal(
#         "obs",
#         pm.math.log(infections),
#         eps,
#         observed=average_missing_data(daily_data.cases.values),
#     )

# with model_r_t_infection_delay:
#     trace_r_t_infection_delay = pm.sample(
#         tune=1000, chains=4, cores=4, target_accept=0.9
#     )


# def conv(a, b, len_observed):
#     """Perform 1D convolution of a and b"""
#     from theano.tensor.signal.conv import conv2d

#     return conv2d(
#         tt.reshape(infections, (1, len_observed)),
#         tt.reshape(p_delay, (1, len(p_delay))),
#         border_mode="full",
#     )[0, :len_observed]


# def get_delay_distribution():
#     # The literature suggests roughly 5 days of incubation before becoming
#     # having symptoms. See:
#     # https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7081172/
#     INCUBATION_DAYS = 5

#     try:
#         p_delay = pd.read_csv(
#             "https://raw.githubusercontent.com/uc-cdis/covid19-tools/feat/model/generative_bayes_model/p_delay.csv",
#             squeeze=True,
#         )  # The delay distribution was calculated from the data in https://github.com/beoutbreakprepared/nCoV2019/tree/master/latest_data
#     except FileNotFoundError:
#         delays = get_delays_from_patient_data()
#         p_delay = delays.value_counts().sort_index()
#         new_range = np.arange(0, p_delay.index.max() + 1)
#         p_delay = p_delay.reindex(new_range, fill_value=0)
#         p_delay /= p_delay.sum()
#         p_delay = (
#             pd.Series(np.zeros(INCUBATION_DAYS))
#             .append(p_delay, ignore_index=True)
#             .rename("p_delay")
#         )
#         p_delay.to_csv("p_delay.csv", index=False)

#     return p_delay


# len_observed = len(daily_data)
# convolution_ready_gt = _get_convolution_ready_gt(len_observed)
# p_delay = get_delay_distribution()
# p_delay.iloc[:5] = 1e-5

# with pm.Model() as model_r_t_onset:
#     log_r_t = pm.GaussianRandomWalk("log_r_t", sigma=0.035, shape=len_observed)
#     r_t = pm.Deterministic("r_t", pm.math.exp(log_r_t))

#     # Define a seed population
#     seed = pm.Exponential("seed", 0.01)
#     y0 = tt.zeros(len_observed)
#     y0 = tt.set_subtensor(y0[0], seed)

#     # Apply the recursive algorithm from above
#     outputs, _ = theano.scan(
#         fn=lambda t, gt, y, r_t: tt.set_subtensor(y[t], tt.sum(r_t * y * gt)),
#         sequences=[tt.arange(1, len_observed), convolution_ready_gt],
#         outputs_info=y0,
#         non_sequences=r_t,
#         n_steps=len_observed - 1,
#     )
#     infections = pm.Deterministic("infections", outputs[-1])

#     test_adjusted_positive = pm.Deterministic(
#         "test_adjusted_positive", conv(infections, p_delay, len_observed)
#     )

#     # Stop infections from taking on unresonably large values that break the NegativeBinomial
#     # infections = tt.clip(infections, 0, 13_000_000)

#     # Likelihood
#     #     pm.NegativeBinomial(
#     #         'obs',
#     #         infections,
#     #         alpha = pm.Gamma('alpha', mu=6, sigma=1),
#     #         observed=daily_data.cases.values
#     #     )
#     eps = pm.HalfNormal("eps", 10)  # Error term
#     pm.Lognormal(
#         "obs",
#         pm.math.log(infections),
#         eps,
#         observed=average_missing_data(daily_data.cases.values),
#     )

#     prior_pred = pm.sample_prior_predictive()

# with model_r_t_onset:
#     trace_r_t_onset = pm.sample(tune=1000, chains=4, cores=4, target_accept=0.9)

# start_date = daily_data.date[0]
# fig, ax = plt.subplots(figsize=(10, 6))
# plt.plot(daily_data.date, trace_r_t_onset["r_t"].T, color="0.5", alpha=0.05)
# # plt.plot(pd.date_range(start=start_date, periods=len(daily_data.cases.values), freq='D'), trace_r_t_infection_delay['r_t'].T, color='r', alpha=0.1)
# ax.set(
#     xlabel="Time",
#     ylabel="$R_e(t)$",
#     Title="Estimated $R_e(t)$ as of {}".format(update_date),
# )
# ax.axhline(1.0, c="k", lw=1, linestyle="--")
# fig.autofmt_xdate()
# fig.savefig("cook_county_rt.svg", dpi=300, bbox_inches="tight")

# with model_r_t_onset:
#     post_pred_r_t_onset = pm.sample_posterior_predictive(trace_r_t_onset, samples=100)
# r2 = az.r2_score(
#     average_missing_data(daily_data.cases.values), post_pred_r_t_onset["obs"]
# )[0]
# start_date = daily_data.date[0]

# y = daily_data["cases"].astype(float)
# T = len(y)
# F = 60
# t = np.arange(T + F)[:, None]

# with pm.Model() as model:
#     c = pm.TruncatedNormal("mean", mu=4, sigma=2, lower=0)
#     mean_func = pm.gp.mean.Constant(c=c)

#     a = pm.HalfNormal("amplitude", sigma=2)
#     l = pm.TruncatedNormal("time-scale", mu=10, sigma=2, lower=0)
#     cov_func = a ** 2 * pm.gp.cov.ExpQuad(input_dim=1, ls=l)

#     gp = pm.gp.Latent(mean_func=mean_func, cov_func=cov_func)

#     f = gp.prior("f", X=t)

#     y_past = pm.Poisson("y_past", mu=tt.exp(f[:T]), observed=y)
#     y_logp = pm.Deterministic("y_logp", y_past.logpt)

# with model:
#     trace = pm.sample(200, tune=100, chains=1, target_accept=0.9, random_seed=42)

# with model:
#     y_future = pm.Poisson("y_future", mu=tt.exp(f[-F:]), shape=F)
#     forecasts = pm.sample_posterior_predictive(trace, vars=[y_future], random_seed=42)

# samples = forecasts["y_future"]

# low = np.zeros(F)
# high = np.zeros(F)
# mean = np.zeros(F)
# median = np.zeros(F)

# for i in range(F):
#     # low[i] = np.min(samples[:,i])
#     low[i] = np.percentile(samples[:, i], 5)
#     high[i] = np.percentile(samples[:, i], 95)
#     # high[i] = np.max(samples[:,i])
#     median[i] = np.percentile(samples[:, i], 50)
#     mean[i] = np.mean(samples[:, i])

# fig, ax = plt.subplots(figsize=(10, 6))
# ax.plot(daily_data.date, post_pred_r_t_onset["obs"].T, color="0.5", alpha=0.05)
# ax.plot(
#     daily_data.date,
#     average_missing_data(daily_data.cases.values),
#     color="r",
#     linewidth=1,
#     markersize=5,
# )


# ax.set(xlabel="Time", ylabel="Daily confirmed cases", yscale="log")
# plt.suptitle(
#     "With Reported Data Since 03/18/2020 and Generative Model Predictions (R-squared = {:.4f})".format(
#         r2
#     ),
#     fontsize=10,
#     y=0.94,
# )
# ax.set_title(
#     "Daily Confirmed Cases in Cook County as of {}".format(update_date),
#     size=14,
#     y=1.1,
# )


# def thousands(x, pos):
#     "The two args are the value and tick position"
#     return "%1.0fK" % (x * 1e-3)


# formatter = FuncFormatter(thousands)
# ax.yaxis.set_major_formatter(formatter)
# fig.autofmt_xdate()
# x_future = np.arange(1, F + 1)
# plt.fill_between(
#     pd.date_range(start=daily_data.date[-1], periods=60, freq="D"), low, high, alpha=0.6
# )
# fig.savefig("cook_county_daily.svg", dpi=300, bbox_inches="tight")
# t1 = time.time()
# totaltime = (t1 - t0) / 3600
# #print("total run time is {:.4f}".format(totaltime))


#####test code#####
import arviz as az
import matplotlib.pyplot as plt
import numpy as np
import pymc3 as pm
import seaborn as sns
from pandas import DataFrame

plt.rcParams.update({"font.size": 14})
seed = 42
rng = np.random.default_rng(seed)


def make_data():
    N = 400
    a, b, cprime = 0.5, 0.6, 0.3
    im, iy, σm, σy = 2.0, 0.0, 0.5, 0.5
    x = rng.normal(loc=0, scale=1, size=N)
    m = im + rng.normal(loc=a * x, scale=σm, size=N)
    y = iy + (cprime * x) + rng.normal(loc=b * m, scale=σy, size=N)
    print(f"True direct effect = {cprime}")
    print(f"True indirect effect = {a*b}")
    print(f"True total effect = {cprime+a*b}")
    return x, m, y


x, m, y = make_data()


def mediation_model(x, m, y):
    with pm.Model() as model:
        x = pm.Data("x", x)
        y = pm.Data("y", y)
        m = pm.Data("m", m)

        # intercept priors
        im = pm.Normal("im", mu=0, sigma=10)
        iy = pm.Normal("iy", mu=0, sigma=10)
        # slope priors
        a = pm.Normal("a", mu=0, sigma=10)
        b = pm.Normal("b", mu=0, sigma=10)
        cprime = pm.Normal("cprime", mu=0, sigma=10)
        # noise priors
        σm = pm.HalfCauchy("σm", 1)
        σy = pm.HalfCauchy("σy", 1)

        # likelihood
        pm.Normal("m_likehood", mu=im + a * x, sigma=σm, observed=m)
        pm.Normal("y_likehood", mu=iy + b * m + cprime * x, sigma=σy, observed=y)

        # calculate quantities of interest
        indirect_effect = pm.Deterministic("indirect effect", a * b)
        total_effect = pm.Deterministic("total effect", a * b + cprime)

    return model


model = mediation_model(x, m, y)
with model:
    result = pm.sample(
        2000,
        tune=2000,
        chains=2,
        target_accept=0.9,
        random_seed=42,
        return_inferencedata=True,
        idata_kwargs={"dims": {"x": ["obs_id"], "m": ["obs_id"], "y": ["obs_id"]}},
    )
fig, ax = plt.subplots(figsize=(10, 10))
f1 = az.plot_trace(result)
fig = plt.gcf()
fig.savefig("cook_county_daily.svg", dpi=300, bbox_inches="tight")


fig, ax = plt.subplots(figsize=(10, 10))
f2 = az.plot_pair(
    result,
    marginals=True,
    point_estimate="median",
    figsize=(12, 12),
    scatter_kwargs={"alpha": 0.05},
    var_names=["a", "b", "cprime", "indirect effect", "total effect"],
)
fig = plt.gcf()
fig.savefig("cook_county_rt.svg", dpi=300, bbox_inches="tight")
