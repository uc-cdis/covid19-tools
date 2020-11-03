import pandas as pd
from itertools import islice
from collections import deque

confirmed_cases_data_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
raw_data_confirmed = pd.read_csv(confirmed_cases_data_url)
# Group by region
data_day = (
    raw_data_confirmed.groupby(["Country/Region"]).sum().drop(["Lat", "Long"], axis=1)
)
data = data_day.reset_index().melt(id_vars="Country/Region", var_name="date")

# Pivot data to wide & index by date
df = data.pivot(index="date", columns="Country/Region", values="value")
# Set index as DateTimeIndex
datetime_index = pd.DatetimeIndex(df.index)
df.set_index(datetime_index, inplace=True)
df = df.sort_values(by="date")

# get a copy of the dataframe and calculate 5 days moving average
new_case = df[1:].copy()

for col in df.columns:
    the_list = df[col]
    new_case[col] = [y - x for x, y in zip(the_list, the_list[1:])]

# Function to calculate 5 days moving average
def five_moving_average(data):
    it = iter(data)
    d = deque(islice(it, 5))
    divisor = float(5)
    s = sum(d)
    yield s / divisor
    for elem in it:
        s += elem - d.popleft()
        d.append(elem)
        yield s / divisor


# Calculate 5 days moving average table
average_table = new_case[2:-2].copy()

for col in new_case.columns:
    average_table[col] = list(five_moving_average(new_case[col]))

transpose_last_row = average_table.iloc[[-1]].T
top10_countries = transpose_last_row.nlargest(10, transpose_last_row.columns[0])

data = average_table[top10_countries.index]
data.reset_index(level=0, inplace=True)
data["date"] = pd.to_datetime(data["date"])
# strftime won't print the timezone with "%z", adding manually
data["date"] = data["date"].dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
data.to_csv("top10.txt", sep="\t", index=False)
