import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import gen3
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
import itertools
from itertools import islice
from collections import deque
import plotly.graph_objects as go


confirmed_cases_data_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
raw_data_confirmed = pd.read_csv(confirmed_cases_data_url)
# Group by region
data_day = (
    raw_data_confirmed.groupby(["Country/Region"]).sum().drop(["Lat", "Long"], axis=1)
)
df = data_day.transpose()
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


# Start plotting
fig = go.Figure()
poi = [
    "US",
    "Italy",
    "France",
    "Spain",
    "United Kingdom",
    "China",
    "Iran",
    "Netherlands",
    "Germany",
    "Belgium",
]
data = average_table[poi]

for i in data.columns:
    fig.add_trace(
        go.Scatter(x=data.index, y=data[i], mode="lines", name=i, connectgaps=True,)
    )

fig.update_layout(
    xaxis=dict(
        showline=True,
        showgrid=False,
        showticklabels=True,
        linecolor="rgb(204, 204, 204)",
        linewidth=2,
        ticks="outside",
        tickfont=dict(family="Arial", size=12, color="rgb(82, 82, 82)",),
    ),
    yaxis=dict(showgrid=False, zeroline=False, showline=False, showticklabels=False,),
    autosize=False,
    margin=dict(autoexpand=False, l=100, r=20, t=110,),
    showlegend=False,
    plot_bgcolor="white",
)

annotations = []

# Title
annotations.append(
    dict(
        xref="paper",
        yref="paper",
        x=0.5,
        y=1.05,
        xanchor="center",
        yanchor="bottom",
        text="Daily Confirmed Cases (5-day Moving Average)",
        font=dict(family="Arial", size=25, color="rgb(37,37,37)"),
        showarrow=False,
    )
)
# subtitle
# annotations.append(dict(xref='paper', yref='paper', x=0.5, y=0.95,
#   xanchor='center', yanchor='bottom',
#    text='Outbreak evolution for the current 10 most affected countries',
#    font=dict(family='Arial',
#              size=18,
#              color='grey'),
#    showarrow=False))
# Source
annotations.append(
    dict(
        xref="paper",
        yref="paper",
        x=0.5,
        y=-0.15,
        xanchor="center",
        yanchor="top",
        text="Data source: COVID-19 Data Repository by Johns Hopkins CSSE",
        font=dict(family="Arial", size=12, color="rgb(150,150,150)"),
        showarrow=False,
    )
)

fig.update_layout(annotations=annotations)

fig.update_layout(
    autosize=False,
    width=1000,
    height=600,
    margin=dict(l=50, r=50, b=150, t=50, pad=4),
    yaxis=dict(
        title="Confirmed New Cases",
        titlefont=dict(family="Arial, sans-serif", size=15, color="black"),
        showticklabels=True,
    ),
)

fig.update_layout(legend_orientation="h", showlegend=True)
fig.update_traces(mode="lines+markers")
fig.update_layout(legend=dict(y=-0.23, traceorder="reversed", font_size=16))
fig.update_layout(hovermode="x")
fig.update_layout(legend=dict(xanchor="center", x=0.5, y=-0.22))
fig.write_html("5_days_moving_averages_JHU.html")


# Generating plot using Illinois department of public health data
api = "https://chicagoland.pandemicresponsecommons.org/"
creds = "credentials.json"
auth = Gen3Auth(api, creds)
sub = Gen3Submission(api, auth)

program = "open"
project = "IDPH"
summary_report = sub.export_node(
    program, project, "summary_report", "tsv", "summary_report_idph.tsv"
)

idph = pd.read_csv("./summary_report_idph.tsv", sep="\t")
idph1 = idph[["date", "confirmed", "deaths", "testing"]]
data_day = idph1.groupby(["date"]).sum()

# Start plotting
fig = go.Figure()

for i in data_day.columns:
    fig.add_trace(
        go.Scatter(
            x=data_day.index, y=data_day[i], mode="lines", name=i, connectgaps=True,
        )
    )

fig.update_layout(
    xaxis=dict(
        showline=True,
        showgrid=False,
        showticklabels=True,
        linecolor="rgb(204, 204, 204)",
        linewidth=2,
        ticks="outside",
        tickfont=dict(family="Arial", size=12, color="rgb(82, 82, 82)",),
    ),
    yaxis=dict(showgrid=False, zeroline=False, showline=False, showticklabels=False,),
    autosize=False,
    margin=dict(autoexpand=False, l=100, r=20, t=110,),
    showlegend=False,
    plot_bgcolor="white",
)

annotations = []

# Title
annotations.append(
    dict(
        xref="paper",
        yref="paper",
        x=0.5,
        y=1.05,
        xanchor="center",
        yanchor="bottom",
        text="Breakdown of Tests, Cases and Deaths in Illinois Over Time",
        font=dict(family="Arial", size=25, color="rgb(37,37,37)"),
        showarrow=False,
    )
)
# subtitle
# annotations.append(dict(xref='paper', yref='paper', x=0.5, y=0.95,
#   xanchor='center', yanchor='bottom',
#    text='Outbreak evolution for the current 10 most affected countries',
#    font=dict(family='Arial',
#              size=18,
#              color='grey'),
#    showarrow=False))
# Source
annotations.append(
    dict(
        xref="paper",
        yref="paper",
        x=0.5,
        y=-0.15,
        xanchor="center",
        yanchor="top",
        text="Data source: IDPH (Illinois Department of Public Health)",
        font=dict(family="Arial", size=12, color="rgb(150,150,150)"),
        showarrow=False,
    )
)

fig.update_layout(annotations=annotations)

fig.update_layout(
    autosize=False,
    width=1000,
    height=600,
    margin=dict(l=50, r=50, b=150, t=50, pad=4),
    yaxis=dict(
        title="Tests/Cases/Deaths",
        titlefont=dict(family="Arial, sans-serif", size=15, color="black"),
        showticklabels=True,
    ),
)

fig.update_layout(legend_orientation="h", showlegend=True)
fig.update_traces(mode="lines+markers")
fig.update_layout(legend=dict(y=-0.23, traceorder="reversed", font_size=16))
fig.update_layout(hovermode="x")
fig.update_layout(legend=dict(xanchor="center", x=0.5, y=-0.22))
fig.write_html("cases_deaths_IL_IDPH.html")
