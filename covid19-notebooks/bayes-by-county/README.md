Insert brief model description and explanations of each plot here.

# Model Summary

Objectives; Assumptions; Scope; Limitations;
Inputs; Outputs; Methods;

# Visualization Descriptions

- `Rt_June_1.png` : desc1

- `Cook/Rt.png` : desc2

- `Cook/cases.png` : Here we plot daily case counts over time, where the gray bars are reported cases and the blue curve traces the estimated number of true (i.e., reported and not-reported) cases. The darker and lighter shaded regions correspond to 50% and 95% posterior credible intervals, respectively. The sharp decrease in estimated true cases in the third week of March results from a similarly sharp decrease in estimated Rt at that same time due to the Illinois statewide lockdown intervention being put in place on March 21st. 

- `Cook/casesForecast.png` : Here we plot reported cases and estimated true cases on a log10 scale and forecast 7 days beyond June 1st, the date of the last observation. We note that the slope of the curve sharply decreases and then stabilizes around the end of March, indicating that the lockdown intervention successfully slowed the spread of COVID-19 in this county.

- `Cook/deaths.png` : Here we plot daily death counts over time, where the gray bars are reported deaths and the blue curve represents modeled death counts. The darker and lighter shaded regions correspond to 50% and 95% posterior credible intervals, respectively. 

- `Cook/deathsForecast.png` : Here we plot observed and modeled daily deaths on a log10 scale and forecast 7 days beyond June 1st, the date of the last observation. We note that the curve bends downward approximately 2-3 weeks after March 21st, the first day of lockdown in Illinois, indicating that the lockdown intervention successfully slowed the spread of COVID-19 in this county.

## Notes to self

1-2 sentences per

I'm writing descriptions specifically for Cook County - we have the same visualizations for
the other counties, but not all of them exhibit exactly the same trends.
For example, in some other counties the curve after lockdown actually has a negative slope (log10)
(Rt < 1), whereas in Cook county the curves after lockdown still have slightly positive slope (log10)
(mean Rt slightly bigger than 1).


date of IL lockdown: Saturday, March 21st, 2020  
source: https://www.chicagotribune.com/coronavirus/ct-coronavirus-illinois-shelter-in-place-lockdown-order-20200320-teedakbfw5gvdgmnaxlel54hau-story.html
