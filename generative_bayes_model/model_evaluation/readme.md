# Evaluate the Model's Prediction Performance

#### Fan Wang
#### 12/08/2021
-------------------
## Objectives

We use the prediciton model to forecast the future trend of the daily confirmed cases. It is important to establish a strong baseline of performance on the forecasting values. To this end, we need to relate the predicted values to the actual reported values. This python scripts implement the comparison between the predicted values and the true values. The users will have the flexibilities to choose the `start_date`, `end_date` and `prediction_window`, therefore the model performance could be evaluated with a suite of training and testing window sizes.

## Usage of `model_evaluation.py`

```
usage: model_evaluation.py [-h] -s START_DATE -e END_DATE -w PREDICTION_WINDOW

optional arguments:
  -h, --help            show this help message and exit

  -s START_DATE, --start_date START_DATE
                        Provides start date for training dataset.

  -e END_DATE, --end_date END_DATE
                        Provides end date for training dataset.

  -w PREDICTION_WINDOW, --prediction_window PREDICTION_WINDOW
                        Provides prediciton window for evaluating the
                        prediciton power.
```

Example:
```
$ python model_evaluation.py --start_date 2021-05-10 --end_date 2021-11-10 --prediction_window 15
```

## Results and Exprected Outputs

* Daily confirmed cases prediction (with 30-days forecast) in Cook County:

![Alt text](images/Daily_cases.svg?raw=true "Title")
