import pandas as pd
from math import ceil
from statsmodels.tsa.arima.model import ARIMA

from re_forecast.training.train_test_split import train_test_split_time_serie


def rolling_arima_model(gen_df: pd.DataFrame,
                        arima_order: tuple,
                        forecast_window: int,
                        initial_train_split: float = 0.7,
                        arima_freq: str = 'H',
                        confidence_prob: float = 0.05
                        ) -> pd.Series:
    """Execute a rolling forecast of an ARIMA model. At each iteration, extend the
    training set of an amont of 'forecast_window', fit given the ARIMA parameters
    and predict for a time range of 'forecast_window'. Aggregate all the forecasts
    inside an pandas serie.
    Arguments:
    - gen_df: the time serie data we use to train and forecast
    - arima_order: a tuple containing the auto-regresive, derivative and moving average parameters for ARIMA.
    - forecast_window: the size of the forecast time span given in number of rows
    Parameters:
    - initial_train_split: the initial train-test split proportion
    - arima_freq: the sampling frequency of ARIMA
    - confidence_prob: the default p-value threshold for the confidence interval"""

    # Store the forecasts and the confidence intervals inside lists
    forecasts = []
    confidence_ints = []

    # Train test split
    train_set, test_set = train_test_split_time_serie(gen_df,
                                                      train_split = initial_train_split)

    # Initial lenght of the train set
    len_initial_train_set = len(train_set)

    # Compute the number of forecast windows to forecast
    nb_forecast_windows = ceil(len(test_set) / forecast_window)

    # Compute the residual number of time steps
    resid = len(test_set) % forecast_window

    # Iterate over the number of forecast windows
    for k in range(1, nb_forecast_windows + 1):
        # Fit an arima model
        arima = ARIMA(train_set, order = arima_order, freq = arima_freq)
        fitted_arima = arima.fit()

        # Forecast and compute confidence intervals with the fitted arima model
        window = forecast_window if k < nb_forecast_windows else resid
        forecast_results = fitted_arima.get_forecast(window, alpha = confidence_prob)
        forecast = forecast_results.predicted_mean
        confidence_int = forecast_results.conf_int()
        forecasts.append(forecast)
        confidence_ints.append(confidence_int)

        # Re-compute and extend the train-set
        train_split = len_initial_train_set + k * forecast_window
        train_split = min(train_split, len(gen_df))
        train_set = train_test_split_time_serie(gen_df, train_split = train_split)[0]

    # Aggregate the forecasts and the confidence intervals inside series
    rolling_forecasts = pd.concat(forecasts)
    rolling_confidence_ints = pd.concat(confidence_ints)

    return rolling_forecasts, rolling_confidence_ints
