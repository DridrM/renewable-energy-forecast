import pandas as pd
import matplotlib.pyplot as plt

from re_forecast.training.train_test_split import train_test_split_time_serie
from re_forecast.training.arima_models import rolling_arima_model


def plot_forecast_arima(fitted_arima: object,
                        train_set: pd.Series,
                        test_set: pd.Series,
                        confidence_prob: float,
                        performance_metric: object,
                        figsize: tuple = (16, 9)
                        ) -> None:
    """Plot the forecast of an ARIMA model compared to the test set, also plot
    the train set values. Compute and show the given performance metric.
    Arguments:
    - fitted_arima: the fitted arima model
    - train_set: the time serie train set as pandas serie
    - test_set: the time serie test set as pandas serie
    - confidence_prob: the confidence probability
    Parameters:
    - figsize: the size of the plot in inches"""

    # Forecast values and the confidence interval
    forecast_results = fitted_arima.get_forecast(len(test_set), alpha = confidence_prob)
    forecast = forecast_results.predicted_mean
    confidence_int = forecast_results.conf_int().values

    # Prepare the forecast, lower and upper confidence bounds series
    forecast_serie = pd.Series(forecast, index = test_set.index)
    lower_serie = pd.Series(confidence_int[:, 0], index = test_set.index)
    upper_serie = pd.Series(confidence_int[:, 1], index = test_set.index)

    # Compute the score of the performance metric
    performance_score = performance_metric(test_set, forecast_serie)
    forecast_message = f"Forecast : {performance_metric.__name__} = {performance_score:.2f}"

    # Plot
    plt.figure(figsize = figsize)
    plt.plot(train_set, label = 'Train set', color = 'navy')
    plt.plot(test_set, label = 'Test set', color = 'navy', ls = '--')
    plt.plot(forecast_serie, label = forecast_message, color = 'orange')
    plt.fill_between(lower_serie.index, lower_serie, upper_serie, color = 'k', alpha = 0.15)
    plt.title('Forecast vs Actuals')
    plt.legend(loc = 'upper left', fontsize = 10)
    plt.show()


def plot_residuals_train_arima(fitted_arima: object,
                               figsize: tuple = (16, 9)
                               ) -> None:
    """Plot the residuals of the fitted ARIMA model on the train set
    Argument:
    - fitted_arima: the fitted arima model
    Parameter:
    - figsize: the size of the plot in inches"""

    # Extract the residuals
    residuals = fitted_arima.resid

    # Plot
    fig, ax = plt.subplots(1, 2, figsize = figsize)
    residuals.plot(title = "Residuals", ax = ax[0])
    residuals.plot(kind = 'kde', title = 'Density', ax = ax[1])


def plot_residuals_test_arima(fitted_arima: object,
                              test_set: pd.Series,
                              figsize: tuple = (16, 9)
                              ) -> None:
    """Plot the residuals of the fitted ARIMA model on the test set
    Argument:
    - fitted_arima: the fitted arima model
    - test_set: the time serie test set as pandas serie
    Parameter:
    - figsize: the size of the plot in inches"""

    # Compute the residuals
    forecast = fitted_arima.forecast(len(test_set))
    forecast = pd.Series(forecast)
    residuals = test_set - forecast

    # Plot
    fig, ax = plt.subplots(1, 2, figsize = figsize)
    residuals.plot(title = "Residuals", ax = ax[0])
    residuals.plot(kind = 'kde', title = 'Density', ax = ax[1])


def plot_rolling_forecast_arima(gen_df: pd.DataFrame,
                                arima_order: tuple,
                                forecast_window: int,
                                performance_metric: object,
                                initial_train_split: float = 0.7,
                                figsize: tuple = (16, 9)
                                ) -> None:
    """Execute a rolling forecast of an ARIMA model, and plot the result alongside the
    test set. Also plot the training set.
    Arguments:
    - gen_df: the time serie data we use to train and forecast
    - arima_order: a tuple containing the auto-regresive, derivative and moving average parameters for ARIMA.
    - forecast_window: the size of the forecast time span given in number of rows
    - performance_metric: the performance metric function (scikit-learn functions)
    Parameters:
    - initial_train_split: the initial train-test split proportion
    - figsize: size of the matplotlib fig"""

    # Train-test split according the initial train-test split
    train_set, test_set = train_test_split_time_serie(gen_df,
                                                      train_split = initial_train_split)

    # Compute the rolling forecast
    rolling_forecast_results, rolling_conf_ints = rolling_arima_model(gen_df,
                                                                      arima_order = arima_order,
                                                                      forecast_window = forecast_window,
                                                                      initial_train_split = initial_train_split)

    # Prepare the lower and upper confidence bounds series
    lower_serie = rolling_conf_ints.iloc[:, 0]
    upper_serie = rolling_conf_ints.iloc[:, 1]

    # Compute the score of the metric
    performance_score = performance_metric(test_set, rolling_forecast_results)
    forecast_message = f"Forecast : {performance_metric.__name__} = {performance_score:.2f}"

    # Plot
    plt.figure(figsize = figsize)
    plt.plot(train_set, label = 'Train set', color = 'navy')
    plt.plot(test_set, label = 'Test set', color = 'navy', ls = '--')
    plt.plot(rolling_forecast_results, label = forecast_message, color = 'orange')
    plt.fill_between(lower_serie.index, lower_serie, upper_serie, color = 'k', alpha = 0.15)
    plt.title('Forecast vs Actuals')
    plt.legend(loc = 'upper left', fontsize = 10)
    plt.show()
