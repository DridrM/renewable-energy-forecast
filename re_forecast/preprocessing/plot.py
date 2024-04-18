import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

from re_forecast.preprocessing.datetime import check_dates_consistency, count_consecutive_time_periods



def plot_missing_dates_repartition(gen_df: pd.DataFrame,
                                   date_col: str
                                   ) -> None:
    """Plot a barplot of the repartition of the 'missing' and 'non-missing data
    of the given df, for its given datetime column.
    Arguments:
    - gen_df: dataframe with a datetime column
    - date_col: the name of the datetime column"""

    # Create the missing dates df
    missing_dates_df = check_dates_consistency(gen_df,
                                               date_col,
                                               missing_dates_keys = {"missing": "missing", "non-missing": "non-missing"}
                                               )

    # Plot the repartition of missing and non missing datas
    plt.figure(figsize = (10, 10))
    ax = plt.axes()
    ax.set_title("Repartition of missing and non missing datas")
    missing_dates_df["missing_dates"].value_counts(normalize = True).plot(kind = "bar", ax = ax)
    plt.show()


def plot_consecutive_time_periods(gen_df: pd.DataFrame,
                                  date_col: str
                                  ) -> None:
    """Plot the distribution of consecutive time periods for missing and non
    missing dates of the given df, for its given datetime column.
    Arguments:
    - gen_df: dataframe with a datetime column
    - date_col: the name of the datetime column"""

    # Compute the consecutuve time periods df
    consecutive_time_periods_df = count_consecutive_time_periods(gen_df, date_col)

    # Filter for missing and non missing dates
    consecutive_missing_dates = consecutive_time_periods_df\
        .loc[consecutive_time_periods_df["value"] == 1, "count"].to_list() # "1" correspond to missing dates
    consecutive_non_missing_dates = consecutive_time_periods_df\
        .loc[consecutive_time_periods_df["value"] == 0, "count"].to_list() # "0" correspond to non missing dates

    # Plot the result
    plt.figure(figsize = (18, 10))

    # Plot repartition of missing dates
    ax = plt.subplot(1, 2, 1)
    ax.set_title("Consecutive missing dates")
    ax.set_xlabel("Duration of consecutive missing date in hour")
    sns.histplot(x = consecutive_missing_dates, ax = ax)

    # Plot the repartition of non missing dates
    ax = plt.subplot(1, 2, 2)
    ax.set_title("Consecutive non missing dates")
    ax.set_xlabel("Duration of consecutive non missing date in hour")
    sns.histplot(x = consecutive_non_missing_dates, ax = ax)

    plt.show()


def plot_time_serie(gen_df: pd.DataFrame,
                    date_col: str | None,
                    value_col: str,
                    dt_index = False
                    ) -> None:
    """Create a plotly line plot of a time serie. A vertical dashed line is draw
    on the plot for each day.
    Arguments:
    - gen_df: The source of the time serie as a dataframe containing at least one
    value column and a datetime column.
    - date_col: The name of the datetime column
    - value_col: The name of the value column
    Parameters:
    - dt_index: True or False. If there is a datetime index istead of a datetime column"""

    # Set the dt_serie is set as the date col
    if date_col and not dt_index:
        dt_serie = gen_df[date_col]

    # Set the datetime column as the index if dt_index is True
    else:
        dt_serie = gen_df.index

    # Create the line plot
    fig = px.line(x = dt_serie, y = gen_df[value_col])

    # Compute the number of days in the dataset
    start = dt_serie.min()
    end = dt_serie.max()
    days = (end - start).days

    # Add vertical lines separating days
    for day in range(days):
        # Add iteratively one day to the start day
        dt_day = start + datetime.timedelta(days = day)

        # Add one vline
        fig.add_vline(x = dt_day,
                      line_width = 0.5,
                      line_dash = "dash",
                      line_color = "black",
                      opacity = 0.3)

    # Show plotly figure
    fig.show()
