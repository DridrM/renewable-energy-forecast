import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def handle_seasonal_time(date_str: str,
                         winter_time_flag = "+01:00",
                         summer_time_flag = "+02:00",
                         dt_format = "%Y-%m-%dT%H:%M:%S"
                         ) -> datetime.datetime:
    """Transform a date string into datetime object and deal with seasonal
    times. If the seasonal time flag indicate summer time, convert into winter
    time the datetime object by removing one hour.
    Arguments: the date as a string
    - date_str :
    Params:
    - dt_format: datetime format to respect
    - winter/summer_time_flag: The flag at end of date string that indicate winter or summer time"""

    # Ensure that the input in of type 'str'
    if isinstance(date_str, str):

        # If the string end with a winter time flag, rid of the flag and return the
        # datetime object corresponding to the time string
        if date_str.endswith(winter_time_flag):
            date_str = date_str.replace(winter_time_flag, "")

            return datetime.datetime.strptime(date_str, dt_format)

        # If the string end with a summer time flag, rid of the flag and return the
        # datetime object minus one hour corresponding to the time string
        elif date_str.endswith(summer_time_flag):
            date_str = date_str.replace(summer_time_flag, "")

            return datetime.datetime.strptime(date_str, dt_format) - datetime.timedelta(hours = 1)

        # Otherwise, there is an error in the format of the string
        else:
            raise ValueError("Wrong format of the datetime string")

    # If the input is not of type string, the wrong column was selected
    else:
        raise ValueError("Wrong column selected")


def format_to_datetime(gen_df: pd.DataFrame,
                       dt_columns: list
                       ) -> pd.DataFrame:
    """Format to datetime multiple columns of a df among a list of columns.
    Arguments:
    - gen_df: the df which we want to transform the datetime columns
    - dt_columns: a list of names of datetime columns to transform"""

    # /!\ Copy the df
    gen_df = gen_df.copy(deep = True)

    # Iterate over the datetime columns
    for dt_column in dt_columns:
        try:
            # Handle summer and winter time and transform to datetime the column
            gen_df.loc[:, dt_column] = gen_df[dt_column].apply(handle_seasonal_time)

        except ValueError as e:
            print(e)

    return gen_df


def construct_time_consistent_df(gen_df: pd.DataFrame,
                                 dt_columns: list, # start_date or/and end_date columns, not updated date column
                                 freq = "1H"
                                 ) -> pd.DataFrame:
    """Merge the given df with a datetime column without missing values.
    The datetime column take the start date from the existing datetime column in the df,
    and the same is done for the end date. You have to specify the datetime column
    of the given df.
    Arguments:
    - gen_df: The df we want to merge with a consistent datetime column
    - dt_columns: the list of datetime columns you want to complete
    Params:
    - freq: time step between each datetime point in order to construct a consistent datetime column"""

    # /!\ Copy the df
    gen_df = gen_df.copy(deep = True)

    # Instanciate a list "date_cols_complete" to fill wit the names of complete
    # datetime columns. We will use this list for the following merge
    date_cols_complete = []

    # Iterate over the date columns
    for i, date_col in enumerate(dt_columns):
        # 1/ Create the reference datetime serie

        # Collect start and end date of the date col
        start = gen_df[date_col].min()
        end = gen_df[date_col].max()

        # Create the datetime index
        dt_index = pd.date_range(start, end, freq = freq)

        # Create the df of complete dates corresponding to the date_col
        if i == 0:
            date_complete = pd.DataFrame({f"{date_col}_complete": dt_index})

        else:
            date_complete = pd.concat([date_complete, pd.DataFrame({f"{date_col}_complete": dt_index})], axis = 1)

        # Complete the date_cols_complete list
        date_cols_complete.append(f"{date_col}_complete")

    # 2/ Compare the date_complete columns with the choosen date columns in gen_df

    # Merge date_complete df with gen_df
    gen_df = pd.merge(left = date_complete, right = gen_df, left_on = date_cols_complete, right_on = dt_columns, how = "left")

    return gen_df


def check_dates_consistency(gen_df: pd.DataFrame,
                            date_col: str,
                            int_missing_dates = False # Output missing dates indicator as int 0 or 1 if True
                            ) -> pd.DataFrame:
    """Output a df with a complete datetime column, and missing_dates column that
    indicate if the date in the original df is present (False or 0) or missing (True or 1)
    Arguments:
    - gen_df: the df we want to check time consistency (missing dates)
    - date_col: the name of the datetime column we take for reference in time consistency
    Params:
    - int_missing_dates: format the indicator column to int 0 or 1"""

    # Construct a df with complete dates
    gen_complete_df = construct_time_consistent_df(gen_df, [date_col])

    # Return a serie that map every missing date on the complete timeline
    missing_dates = gen_complete_df[date_col].isnull()

    # Case user want the output missing dates indicator as being int 0 or 1
    if int_missing_dates:
        missing_dates = missing_dates.apply(lambda x: 1 if x else 0)

    return pd.DataFrame({f"{date_col}_complete": gen_complete_df[f"{date_col}_complete"],
                         "missing_dates": missing_dates})


def count_consecutive_time_periods(gen_df: pd.DataFrame,
                                  date_col: str
                                  ) -> pd.DataFrame:
    """Construct the distribution of consecutive time periods for missing and non
    missing dates of the given df, for its given datetime column.
    Arguments:
    - gen_df: dataframe with a datetime column
    - date_col: the name of the datetime column"""

    # Create the missing dates df
    missing_dates_df = check_dates_consistency(gen_df,
                                               date_col,
                                               int_missing_dates = True)

    # Add the column that give each "0" or "1" cluster an unique id
    # The diff method check the diffence with the previous row
    # The ne method output true when the difference is not 0
    # The cumsum method is just a cumulative sum
    missing_dates_df["consecutive_group_id"] = missing_dates_df["missing_dates"].diff().ne(0).cumsum()

    # Create the consecutive time periods df
    # This df show what is the consecutive value of each group/cluster
    consecutive_time_periods_df = missing_dates_df\
        .groupby("consecutive_group_id")["missing_dates"]\
        .min().to_frame(name = "value")

    # We create a serie that compute the number of consecutive values in each group/cluster
    value_count = missing_dates_df.groupby("consecutive_group_id")["missing_dates"].count().rename("count")

    # Join the serie and the consecutive time periods df
    consecutive_time_periods_df = consecutive_time_periods_df.join(value_count)

    return consecutive_time_periods_df


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
