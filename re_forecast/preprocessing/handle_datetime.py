import datetime
import pandas as pd

from re_forecast.params import DATE_TIME_COLUMNS


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
    gen_df_copy = gen_df.copy(deep = True)

    # Iterate over the datetime columns
    for dt_column in dt_columns:
        try:
            # Handle summer and winter time and transform to datetime the column
            gen_df_copy.loc[:, dt_column] = gen_df_copy[dt_column].apply(handle_seasonal_time)

        except ValueError as e:
            print(e)

    return gen_df_copy


def construct_time_consistent_df(gen_df: pd.DataFrame,
                                 dt_columns: list, # start_date or/and end_date columns, not updated date column
                                 freq = "1H",
                                 dt_columns_all = DATE_TIME_COLUMNS
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

    # Format to datetime the df
    gen_df_dt = format_to_datetime(gen_df, dt_columns_all)

    # Instanciate a list "date_cols_complete" to fill wit the names of complete
    # datetime columns. We will use this list for the following merge
    date_cols_complete = []

    # Iterate over the date columns
    for i, date_col in enumerate(dt_columns):
        # 1/ Create the reference datetime serie

        # Collect start and end date of the date col
        start = gen_df_dt[date_col].min()
        end = gen_df_dt[date_col].max()

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
    gen_df_dt = pd.merge(left = date_complete, right = gen_df_dt, left_on = date_cols_complete, right_on = dt_columns, how = "left")

    return gen_df_dt
