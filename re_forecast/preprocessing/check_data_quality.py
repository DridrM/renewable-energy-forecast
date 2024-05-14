import pandas as pd

from re_forecast.preprocessing.handle_datetime import construct_time_consistent_df
from re_forecast.exploration.compute_statistics import count_consecutive_time_periods, check_dates_consistency
from re_forecast.params import DATA_QUALITY_THRESHOLDS


def check_nb_row(gen_df: pd.DataFrame,
                 eval_col: str,
                 threshold: int | float,
                 ) -> bool:
    """Check if the gen_df respect the threshold for the
    minimal number of values
    Arguments:
    - gen_df: the time serie df we want to check the quality of the data
    - eval_col: the column name used to evaluate the quality
    - threshold: the value threshold to respect in order to pass the quality check"""

    # Construct a time complete (time consistent) df
    gen_df_complete = construct_time_consistent_df(gen_df, [eval_col])

    return len(gen_df_complete[eval_col]) > threshold


def check_missing_values_prop(gen_df: pd.DataFrame,
                              eval_col: str,
                              threshold: int | float
                              ) -> bool:
    """Check if the gen_df respect the threshold for the proportion
    of missing values
    Arguments:
    - gen_df: the time serie df we want to check the quality of the data
    - eval_col: the column name used to evaluate the quality
    - threshold: the value threshold to respect in order to pass the quality check"""

    # Compute the df of missing and non missing values
    missing_values_df = check_dates_consistency(gen_df,
                                                eval_col,
                                                missing_dates_keys = {"missing": "missing", "non-missing": "non-missing"}
                                                )

    # Compute the proportion of missing values
    prop_missing_values = missing_values_df.value_counts("missing_dates", normalize = True)["missing"]

    return prop_missing_values < threshold


def check_max_empty_gap_duration(gen_df: pd.DataFrame,
                                 eval_col: str,
                                 threshold: int | float
                                 ) -> bool:
    """Check if the gen_df respect the threshold for the max
    missing value gap duration
    Arguments:
    - gen_df: the time serie df we want to check the quality of the data
    - eval_col: the column name used to evaluate the quality
    - threshold: the value threshold to respect in order to pass the quality check"""

    # Compute the df listing all the time periods with nan values
    consecutive_time_periods_df = count_consecutive_time_periods(gen_df, eval_col)

    # Compute the maximum duration of time periods containing nans
    max_empty_gap_duration = consecutive_time_periods_df\
        .loc[consecutive_time_periods_df["value"] == 1, "count"].max()

    return max_empty_gap_duration < threshold


def check_data_quality(gen_df: pd.DataFrame,
                       eval_col: str,
                       quality_thresholds = DATA_QUALITY_THRESHOLDS
                       ) -> tuple:
    """Check weather or not the input time serie dataset respects quality
    standards defined inside the quality_thresholds parameter.
    Arguments:
    - gen_df: the time serie df we want to check the quality of the datas
    given the quality_thresholds dict
    - eval_col: the column name used by the check functions to evaluate the df
    Parameters:
    - quality_thresholds: dict mapping each quality check name with a tuple
    containing the quality threshold, the name of the quality check function
    and a message explaining the reason of the non quality check"""

    # Set a check indicator to True and a quality check message
    global_check = True
    global_message = "Quality check passed"

    # Check iteratively the df for all the check functions, unpacking
    # the quality_thresholds dict and the eval function
    for threshold, function, message in quality_thresholds.values():
        code_to_eval = f'{function}(gen_df, eval_col, {threshold})'
        check = eval(code_to_eval)

        # If the check is not passed (check function return False),
        # return False and the reason why the test is negative (the message)
        if not check:
            return check, message

    return global_check, global_message
