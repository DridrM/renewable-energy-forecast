import pandas as pd

from re_forecast.exploration.compute_statistics import count_consecutive_time_periods, check_dates_consistency
from re_forecast.params import DATA_QUALITY_THRESHOLDS


def check_data_quality(gen_df: pd.DataFrame,
                       date_col: str,
                       quality_thresholds = DATA_QUALITY_THRESHOLDS
                       ) -> tuple:
    """Check weather or not the input time serie dataset
    respects quality standards about proportion of missing values,
    sufficient row number and duration of missing values gaps.
    Arguments:
    - gen_df:
    - date_col: """

    # Set a check indicator to True and a quality check message
    check_indicator = True
    quality_check_message = "Quality check passed"

    # Unpack the thresholds values
    row_nb = quality_thresholds["row_nb"]
    prop_missing_values = quality_thresholds["prop_missing_values"]
    max_empty_gap_duration = quality_thresholds["max_empty_gap_duration"]

    # Check if there is a sufficient number of rows
    if len(gen_df) < row_nb:
        check_indicator = False
        quality_check_message = "Unsufficient number of rows"

        return check_indicator, quality_check_message

    # Check if the proportion of missing value is bellow the threshold


    # Check if the largest missing value gap is bellow the limit


    return check_indicator, quality_check_message
