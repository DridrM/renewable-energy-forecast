import pandas as pd

from re_forecast.preprocessing.check_data_quality import check_data_quality
from re_forecast.preprocessing.handle_datetime import construct_time_consistent_df
from re_forecast.preprocessing.clean_values import set_min_max_limits_time_serie
from re_forecast.preprocessing.fill_missing_values import knn_impute

from re_forecast.params import (DATE_TIME_COLUMNS, VALUE_COL_NAME, MIN_MAX_BOUND_VALUES, KNN_IMPUTATION_MISSING_VALUES)


def preprocess_data(gen_df: pd.DataFrame,
                    dt_columns: list = DATE_TIME_COLUMNS[:-1],
                    value_col: str = VALUE_COL_NAME,
                    min_max_values: list = MIN_MAX_BOUND_VALUES,
                    knn_impute_params: dict = KNN_IMPUTATION_MISSING_VALUES
                    ) -> pd.DataFrame:
    """Hard (not configurable) preprocessing pipeline. Three steps: Check the data
    quality (number of rows available for learning, proportion of missing values
    and length of the missing data gaps), apply the base preprocessing such as
    completing gaps of missing dates and bound the values in respect to a max
    and a min value, and impute the missing values with a KNN imputer. The preprocessed
    data is aimed to be store when the preprocessing is complete in order to separate
    preprocessing and training and to save computing ressources.
    Argument:
    - gen_df: A df with datetime columns and value columns, representing a time serie.
    Parameters:
    - dt_columns: Names of the datetime columns to evauate, without the column "updated dates"
    (because this column does not contain regularly spaced dates and can cause issues)
    - value_col: Name of the value column of the time serie df
    - min_max_values: Minimum and maximum bound values for the time serie df
    - knn_impute_params: Parameters of the KNN imputation of missing values"""

    #############################
    # 1/ Check the data quality #
    #############################

    # Use the check data quality function
    quality_check, message = check_data_quality(gen_df, dt_columns[0])

    # If the quality check is not fulfilled, return the reason why it isn't
    if not quality_check:
        print(message)
        return

    ###############################
    # 2/ Apply base preprocessing #
    ###############################

    # Construct a complete, time consistent df
    gen_df_complete = construct_time_consistent_df(gen_df, dt_columns)

    # Constrain the min and max values of the df
    gen_df_caped = set_min_max_limits_time_serie(gen_df_complete,
                                                 value_col,
                                                 min_value = min_max_values["min_value"],
                                                 max_value = min_max_values["max_value"]
                                                 )

    ################################
    # 3/ Impute the missing values #
    ################################

    # Impute the missing values with a knn algorithm
    gen_df_imputed = knn_impute(gen_df_caped,
                                value_col,
                                **knn_impute_params)

    # Return the df imputed
    return gen_df_imputed
