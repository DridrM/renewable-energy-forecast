import pandas as pd

from re_forecast.exceptions import NotFittedError, NotTransformedError


def peel_time_serie_df(gen_df: pd.DataFrame,
                       keeped_columns = {"dt_column": "start_date_complete", "value_column": "value"}
                       ) -> pd.DataFrame:
    """Drop all columns appart the choosen value column and the choosen
    datetime column
    Arguments:
    - gen_df: a df with datetime columns and value columns, representing a time serie
    Params:
    - keeped_columns: the datetime column and the value column to keep"""

    # /!\ Copy the gen df to avoid setting with copy warning
    gen_df = gen_df.copy(deep = True)

    # Extract the dt column and the value column names
    dt_column = keeped_columns["dt_column"]
    value_column = keeped_columns["value_column"]

    # Set the dt column as index
    gen_df.set_index(dt_column, drop = True, inplace = True)

    # Drop all others columns appart the choosen value column
    droped_columns = gen_df.columns[~gen_df.columns.isin([value_column])]
    gen_df.drop(droped_columns, axis = 1, inplace = True)

    return gen_df


def set_min_max_limits_time_serie(gen_df: pd.DataFrame,
                                  value_col: str,
                                  min_value: float | None = None,
                                  max_value: float | None = None
                                  ) -> pd.DataFrame:
    """Limits min and max values of a time serie df.
    Arguments:
    - gen_df: A consistent time serie df with one or more complete datetime columns
    and one value column
    - value_col: the name of the value column
    - min_value: minimum limit value
    - max_value: maximum limit value
    """

    # Copy the gen_df to avoid the setting with copy warning
    gen_df_copy = gen_df.copy(deep = True)

    # Replace values bellow min_value by min_value
    if isinstance(min_value, (int, float)):
        min_values = gen_df_copy.loc[gen_df_copy[value_col] < min_value, value_col].values
        gen_df_copy.replace({value: min_value for value in min_values}, inplace = True)

    # Replace values above max_value by max_value
    if isinstance(max_value, (int, float)):
        max_values = gen_df_copy.loc[gen_df_copy[value_col] > max_value, value_col].values
        gen_df_copy.replace({value: max_value for value in max_values}, inplace = True)

    return gen_df_copy


class BaseTsScaler:
    """The BaseTsScaler is only used to avoid implementing
    a fit_transform method each time for each Ts scalers
    objects."""

    def __init__(self) -> None:
        pass

    def fit(self, *args) -> None:
        return

    def transform(self, *args) -> None:
        return

    def fit_transform(self, *args) -> any:
        """The fit_transform method to be inherited
        by the child objects."""

        # Fit
        self.fit(*args)

        # Transform
        return self.transform(*args)


class NormalScalerTs(BaseTsScaler):

    def __init__(self) -> None:
        """Initialize self.mean, self.std and the
        error message used in case of error handling."""

        # Create null std and mean
        self.mean, self.std = (0, 0)

        # Create the error message
        self.error_message = "The fit method must be called before the transform and the inverse_transform method"

    def fit(self, gen_df: pd.DataFrame) -> None:
        """Extract the mean and the std of a time serie df
        Arguments:
        - gen_df: df with one column value and a datetime index"""

        # Extract the mean and the std of the df
        self.mean, self.std = gen_df.mean(), gen_df.std()

    def transform(self, gen_df: pd.DataFrame) -> pd.DataFrame:
        """Normalize the time serie.
        Arguments:
        - gen_df: df with one column value and a datetime index"""

        # Raise the not fitted error if the transform method is called before fit
        if isinstance(self.mean, int):
            raise NotFittedError(f"{self.error_message}")

        # Normalize the df
        return (gen_df - self.mean) / self.std

    def inverse_transform(self, gen_df_normalized: pd.DataFrame) -> pd.DataFrame:
        """Inverse normalize the time serie.
        Arguments:
        - gen_df_normalized: normalized df with one column value
        and a datetime index"""

        # Raise the not fitted error if the inverse_transform method is called before fit
        if isinstance(self.mean, int):
            raise NotFittedError(f"{self.error_message}")

        # Inverse a normalize df
        return gen_df_normalized * self.std + self.mean


class StationarizerTs(BaseTsScaler):

    def __init__(self, order: int = 0) -> None:
        """Set the initial_values list and the error message for
        the error handling in the inverse transform method"""

        # Init the order of diferenciation
        self.order = order

        # Initialize the list of the initial values
        # This list store the initial values and their index
        # The initial values are used as constant of integration for the
        # inverse transform method
        self.initial_values = list()

        # Initialize the error message for the exception handling of the inverse transform function
        self.error_message = "The inverse_transform method cannot be called before the transform method"

    def fit(self) -> None:
        """Define the fit method in order to the inherited
        fit_transform method to work properly"""

        return

    def transform(self, gen_df: pd.DataFrame) -> pd.DataFrame:
        """Stationarize the time serie by the order given.
        Arguments:
        - gen_df: df with one column value and a datetime index"""

        # Copy the original df
        gen_df_diff = gen_df.copy(deep = True)

        # Case order == 0, just return the copy
        if not self.order:
            return gen_df_diff

        # Iterate over the order of the derivative
        for _ in range(self.order):

            # 1/ Extract the id of the initial value and the initial value and store them into initial values list #

            # Row number of the first non null value (the initial value)
            id_initial_value = gen_df_diff.notnull().idxmax()

            # The initial value itself
            initial_value = gen_df_diff.loc[id_initial_value].values[0][0]

            # We pack the id and its initial value into a tuple
            initial_value_tuple = (id_initial_value, initial_value)

            # Add the first non null value of the time serie to the initial values list
            self.initial_values.append(initial_value_tuple)

            # 2/ Differenciate sequentially the gen_df_diff #
            gen_df_diff = gen_df_diff.diff()

        return gen_df_diff

    def inverse_transform(self, gen_df_diff: pd.DataFrame) -> pd.DataFrame:
        """Un-stationarize the time serie whatever its order.
        Note: The transform method must be called before the inverse_transform method.
        Arguments:
        - gen_df: df with one column value and a datetime index"""

        # First check if the initial values list is empty and if it is, raise an exception
        if not self.initial_values:
            raise NotTransformedError(f"{self.error_message}")

        # Copy the original df
        gen_df_undiff = gen_df_diff.copy(deep = True)

        # Iterate over the inverted initial values list
        for initial_value_tuple in self.initial_values[::-1]:
            # Unpack the row number and its initial value
            id_initial_value, initial_value = initial_value_tuple

            # Add the initial value to the gen_df_undiff
            # The initial value act as the "constant of integration"
            gen_df_undiff.loc[id_initial_value] = initial_value

            # Cumsum the gen_df_undiff.
            # The cumsum act as an integral
            gen_df_undiff = gen_df_undiff.cumsum()

        return gen_df_undiff


class VolatilityRemoverTs(BaseTsScaler):

    def __init__(self, window_size: int = 1) -> None:
        """Initialize self.volatility and the
        error message used in case of error handling."""

        # Init the window size
        self.window_size = window_size

        # Set the volatility to 0, for the error handling
        self.volatility = 0

        # Set the error message
        self.error_message = "The fit method must be called before the transform and the inverse_transform method"

    def fit(self, gen_df: pd.DataFrame) -> None:
        """Extract the seasonal volatility of a time serie df
        Arguments:
        - gen_df: df with one column value and a datetime index
        - window_size: size of the window for the rolling standard deviation"""

        # Compute the volatility
        self.volatility = gen_df.rolling(self.window_size).std().bfill()

    def transform(self, gen_df: pd.DataFrame) -> pd.DataFrame:
        """Remove the volatility of the time serie.
        Arguments:
        - gen_df: df with one column value and a datetime index"""

        # Raise the not fitted error if the inverse_transform method is called before fit
        if isinstance(self.volatility, int):
            raise NotFittedError(f"{self.error_message}")

        # Remove the volatility from the original dataframe
        return gen_df / self.volatility

    def inverse_transform(self, gen_df: pd.DataFrame) -> pd.DataFrame:
        """Re-add the volatility to the time serie.
        Arguments:
        - gen_df: df without volatility with one column value
        and a datetime index"""

        # Raise the not fitted error if the inverse_transform method is called before fit
        if isinstance(self.volatility, int):
            raise NotFittedError(f"{self.error_message}")

        # Multiply by the volatility
        return gen_df * self.volatility


class AverageSeasonalityRemoverTs(BaseTsScaler):

    def __init__(self, window_size: int = 1) -> None:
        """Initialize self.avg_seasonality and the
        error message used in case of error handling."""

        # Init the window_size
        self.window_size = window_size

        # Set the avg_seasonality to 0, for the error handling
        self.avg_seasonality = 0

        # Set the error message
        self.error_message = "The fit method must be called before the transform and the inverse_transform method"

    def fit(self, gen_df: pd.DataFrame) -> None:
        """Extract the seasonal avg_seasonality of a time serie df
        Arguments:
        - gen_df: df with one column value and a datetime index
        - window_size: size of the window for the rolling standard deviation"""

        # Compute the avg_seasonality
        self.avg_seasonality = gen_df.rolling(self.window_size).mean().bfill()

    def transform(self, gen_df: pd.DataFrame) -> pd.DataFrame:
        """Remove the avg_seasonality of the time serie.
        Arguments:
        - gen_df: df with one column value and a datetime index"""

        # Raise the not fitted error if the inverse_transform method is called before fit
        if isinstance(self.avg_seasonality, int):
            raise NotFittedError(f"{self.error_message}")

        # Remove the avg_seasonality from the original dataframe
        return gen_df - self.avg_seasonality

    def inverse_transform(self, gen_df: pd.DataFrame) -> pd.DataFrame:
        """Re-add the avg_seasonality to the time serie.
        Arguments:
        - gen_df: df without avg_seasonality with one column value
        and a datetime index"""

        # Raise the not fitted error if the inverse_transform method is called before fit
        if isinstance(self.avg_seasonality, int):
            raise NotFittedError(f"{self.error_message}")

        # Multiply by the avg_seasonality
        return gen_df + self.avg_seasonality
