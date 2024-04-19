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


class NormalScaler:

    def __init__(self):
        """Initialize self.mean, self.std and the
        error message used in case of error handling."""

        # Create null std and mean
        self.mean, self.std = (0, 0)

        # Create the error message
        self.error_message = "The fit method must be called before the transform and the inverse_transform method"

    def fit(self, gen_df: pd.DataFrame):
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

    def fit_transform(self, gen_df: pd.DataFrame) -> pd.DataFrame:
        """Extract the mean and the std of a time serie df and
        normalize the time serie.
        Arguments:
        - gen_df: df with one column value and a datetime index"""

        # Fit
        self.fit(gen_df)

        # Transform: Normalization
        return self.transform(gen_df)

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


class Stationarizer:

    def __init__(self):
        """Set the initial_values list and the error message for
        the error handling in the inverse transform method"""

        # Initialize the list of the initial values
        # This list store the initial values and their index
        # The initial values are used as constant of integration for the
        # inverse transform method
        self.initial_values = list()

        # Initialize the error message for the exception handling of the inverse transform function
        self.error_message = "The inverse_transform method cannot be called before the transform method"

    def transform(self,
                  gen_df: pd.DataFrame,
                  order: int
                  ) -> pd.DataFrame:
        """Stationarize the time serie by the order given.
        Arguments:
        - gen_df: df with one column value and a datetime index
        - order: the order of the derivation of the time serie"""

        # Copy the original df
        gen_df_diff = gen_df.copy(deep = True)

        # Case order == 0, just return the copy
        if not order:
            return gen_df_diff

        # Iterate over the order of the derivative
        for _ in range(order):

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


class VolatilityRemover:

    def __init__(self):
        """Initialize self.volatility and the
        error message used in case of error handling."""

        # Set the volatility to 0, for the error handling
        self.volatility = 0

        # Set the error message
        self.error_message = "The fit method must be called before the transform and the inverse_transform method"

    def fit(self,
            gen_df: pd.DataFrame,
            window_size: int
            ):
        """Extract the seasonal volatility of a time serie df
        Arguments:
        - gen_df: df with one column value and a datetime index
        - window_size: size of the window for the rolling standard deviation"""

        # Compute the volatility
        self.volatility = gen_df.rolling(window_size).std().bfill()

    def transform(self, gen_df: pd.DataFrame) -> pd.DataFrame:
        """Remove the volatility of the time serie.
        Arguments:
        - gen_df: df with one column value and a datetime index"""

        # Raise the not fitted error if the inverse_transform method is called before fit
        if isinstance(self.volatility, int):
            raise NotFittedError(f"{self.error_message}")

        # Remove the volatility from the original dataframe
        return gen_df / self.volatility

    def fit_transform(self,
                      gen_df: pd.DataFrame,
                      window_size: int
                      ) -> pd.DataFrame:
        """Extract the seasonal volatility of a time serie df and
        remove the volatility of the time serie.
        Arguments:
        - gen_df: df with one column value and a datetime index
        - window_size: size of the window for the rolling standard deviation"""

        # Fit
        self.fit(gen_df, window_size)

        # Transform: Remove volatility
        return self.transform(gen_df)

    def inverse_transform(self, gen_df: pd.DataFrame):
        """Re-add the volatility to the time serie.
        Arguments:
        - gen_df: df without volatility with one column value
        and a datetime index"""

        # Raise the not fitted error if the inverse_transform method is called before fit
        if isinstance(self.volatility, int):
            raise NotFittedError(f"{self.error_message}")

        # Multiply by the volatility
        return gen_df * self.volatility


class AverageSeasonalityRemover:

    def __init__(self):
        """Initialize self.avg_seasonality and the
        error message used in case of error handling."""

        # Set the avg_seasonality to 0, for the error handling
        self.avg_seasonality = 0

        # Set the error message
        self.error_message = "The fit method must be called before the transform and the inverse_transform method"

    def fit(self,
            gen_df: pd.DataFrame,
            window_size: int
            ):
        """Extract the seasonal avg_seasonality of a time serie df
        Arguments:
        - gen_df: df with one column value and a datetime index
        - window_size: size of the window for the rolling standard deviation"""

        # Compute the avg_seasonality
        self.avg_seasonality = gen_df.rolling(window_size).mean().bfill()

    def transform(self, gen_df: pd.DataFrame) -> pd.DataFrame:
        """Remove the avg_seasonality of the time serie.
        Arguments:
        - gen_df: df with one column value and a datetime index"""

        # Raise the not fitted error if the inverse_transform method is called before fit
        if isinstance(self.avg_seasonality, int):
            raise NotFittedError(f"{self.error_message}")

        # Remove the avg_seasonality from the original dataframe
        return gen_df - self.avg_seasonality

    def fit_transform(self,
                      gen_df: pd.DataFrame,
                      window_size: int
                      ) -> pd.DataFrame:
        """Extract the seasonal avg_seasonality of a time serie df and
        remove the avg_seasonality of the time serie.
        Arguments:
        - gen_df: df with one column value and a datetime index
        - window_size: size of the window for the rolling standard deviation"""

        # Fit
        self.fit(gen_df, window_size)

        # Transform: Remove avg_seasonality
        return self.transform(gen_df)

    def inverse_transform(self, gen_df: pd.DataFrame):
        """Re-add the avg_seasonality to the time serie.
        Arguments:
        - gen_df: df without avg_seasonality with one column value
        and a datetime index"""

        # Raise the not fitted error if the inverse_transform method is called before fit
        if isinstance(self.avg_seasonality, int):
            raise NotFittedError(f"{self.error_message}")

        # Multiply by the avg_seasonality
        return gen_df + self.avg_seasonality
