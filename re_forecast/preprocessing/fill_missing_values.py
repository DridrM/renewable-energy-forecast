# Imports
import pandas as pd

# Handle missing values with scikit learn
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import KNNImputer, IterativeImputer

from re_forecast.preprocessing.clean_values import peel_time_serie_df
from re_forecast.preprocessing.make_supervised import transform_dt_df_into_supervised


def interpolate_time_serie_df(gen_df: pd.DataFrame,
                              value_col: str,
                              interpolation_method: str,
                              **kwargs
                              ) -> pd.DataFrame:
    """Interpolate (fill nans values by interpolation) the value
    column of a time serie df.
    Arguments:
    - gen_df: A consistent time serie df with one or more complete datetime columns
    and one value column
    - value_col: the name of the value column
    - interpolation_method: The interpolation method to pass to the pandas
    - **kwargs: any key-word argument to pass to the interpolation function,
    depending on the method called. See 'interpolate' in the pandas doc for more.
    'interpolate' method"""

    # Copy the original df
    gen_df = gen_df.copy(deep = True)

    # Interpolate the value column
    gen_df.loc[:, value_col] = gen_df.loc[:, value_col].interpolate(method = interpolation_method, **kwargs)

    return gen_df


def knn_impute(gen_df: pd.DataFrame,
               value_col: str,
               param: int,
               nb_supervised_features: int = 24,
               ) -> pd.DataFrame:
    """Impute a time serie df with the KNN method from scikit-learn.
    Arguments:
    - gen_df: A consistent time serie df with one or more complete datetime columns
    and one value column
    - param: the parameter we want to optimise for the knn imputer, here the number of
    closest neighbours
    Parameters:
    - nb_supervised_features: number of features to add to the time serie df to transform
    it into a df suited for supervised learning algorithms"""

    # Detect if the df has a dt index. If it doesn't, transform into a peeled df
    if gen_df.index.dtype == "int64":
        gen_df = peel_time_serie_df(gen_df)

    # Transform into a supervised df
    gen_df_supervised = transform_dt_df_into_supervised(gen_df, value_col, nb_supervised_features)

    # Instanciate a KNN imputer
    knn_imputer = KNNImputer(n_neighbors = param)

    # Create the X matrix
    X = gen_df_supervised.copy(deep = True)

    # Fit the imputer
    knn_imputer.fit(X)

    # Transform
    X_imputed = knn_imputer.transform(X)

    # Extract the first column of the X_imputed array, and re-create a df
    return pd.DataFrame({"value": X_imputed[:, 0]}, index = gen_df_supervised.index)


def iterative_impute(gen_df: pd.DataFrame,
                     value_col: str,
                     param: int,
                     nb_supervised_features: int = 24,
                     ) -> pd.DataFrame:
    """Impute a time serie df with the IterativeImputer method from scikit-learn.
    Arguments:
    - gen_df: A consistent time serie df with one or more complete datetime columns
    and one value column
    - param: the parameter we want to optimise for the knn imputer, here the max
    iteration of the iterative algorithm
    Parameters:
    - nb_supervised_features: number of features to add to the time serie df to transform
    it into a df suited for supervised learning algorithms"""

    # Detect if the df has a dt index. If it doesn't, transform into a peeled df
    if gen_df.index.dtype == "int64":
        gen_df = peel_time_serie_df(gen_df)

    # Transform into a supervised df
    gen_df_supervised = transform_dt_df_into_supervised(gen_df, value_col, nb_supervised_features)

    # Instanciate a iterative imputer
    iterative_imputer = IterativeImputer(max_iter = param, random_state = 42)

    # Create the X matrix
    X = gen_df_supervised.copy(deep = True)

    # Fit the imputer
    iterative_imputer.fit(X)

    # Transform
    X_imputed = iterative_imputer.transform(X)

    # Extract the first column of the X_imputed array, and re-create a df
    return pd.DataFrame({"value": X_imputed[:, 0]}, index = gen_df_supervised.index)
