# Imports
import pandas as pd
import numpy as np

# Missing values perf measurment with the KL divergence
from scipy.special import kl_div

# Compute a kernel density estimator
from sklearn.neighbors import KernelDensity
from sklearn.preprocessing import MinMaxScaler

from re_forecast.preprocessing.handle_datetime import construct_time_consistent_df


def check_dates_consistency(gen_df: pd.DataFrame,
                            date_col: str,
                            missing_dates_keys = None # Example: {"missing": 1, "non-missing": 0}
                            ) -> pd.DataFrame:
    """Output a df with a complete datetime column, and missing_dates column that
    indicate if the date in the original df is present (False or 0) or missing (True or 1)
    Arguments:
    - gen_df: the df we want to check time consistency (missing dates)
    - date_col: the name of the datetime column we take for reference in time consistency
    Params:
    - missing_dates_keys: dict that indicate in which format you want to indicate 'missing' or 'non-missing'.
    Example: {"missing": 1, "non-missing": 0}"""

    # Construct a df with complete dates
    gen_complete_df = construct_time_consistent_df(gen_df, [date_col])

    # Return a serie that map every missing date on the complete timeline
    missing_dates = gen_complete_df[date_col].isnull()

    # Case user want the output missing dates indicator as being int 0 or 1
    if missing_dates_keys:
        missing_dates = missing_dates.apply(lambda x: missing_dates_keys["missing"] if x else missing_dates_keys["non-missing"])

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
                                               missing_dates_keys = {"missing": 1, "non-missing": 0}
                                               )

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


def compute_kde_time_serie(gen_df: pd.DataFrame,
                           value_col: str,
                           n_samples: int = 1000,
                           bandwidth: str | float = 5,
                           kernel: str = "gaussian",
                           return_density_only = False
                           ) -> np.array:
    """Compute a kernel density estimation (kde) of the value column of a time serie
    df, and return a sample of this kde for a given number of sample points. Also return
    the array of the sample points.
    Arguments:
    - gen_df: A consistent time serie df with one or more complete datetime columns
    and one value column
    - value_col: the name of the value column
    Parameters:
    - n_samples: the number of sample points
    - bandwitdth: the bandwith number of the kde or the method to estimate the best bandwith
    - kernel: the density function to use as kernel for the kde"""

    # Create X, without nan values
    gen_df_without_nan = gen_df.loc[~gen_df[value_col].isnull(), :].copy(deep = True)
    X = gen_df_without_nan[[value_col]]

    # Instanciate a KDE object from sklearn
    kde = KernelDensity(kernel = kernel, bandwidth = bandwidth).fit(X)

    # Create the points from which to sample
    min_value = np.min(X, axis = 0).value
    max_value = np.max(X, axis = 0).value
    points_to_sample = np.linspace(min_value, max_value, n_samples)

    # Create a df with points_to_sample in order to have 2D data
    # and to give the column the same name than the value column of the
    # original df (avoiding annoying warning)
    points_to_sample_2d = pd.DataFrame({f"{value_col}": points_to_sample})

    # Compute the log-density
    log_density = kde.score_samples(points_to_sample_2d)

    # Compute the log-likelihood from the original points of the time serie df
    log_likelihood = kde.score(X)

    # Return only the density if requested
    if return_density_only:
        return np.exp(log_density)

    # Return the points to sample and the sampled density
    return points_to_sample, np.exp(log_density), log_likelihood


def compute_kl_divergence_time_series(gen_df_reference: pd.DataFrame,
                                      gen_df_to_evaluate: pd.DataFrame,
                                      value_col: str,
                                      n_samples: int = 1000,
                                      bandwidth: str | float = 5,
                                      kernel: str = "gaussian",
                                      ) -> float:
    """Compute the K-L divergence for two time series, one reference time serie
    and a time serie we want to evaluate For more informations about the K-L divergence,
    see the 'kl_div' function scipy documentation.
    Arguments:
    - gen_df_reference: A consistent time serie df with one or more complete datetime columns
    and one value column
    - gen_df_to_evaluate: A consistent time serie df with one or more complete datetime columns
    and one value column
    - value_col:
    Parameters:
    - n_samples: the number of sample points
    - bandwitdth: the bandwith number of the kde or the method to estimate the best bandwith
    - kernel: the density function to use as kernel for the kde
    """

    # Copy
    gen_df_reference = gen_df_reference.copy(deep = True)
    gen_df_to_evaluate = gen_df_to_evaluate.copy(deep = True)

    # In order to compare the two series for the exact same range of values,
    # we have to min-max scale these series
    gen_df_reference.loc[:, value_col] = MinMaxScaler().fit_transform(gen_df_reference[[value_col]])
    gen_df_to_evaluate.loc[:, value_col] = MinMaxScaler().fit_transform(gen_df_to_evaluate[[value_col]])

    # Compute the kernel density estimation for both df
    density_reference = compute_kde_time_serie(gen_df_reference,
                                               value_col,
                                               n_samples = n_samples,
                                               bandwidth = bandwidth,
                                               kernel = kernel,
                                               return_density_only = True)

    density_to_evaluate = compute_kde_time_serie(gen_df_to_evaluate,
                                                 value_col,
                                                 n_samples = n_samples,
                                                 bandwidth = bandwidth,
                                                 kernel = kernel,
                                                 return_density_only = True)

    # Return the K-L divergence
    return np.sum(kl_div(density_to_evaluate, density_reference))
