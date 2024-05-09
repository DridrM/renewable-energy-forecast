import pandas as pd


def transform_dt_df_into_supervised(gen_df: pd.DataFrame,
                                    value_col: str,
                                    nb_features: int
                                    ) -> pd.DataFrame:
    """Transform a time serie df into a df suited for supervised
    learning techniques, by offsetting several times the value column by one
    time step.
    Arguments:
    - gen_df: A consistent time serie df with one or more complete datetime columns
    and one value column
    - value_col: the name of the value column
    - nb_features: the number of features to create by offsetting the value column"""

    # Verify if the number of feature you want to create is superior to 1
    if nb_features <= 1:
        raise ValueError("Please insert a number of features superior to 1")

    # Copy the original df
    gen_df_supervised = gen_df.copy(deep = True)

    # Get the index of the target value column
    col_index = gen_df_supervised.columns.get_indexer([value_col])

    # Iterate over the number of features
    for i in range(1, nb_features + 1):
        # Create the offset feature (dataframe)
        feature_i = gen_df_supervised.iloc[:-i, col_index].rename({value_col: f"{value_col}_{i}"}, axis = 1)

        # Reset the index in order to correspond to the gen_df index minus the
        # last element of the index
        feature_i.index = gen_df_supervised.index[i:]

        # Merge the new feature with the original df
        gen_df_supervised = gen_df_supervised.join(feature_i)

    return gen_df_supervised
