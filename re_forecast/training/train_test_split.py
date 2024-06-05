import pandas as pd

from re_forecast.params import SPLIT_VALUES_ARGS_NAMES, TRAIN_TEST_SPLIT_PROP_BOUNDS


def check_split_values(split_values_dict: dict,
                       split_values_names: tuple = SPLIT_VALUES_ARGS_NAMES,
                       split_values_bounds: tuple = TRAIN_TEST_SPLIT_PROP_BOUNDS
                       ) -> bool:
    """Check function for the train_test_split_time_serie.
    Check:
    - if there is only one split value
    - if the split value name is amoung the valid split value names
    - if the split value is between 0 and 1 excluded
    Arguments:
    - split_value_dict: dict matching split value names and values
    Parameters:
    - split_values_names: tuple containing the names of the split values arguments
    - split_values_bounds: bounds to respect for the split values"""

    # Check if the dict is lenght 1
    if len(split_values_dict) != 1:
        return False

    # Check if the name of the split value is among the accepted names
    if not all(key in split_values_names for key in split_values_dict.keys()):
        return False

    # Check if the split value is between 0 and 1
    if not all(split_values_bounds[0] < value < split_values_bounds[1] for value in split_values_dict.values()):
        return False

    return True


def compute_split_value(split_value_dict: dict,
                        split_values_names: tuple = SPLIT_VALUES_ARGS_NAMES
                        ) -> float:
    """Compute the split value depending on the split type,
    train or test.
    Arguments:
    - split_value_dict: dict matching split value names and values
    Parameters:
    - split_values_names: tuple containing the names of the split values arguments"""

    # Extract the name of the split value
    split_value_key = list(split_value_dict.keys())[0]

    # If the split value is equal to "train_split",
    # just return the split value corresponding
    if split_value_key == split_values_names[0]:
        return split_value_dict[split_value_key]

    # Else return 1 - the split value given
    return 1 - split_value_dict[split_value_key]


def train_test_split_time_serie(gen_df: pd.DataFrame,
                                **split_value: float | None
                                ) -> tuple:
    """Train test split a time serie. You can enter a train
    proportion or a test proportion.
    Arguments:
    - gen_df: the time serie df to train test split
    - **split_value: the split proportion, train or test"""

    # Verify the split values
    if not check_split_values(split_value):
        raise ValueError("Enter one valid split value between 0 and 1 excluded")

    # Compute the split value corresponding
    # to the split type, train or test
    split_value = compute_split_value(split_value)

    # Compute the index corresponding to the split value
    split_index = int(split_value*len(gen_df))

    return gen_df.iloc[0:split_index, :], gen_df.iloc[split_index:, :]
