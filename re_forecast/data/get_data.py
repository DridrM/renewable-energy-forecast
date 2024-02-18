import pandas as pd

from re_forecast.data.load_data import download_rte_data
from re_forecast.data.format_data import extract_generation_units, extract_generation_values, extract_all_generation_values
from re_forecast.data.store_data import store_to_csv
from re_forecast.data.utils import delay
from re_forecast.params import DATA_CSV_ENERGY_PRODUCTION_PATH


@delay
def get_rte_data() -> pd.DataFrame:
    """"""


@delay
def get_rte_units_names() -> pd.DataFrame:
    """"""
