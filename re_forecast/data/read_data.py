import pandas as pd

from re_forecast.data.manage_data_storage import show_register
from re_forecast.data.utils import create_csv_path


def read_generation_data(ressource_nb: int,
                         start_date: str | None,
                         end_date: str | None,
                         eic_code: str | None,
                         prod_type: str | None,
                         prod_subtype: str | None,
                         generation_data_path: str,
                         metadata_fields: str
                         ) -> pd.DataFrame:
    """"""

    # Re-construct the file name based on the params
    generation_file_name = create_csv_path("",
                                           ressource_nb,
                                           start_date,
                                           end_date,
                                           eic_code,
                                           prod_type,
                                           prod_subtype)

    # Construct the full file path
    generation_data_path = f"{generation_data_path}/{generation_file_name}"

    # Read the generation file
    generation_data_full = pd.read_csv(generation_data_path, header = True)
