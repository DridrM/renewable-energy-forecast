import pandas as pd

from re_forecast.data.load_data import download_rte_data
from re_forecast.data.format_data import extract_generation_units, extract_generation_values, extract_all_generation_values
from re_forecast.data.store_data import store_to_csv
from re_forecast.data.manage_data_storage import create_hash_id, register_exists, create_register, show_register
from re_forecast.data.utils import api_delay, call_delay, slice_dates, create_csv_path, slice_df
from re_forecast.params import DATA_CSV_ENERGY_PRODUCTION_PATH, METADATA_ENERGY_PRODUCTION_FIELDS


@api_delay
def get_rte_data(ressource_nb: int,
                 start_date: str | None,
                 end_date: str | None,
                 eic_code: str | None,
                 prod_type: str | None,
                 prod_subtype: str | None,
                 creation_date: str | None,
                 metadata_fields = METADATA_ENERGY_PRODUCTION_FIELDS
                 ) -> pd.DataFrame:
    """"""

    # Verify if the register exists. If it doesn't, download, format, store
    # and read a new dataset (the store_to_csv function include the creation of
    # a new register)
    if not register_exists():
        # Slice the dates
        dates_ranges = slice_dates(ressource_nb,
                                   start_date,
                                   end_date)

        # Create a list to collect the generation values after each iteration
        generation_values_all = list()

        # Iterate over the dates ranges
        for i, date_range in enumerate(dates_ranges):
            ## Download the dataset
            data = download_rte_data(ressource_nb,
                                     start_date,
                                     end_date) # Maybe it is a better strategy to make a call for all units and to filter

            ## Format the dataset
            # Get the units names depending on the ressource called
            # units_names = get_rte_units_names() # To complete

            # # Slice the df following a rule using the slice_df function
            # row_rule = {}
            # units_names_sliced = slice_df(units_names,
            #                               ressource_nb,
            #                               row_rules = row_rule)

            ## Add the curent list of generation values to generation_values_all


            # Verify if the last item of dates_ranges is reach; in that case, the loop
            # is broken because there is no reason to delay
            if (i + 1) == len(dates_ranges):
                break

            # Otherwise delay the next loop with a delay corresponding
            # to the ressource called
            else:
                call_delay(ressource_nb)

        ## Store the data

    else:
        # Read the register
        register = show_register(return_register = True)

        # Recreate the csv path and the hash_id
        csv_name = create_csv_path("", # It is just to trigger the create_csv_path function and avoid a positionnal argument error.
                                   ressource_nb,
                                   start_date,
                                   end_date,
                                   eic_code,
                                   prod_type,
                                   prod_subtype,
                                   return_csv_name = True)
        hash_id = create_hash_id(creation_date, csv_name)

        # Check in the register if already exists, and if so return data
        hash_col_name = metadata_fields[1]

        if register.query(f"{hash_col_name} == {hash_id}"):
            # Read the dataset corresponding to the csv path and return
            pass

        # Download RTE data and format data

        # Store data

        # Read data


@api_delay
def get_rte_units_names() -> pd.DataFrame:
    """"""
