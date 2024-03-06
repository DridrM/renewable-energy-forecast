import pandas as pd

from re_forecast.data.load_data import download_rte_data
from re_forecast.data.format_data import extract_generation_units, extract_all_generation_values
from re_forecast.data.store_data import store_to_csv
from re_forecast.data.manage_data_storage import register_exists, file_exists
from re_forecast.data.read_data import read_generation_data
from re_forecast.data.utils import api_delay, call_delay, slice_dates
from re_forecast.params import DATA_CSV_ENERGY_PRODUCTION_PATH


def download_and_format(ressource_nb: int,
                        start_date: str,
                        end_date: str
                        ) -> list:
    """The download and format function is made to bypass the time range limit for the
    datas you can make for one ressource call. To do that, the time range specified is sliced
    into smaller time ranges that respect the time range limit for the ressource called.
    Then, we iterate over the date ranges to download and format the data. The different
    slices of data are aggregated into one ('generation_values_all'). After each iteration,
    a time delay is respected depending on the ressource in order to not overload the API."""

    # Slice the dates
    dates_ranges = slice_dates(ressource_nb,
                               start_date,
                               end_date)

    # Create a list to collect the generation values after each iteration
    generation_values_all = list()

    # Iterate over the dates ranges
    for i, date_range in enumerate(dates_ranges):
        ## Download the dataset
        # Extract the dates for this iteration
        start_subdate = date_range["start_date"]
        end_subdate = date_range["end_date"]

        # Download the dataset
        data = download_rte_data(ressource_nb,
                                 start_subdate,
                                 end_subdate)

        ## Format the dataset
        format_data = extract_all_generation_values(data,
                                                    ressource_nb)

        ## Add the curent list of generation values to generation_values_all
        generation_values_all += format_data

        # Verify if the last item of dates_ranges is reach; in that case, the loop
        # is broken because there is no reason to delay
        if (i + 1) == len(dates_ranges):
            break

        # Otherwise delay the next loop with a delay corresponding
        # to the ressource called
        else:
            call_delay(ressource_nb)

    return generation_values_all


@api_delay
def get_rte_data(ressource_nb: int,
                 start_date: str | None,
                 end_date: str | None,
                 eic_code: str | None,
                 production_type: str | None,
                 production_subtype: str | None,
                 generation_data_path = DATA_CSV_ENERGY_PRODUCTION_PATH
                 ) -> pd.DataFrame:
    """"""

    #############################################################################
    # Verify if the register exists. If it doesn't, download, format, store     #
    # and read a new dataset (the store_to_csv function include the creation of #
    # a new register)                                                           #
    #############################################################################
    if not register_exists():
        ## Download and format the data. The download and format function will
        generation_values_all = download_and_format(ressource_nb,
                                                    start_date,
                                                    end_date)

        ## Store the data
        store_to_csv(generation_values_all,
                     generation_data_path,
                     ressource_nb,
                     start_date,
                     end_date,
                     None,
                     None,
                     None,
                     store_units_names = False) # Note that if the register does not exists, it is created at this step inside the 'store_to_csv' function

        ## Read the data
        generation_data_filtered = read_generation_data(ressource_nb,
                                                        start_date,
                                                        end_date,
                                                        eic_code,
                                                        production_type,
                                                        production_subtype,
                                                        generation_data_path)

        return generation_data_filtered

    #############################################################################
    # Verify if the register exists. If it does, first check in the register if #
    # the dataset file exists, read it and return it. If the dataset file does  #
    # not exists, download, format, store and read a new dataset                #
    #############################################################################
    else:
        # Determine the existence of the file and recreate the csv file name
        file_exists_bool = file_exists(ressource_nb,
                                       start_date,
                                       end_date,
                                       eic_code,
                                       production_type,
                                       production_subtype)

        ## If the dataset file exists, read it and return it filtered
        if file_exists_bool:
            # Read the data and return
            generation_data_filtered = read_generation_data(ressource_nb,
                                                            start_date,
                                                            end_date,
                                                            eic_code,
                                                            production_type,
                                                            production_subtype,
                                                            generation_data_path)

            return generation_data_filtered

        ## If it does not exists, download, format, store and read the data
        else:
            pass



@api_delay
def get_rte_units_names() -> pd.DataFrame:
    """"""
