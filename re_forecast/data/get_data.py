import pandas as pd

from re_forecast.data.load_data import download_rte_data
from re_forecast.data.format_data import extract_generation_units, extract_all_generation_values
from re_forecast.data.store_data import store_to_csv
from re_forecast.data.manage_data_storage import register_exists, gen_file_exists, units_names_file_exists
from re_forecast.data.read_data import read_generation_data, read_units_names_data
from re_forecast.data.utils import api_delay, call_delay, slice_dates, format_dates
from re_forecast.params import DATA_CSV_ENERGY_PRODUCTION_PATH


def download_and_format_gen_data(ressource_nb: int,
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
        start_subdate = format_dates(date_range["start_date"], mode = 2)
        end_subdate = format_dates(date_range["end_date"], mode = 2)

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


def download_and_format_units_names(ressource_nb: int) -> list:
    """Download RTE data with default API call to extract units names
    data, return the list of formated units names data."""

    # Download default RTE data for the ressource called
    data = download_rte_data(ressource_nb,
                             None,
                             None,
                             None,
                             None,
                             None)

    # Format the data in order to extract the units names
    format_units_names_data = extract_generation_units(data, ressource_nb)

    return format_units_names_data


@api_delay
def get_rte_data(ressource_nb: int,
                 start_date: str | None,
                 end_date: str | None,
                 eic_code: str | None,
                 production_type: str | None,
                 production_subtype: str | None,
                 generation_data_path = DATA_CSV_ENERGY_PRODUCTION_PATH,
                 ressources_names = {1: "actual_generations_per_production_type",
                                     2: "actual_generations_per_unit",
                                     3: "generation_mix_15min_time_scale"}
                 ) -> pd.DataFrame:
    """End user and general purpose function used to download, format, store and read
    the electricity generation data collected from the RTE API. The strategy used to manage the
    data files storage is to download them for a given API ressource, start date and end date,
    but for all generation units. When a specific data file is read, it is also filtered to
    show you the result corresponding to the other params passed (eic_code, production_type,
    production_subtype).
    Notes:
    - For the dates, please use this format: 'YYYY-MM-DD hh:mm:ss'
    - For the eic code and the prod type, please refer to the API documentation
    Note: Fill the function arguments as key words arguments, due to the api_delay decorator."""

    # First, check if the ressource number is correct
    if ressource_nb in list(ressources_names.keys()):
        #############################################################################
        # Verify if the register exists. If it doesn't, download, format, store     #
        # and read a new dataset (the store_to_csv function include the creation of #
        # a new register)                                                           #
        #############################################################################
        if not register_exists():
            ## Download and format the data. The download and format function will
            generation_values_all = download_and_format_gen_data(ressource_nb,
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
            gen_file_exists_bool = gen_file_exists(ressource_nb,
                                                   start_date,
                                                   end_date,
                                                   None,
                                                   None,
                                                   None)

            ## If the dataset file exists, read it and return it filtered
            if gen_file_exists_bool:
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
                ## Download and format the data. The download and format function will
                generation_values_all = download_and_format_gen_data(ressource_nb,
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

    # If the ressource number is not amoung the accepted ressource numbers, print an error message
    else:
        print("The ressource number given is incorrect. Here the ressource numbers accepted :\n")
        for key, value in ressources_names.items():
            print(f"{value} -> {key}")


@api_delay
def get_rte_units_names(ressource_nb: int,
                        ressources_names = {1: "actual_generations_per_production_type",
                                            2: "actual_generations_per_unit",
                                            3: "generation_mix_15min_time_scale"},
                        units_names_data_path = DATA_CSV_ENERGY_PRODUCTION_PATH
                        ) -> pd.DataFrame:
    """End user and general purpose function used to download, format, store and read
    the units names from the electricity generation data collected from the RTE API.
    Return the units names for a given ressource as dataframe."""

    ## First, check if the ressource number is correct
    if ressource_nb in list(ressources_names.keys()):
        ## Does the file exists ?
        units_names_file_exists_bool = units_names_file_exists(ressource_nb)

        ## If the units names file exists, read it
        if units_names_file_exists_bool:
            return read_units_names_data(ressource_nb)

        ## If it doesn't, download, format and read it
        else:
            # Download and format units names data
            units_names_data = download_and_format_units_names(ressource_nb)

            # Store the units names data
            store_to_csv(units_names_data,
                         units_names_data_path,
                         ressource_nb,
                         None,
                         None,
                         None,
                         None,
                         None,
                         store_units_names = True)

            return read_units_names_data(ressource_nb)

    ## If the ressource number is not amoung the accepted ressource numbers, print an error message
    else:
        print("The ressource number given is incorrect. Here the ressource numbers accepted :\n")
        for key, value in ressources_names.items():
            print(f"{value} -> {key}")
