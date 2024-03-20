import pandas as pd

from re_forecast.data.utils import create_csv_path, create_csv_path_units_names, handle_params_presence, handle_params_presence_read_mode
from re_forecast.params import DATA_CSV_ENERGY_PRODUCTION_PATH


def construct_query_string(bound_word = " and ",
                           **params
                           ) -> str:
    """Construct a query string in the right format for the pandas 'query'
    function. The different params are bounded together in the query string with the
    bound word given by default. If one of the params is 'None', it is not
    included in the final query string."""

    # Instanciate query string
    query_string = ""

    # Iterate over the params to construct the query string
    for param_key, param in params.items():
        # Construct the param sub string if the param is not 'None'
        if param:
            query_sub_string = f"{param_key} == '{param}'"

            # Add to the query string
            query_string += f"{query_sub_string}{bound_word}"

    # Strip any remaining " and " at the end of the query string
    return query_string.strip(bound_word)


def handle_query(ressource_nb: int,
                 eic_code: str | None,
                 production_type: str | None,
                 production_subtype: str | None
                 ) -> str:
    """Construct the query strinng depending on the ressource call.
    If the parameters passes doesn't correspond to the ressource call,
    an error message is raised and the function return None."""

    match ressource_nb:
        case 1:
            # Handle the param presence
            params = handle_params_presence(production_type = production_type)

            # Case the param does not correspond to the ressource called
            if not params:
                print(f"The param(s) does not correspond to the ressource nb {ressource_nb}")
                return

            # In other case, return the query string
            else:
                return construct_query_string(production_type = production_type)


        case 2:
            # Handle the param presence
            params = handle_params_presence(eic_code = eic_code)

            # Case the param does not correspond to the ressource called
            if not params:
                print(f"The param(s) does not correspond to the ressource nb {ressource_nb}")
                return

            # In other case, return the query string
            else:
                return construct_query_string(eic_code = eic_code)

        case 3:
            # Handle the param presence
            params = handle_params_presence(production_type = production_type,
                                            production_subtype = production_subtype)

            # Case the param does not correspond to the ressource called
            if not params:
                print(f"The param(s) does not correspond to the ressource nb {ressource_nb}")
                return

            # In other case, return the query string
            else:
                return construct_query_string(production_type = production_type,
                                              production_subtype = production_subtype)


def query_generation_data(generation_data: pd.DataFrame,
                          ressource_nb: int,
                          eic_code: str | None,
                          production_type: str | None,
                          production_subtype: str | None
                          ) -> pd.DataFrame:
    """Query the generation data df with the query string constructed with the
    construct_query_string funnction. If no params are given, the function return
    the generation data as it is. If the params given does not correspond to the
    ressource called, the generation data is return as it is. In all other cases,
    the generation data is filter using the query string constructed in the construct_query_string
    function."""

    # Use the handle params presence function
    params = handle_params_presence_read_mode(eic_code = eic_code,
                                              production_type = production_type,
                                              production_subtype = production_subtype)

    # If there is no params there is no query, just return the data as it is
    if not params:
        return generation_data

    # Construct the query string
    query_string = handle_query(ressource_nb,
                                eic_code = params["eic_code"],
                                production_type = params["production_type"],
                                production_subtype = params["production_subtype"])

    # The query string can be empty when the param(s) doesn't correspond to the ressource
    if not query_string:
        print("The generation data will be return without filtering")
        return generation_data

    return generation_data.query(query_string)


def read_generation_data(ressource_nb: int,
                         start_date: str | None,
                         end_date: str | None,
                         eic_code: str | None,
                         production_type: str | None,
                         production_subtype: str | None,
                         generation_data_path: str,
                         ) -> pd.DataFrame:
    """Read the generation data and query it in order to filter given the params
    corresponding to a given ressource called. The presence and the correspondance
    of the params with the ressource called is taken into account."""

    # Re-construct the file name based on the params
    generation_file_name = create_csv_path("",
                                           ressource_nb,
                                           start_date,
                                           end_date,
                                           None,
                                           None,
                                           None) # The last three params are set to 'None' because of the download strategy:
                                                                # Download for all type of generation, and filter for a specific type of generation

    # Construct the full file path
    generation_data_path = f"{generation_data_path}/{generation_file_name}"

    # Read the generation file
    generation_data_full = pd.read_csv(generation_data_path)

    # Filter the generation file
    generation_data_filtered = query_generation_data(generation_data_full,
                                                     ressource_nb,
                                                     eic_code,
                                                     production_type,
                                                     production_subtype)

    return generation_data_filtered


def read_units_names_data(ressource_nb: int,
                          units_names_data_path: str
                          ) -> pd.DataFrame:
    """Read the units names file for a given ressource"""

    # Retreive the csv path
    units_names_path = create_csv_path_units_names(units_names_data_path,
                                                   ressource_nb)

    return pd.read_csv(units_names_path)


if __name__ == "__main__":
    # Set all the params
    ressource = 1
    start_date = "2024-02-25 00:00:00"
    end_date = "2024-02-25 23:00:00"
    eic_code = None
    production_type = "WIND_ONSHORE"
    production_subtype = None

    generation_data_path = DATA_CSV_ENERGY_PRODUCTION_PATH

    # Test the function
    read_generation_data(ressource,
                         start_date,
                         end_date,
                         eic_code,
                         production_type,
                         production_subtype,
                         generation_data_path)
