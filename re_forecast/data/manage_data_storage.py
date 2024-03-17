import datetime
import csv
import os
import pandas as pd

from re_forecast.data.utils import handle_params_storage, format_dates, create_csv_path, create_csv_path_units_names
from re_forecast.params import DATA_CSV_ENERGY_PRODUCTION_PATH, DATA_ENERGY_PRODUCTION_REGISTER, METADATA_ENERGY_PRODUCTION_FIELDS


def register_exists(register_path = DATA_ENERGY_PRODUCTION_REGISTER) -> bool:
    """Return True if the register exists, false otherwise"""

    return os.path.isfile(register_path)


def gen_file_exists(ressource_nb: int,
                    start_date: str | None,
                    end_date: str | None,
                    eic_code: str | None,
                    production_type: str | None,
                    production_subtype: str | None,
                    metadata_fields = METADATA_ENERGY_PRODUCTION_FIELDS,
                    return_csv_name = False
                    ) -> bool:
    """Return True if the file exists, False otherwise.
    Any file not present in the register is considered as non existing."""

    # Create the register if it doesn't exists
    if not register_exists():
        create_register()

    # Read the register
    register = read_register()

    # Recreate the csv path
    csv_name = create_csv_path("", # It is just to trigger the create_csv_path function and avoid a positionnal argument error.
                               ressource_nb,
                               start_date,
                               end_date,
                               eic_code,
                               production_type,
                               production_subtype,
                               return_csv_name = True)

    # Check in the register if the file already exists, and if so return data
    csv_name_key = metadata_fields[8]

    # Transform the query into a bool
    file_exists_bool = not register.query(f"{csv_name_key} == '{csv_name}'").empty

    # If the return csv_name param is set to True, return file_exists and the csv name
    if return_csv_name:
        return file_exists_bool, csv_name

    return file_exists_bool


def units_names_file_exists(ressource_nb: int,
                            root_data_path = DATA_CSV_ENERGY_PRODUCTION_PATH
                            ) -> bool:
    """Return true if the units names file exists, false
    otherwise."""

    # Retreive the csv path
    units_names_path = create_csv_path_units_names(root_data_path,
                                                   ressource_nb)

    return os.path.isfile(units_names_path)


def create_register(fields = METADATA_ENERGY_PRODUCTION_FIELDS,
                    register_path = DATA_ENERGY_PRODUCTION_REGISTER
                    ) -> None:
    """Create a csv register to track generation data presence and infos.
    The register is saved at the same path as the data papth 'root_path'
    The infos are:
    - Date of creation of any generation data csv
    - The API ressource called
    - The params of the API call corresponding to a given generation data csv"""

    # If the register already exists, just print a warning message and return
    if os.path.isfile(register_path):
        print("The register already exists")

        return

    # Create the csv and his header
    with open(register_path, mode = "w") as f:
        writer = csv.DictWriter(f, fieldnames = fields.values())

        writer.writeheader()


def create_hash_id(*params) -> int:
    """Create a hash int given any number of params.
    This function is conserved for legacy but no longer used."""

    # Create an empty string as base to receive the params str
    hash_base = ""

    # Iterate over the params and fill the hash_base
    for param in params:
        hash_base += f"{param}"

    # Return the hash of hash_base
    return hash(hash_base)


def create_metadata_row(ressource_nb: int,
                        start_date: str | None,
                        end_date: str | None,
                        eic_code: str | None,
                        production_type: str | None,
                        production_subtype: str | None,
                        ressources_names = {1: "actual_generations_per_production_type",
                                            2: "actual_generations_per_unit",
                                            3: "generation_mix_15min_time_scale"},
                        metadata_fields = METADATA_ENERGY_PRODUCTION_FIELDS
                        ) -> dict:
    """Create the metadata row to append to the register each time a new generation
    dataset is downloaded from the API."""

    # Create the params for storage metadata
    metadata = handle_params_storage(ressource_nb,
                                     start_date,
                                     end_date,
                                     eic_code,
                                     production_type,
                                     production_subtype)

    # Pick the names of the fields to append to the 'metadata' dict
    # in the metadata fields general parameter
    creation_date_key = metadata_fields[1]
    ressource_key = metadata_fields[2]
    csv_name_key = metadata_fields[8]

    # Append to the row the creation date
    now_dt = datetime.datetime.now()
    now_str = format_dates(now_dt, mode = 1)
    metadata[creation_date_key] = now_str

    # Append to the row the ressource name
    metadata[ressource_key] = ressources_names[ressource_nb]

    # Append to the row the csv file name
    csv_name = create_csv_path("", # It is just to trigger the create_csv_path function and avoid a positionnal argument error.
                               ressource_nb,
                               start_date,
                               end_date,
                               eic_code,
                               production_type,
                               production_subtype,
                               return_csv_name = True)
    metadata[csv_name_key] = csv_name

    return metadata


def fill_register(ressource_nb: int,
                  start_date: str | None,
                  end_date: str | None,
                  eic_code: str | None,
                  production_type: str | None,
                  production_subtype: str | None,
                  fields = METADATA_ENERGY_PRODUCTION_FIELDS,
                  register_path = DATA_ENERGY_PRODUCTION_REGISTER
                  ) -> None:
    """Fill the register with data generation date, file name, ressource called
    name and params values."""

    # Use the create register function to create the register if it not already exists
    create_register()

    # Create the row to append to the register
    row = create_metadata_row(ressource_nb,
                              start_date,
                              end_date,
                              eic_code,
                              production_type,
                              production_subtype)

    # Append a new line to the register
    with open(register_path, mode = 'a') as f:
        writer = csv.DictWriter(f, fieldnames = fields.values())

        writer.writerow(row)


def read_register(register_path = DATA_ENERGY_PRODUCTION_REGISTER) -> pd.DataFrame:
    """Read and return the register as dataframe."""

    # Load the register
    register = pd.read_csv(register_path)

    # Return the register
    return register


def delete_generation_data(ressource_nb: int,
                           start_date: str | None,
                           end_date: str | None,
                           eic_code: str | None,
                           production_type: str | None,
                           production_subtype: str | None,
                           register_path = DATA_ENERGY_PRODUCTION_REGISTER,
                           root_data_path = DATA_CSV_ENERGY_PRODUCTION_PATH,
                           metadata_fields = METADATA_ENERGY_PRODUCTION_FIELDS
                           ) -> None:
    """Remove a generation data file given its ressource number and its params,
    and remove the corresponding row inside the register accordingly."""

    # Determine the existence of the file and recreate the csv file name
    file_exists_bool, csv_name = gen_file_exists(ressource_nb,
                                                 start_date,
                                                 end_date,
                                                 eic_code,
                                                 production_type,
                                                 production_subtype,
                                                 return_csv_name = True)

    # If the file you want to delete does not exists, print an error message
    if not file_exists_bool:
        print("The file you want to delete does not exists")
        return

    else:
        # Open the register
        register = read_register()

        # Remove the row corresponding to the csv_name
        csv_name_col = metadata_fields[8]
        row_index = register.loc[register[csv_name_col] == csv_name, :].index
        new_register = register.drop(row_index, axis = 0)

        # Construct the csv_path and delete the file
        csv_path = f"{root_data_path}/{csv_name}"
        os.remove(csv_path)

        # Save the new register by overwritting
        new_register.to_csv(register_path)


def delete_units_names(*ressource_nb: int,
                       root_data_path = DATA_CSV_ENERGY_PRODUCTION_PATH) -> None:
    """Delete the units_names csvs based on their ressource numbers
    specified."""

    # Iterate over the ressource number tuple
    for ressource in ressource_nb:
        if ressource in [1, 2, 3]:
            # Retreive the csv path
            units_names_path = create_csv_path_units_names(root_data_path,
                                                           ressource)

            # Delete the units names csv
            os.remove(units_names_path)

        else:
            print("The ressource number you ask doesn't exists")
