import datetime
import csv
import os
import pandas as pd

from re_forecast.params import DATA_CSV_ENERGY_PRODUCTION_PATH, DATA_ENERGY_PRODUCTION_REGISTER, METADATA_ENERGY_PRODUCTION_FIELDS
from re_forecast.data.utils import handle_params_storage, format_dates, create_csv_path, create_csv_path_units_names


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
    if not os.path.isfile(register_path):
        print("The register already exists")

        return

    # Create the csv and his header
    with open(register_path, mode = "w") as f:
        writer = csv.DictWriter(f, fieldnames = fields.values())

        writer.writeheader()


def create_metadata_row(ressource_nb: int,
                        start_date: str | None,
                        end_date: str | None,
                        eic_code: str | None,
                        prod_type: str | None,
                        prod_subtype: str | None,
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
                                     prod_type,
                                     prod_subtype)

    # Pick the names of the fields to append to the 'metadata' dict
    # in the metadata fields general parameter
    hash_key = metadata_fields[1]
    creation_date_key = metadata_fields[2]
    ressource_key = metadata_fields[3]
    csv_name_key = metadata_fields[9]

    # Append to the row the creation date
    now_dt = datetime.datetime.now()
    now_str = format_dates(now_dt, for_storage = True)
    metadata[creation_date_key] = now_str

    # Append to the row the ressource name
    metadata[ressource_key] = ressources_names[ressource_nb]

    # Append to the row the csv file name
    csv_name = create_csv_path("", # It is just to trigger the create_csv_path function and avoid a positionnal argument error.
                               ressource_nb,
                               start_date,
                               end_date,
                               eic_code,
                               prod_type,
                               prod_subtype,
                               return_csv_name = True)
    metadata[csv_name_key] = csv_name

    # Create a hash with the csv name and the curent date, and fill the hash_id field
    hash_base = f"{now_str}{csv_name}"
    metadata[hash_key] = hash(hash_base)

    return metadata


def fill_register(ressource_nb: int,
                  start_date: str | None,
                  end_date: str | None,
                  eic_code: str | None,
                  prod_type: str | None,
                  prod_subtype: str | None,
                  register_path = DATA_ENERGY_PRODUCTION_REGISTER
                  ) -> None:
    """Fill the register with data generation date, file name, ressource called
    name, unique hash id and params values."""

    # Use the create register function to create the register if it not already exists
    create_register()

    # Create the row to append to the register
    row = create_metadata_row(ressource_nb,
                              start_date,
                              end_date,
                              eic_code,
                              prod_type,
                              prod_subtype)

    # Append a new line to the register
    with open(register_path, mode = 'a') as f:
        writer = csv.DictWriter(f)

        writer.writerow(row)


def show_register(register_path = DATA_ENERGY_PRODUCTION_REGISTER,
                  return_register = False
                  ) -> None | pd.DataFrame:
    """Print the register in any cases. If return_register is set to True,
    the register is returned."""

    # Load the register
    register = pd.read_csv(register_path)

    # Show the register
    print(register)

    # Return the register if specified
    if return_register:
        return register


def delete_generation_data(*hash_id: int,
                           register_path = DATA_ENERGY_PRODUCTION_REGISTER,
                           root_data_path = DATA_CSV_ENERGY_PRODUCTION_PATH,
                           metadata_fields = METADATA_ENERGY_PRODUCTION_FIELDS
                           ) -> None:
    """Remove the generation data csvs among the specified hash_id corresponding
    to these csvs. Remove the lines corresponding to these files in the
    register accordingly."""

    # Open the register
    register = pd.read_csv(register_path)

    # Iterate over the hash_id tuple
    for hash in hash_id:
        # Read the csv name
        hash_col = metadata_fields[1]
        csv_name_col = metadata_fields[9]
        csv_name = register.loc[register[hash_col] == hash, csv_name_col]

        # Contruct the csv path and delete the file
        csv_path = f"{root_data_path}/{csv_name}"

        # Delete the csv file
        os.remove(csv_path)

    # Delete the lines in the register df in one time
    indexes = register.loc[register.loc[hash_col].isin(hash_id), :].index
    new_register = register.drop(indexes)

    # Save the new register, overwrite the old register csv
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
