import datetime
import csv
import os


def write_csv(data: list, csv_path: str) -> None:
    """Write csv with the function csv.Dictwriter
    data: list of dicts
    path: path to file"""

    # Error handling when the API return an error
    try:
        # Create the csv and open it with context manager
        with open(csv_path, mode = "w") as f:
            # Create a writer object with the right field names
            writer = csv.DictWriter(f, fieldnames = data[0].keys())

            # Write the header
            writer.writeheader()

            # Write the rows
            writer.writerows(data)

    # In the case of the server return an error, show the error message and return the json
    except:
        print("The JSON return by the API is not at the right format, the API may encounter an issue")
        # In this case 'data' should be a dict containing an error message
        print(data)

        # Delete the file created with the 'open' function
        os.remove(csv_path)


def create_csv_path(root_path: str,
                    ressource_nb: int,
                    ressources_names = {1: "actual_generations_per_production_type",
                                        2: "actual_generations_per_unit",
                                        3: "generation_mix_15min_time_scale"},
                    **params: str,
                    ) -> str:
    """Create a csv path by adding the root path, the ressource name based on
    the ressource number you choose and the params one after the other"""

    # Create empty string to store params strings
    params_str = ""

    # Case all params are None, this is a default API call with the values of today, and all units
    if not any(params.values()):
        # Indicate date if no value: API return values for today
        date = datetime.datetime.now()
        date_str = f"{date.year}-{date.month}-{date.day} {date.hour}:00:00"

        # Indicate all units are included
        units_str = "all-units"

        # Append to params_str
        params_str += f"__{date_str}__{units_str}"

    else:
        # Handle the params
        for param in params.values():
            # If the param is not None, add to params_str
            if param:
                params_str += f"__{param}"

    # Concat the ressource name and the params_str into final csv_path
    csv_path = f"{root_path}/{ressources_names[ressource_nb]}{params_str}.csv"

    # Final check: replace " " by "_"
    csv_path = csv_path.replace(" ", "_")

    return csv_path


def create_csv_path_units_names(root_path: str,
                                ressource_nb: int,
                                ressources_names = {1: "actual_generations_per_production_type",
                                                    2: "actual_generations_per_unit",
                                                    3: "generation_mix_15min_time_scale"},
                                units_names = {1: "production_type",
                                               2: "unit",
                                               3: "production_type_&_subtype"}
                                ) -> str:
    """Create a csv path specifically for the lists of units names,
    depending on the ressource you have called."""


    # Concat the ressource name and the params_str into final csv_path
    csv_path = f"{root_path}/{ressources_names[ressource_nb]}__{units_names[ressource_nb]}.csv"

    # Final check: replace " " by "_"
    csv_path = csv_path.replace(" ", "_")

    return csv_path


def create_dir_if_not_exists(root_path: str) -> None:
    """If the dir specified as 'root_path' does not exists,
    create the dir, else do nothing"""

    # Verify if the dir exists
    if not os.path.isdir(root_path):
        # If not, create it (use makedirs to create intermediary directories)
        os.makedirs(root_path)

    # Otherwise, the function return None


def write_if_not_exists(data: list, csv_path: str) -> None:
    """This function will only write a csv if it doesn't exists
    already at the path specified"""

    # Case 1: Verify if the file exists. If it doesn't exists :
    if not os.path.isfile(csv_path):
        # Write the csv
        write_csv(data, csv_path)

    # Case 2: The file already exists.
    else:
        # Print a message
        print(f"The file {csv_path} already exists.")


def store_to_csv(data: list,
                 root_path: str,
                 ressource_nb: int,
                 start_date: str,
                 end_date: str,
                 eic_code: str,
                 prod_type: str,
                 prod_subtype: str,
                 store_units_names = False
                 ) -> None:
    """Store the format data into a csv, using the create_csv_path
    function and the write_csv function.
    If you set 'store_units_names' to true, the function will create
    a special path name to store the list of units names."""

    # Case we want to store units names list
    if store_units_names:
        # Create the csv path
        csv_path = create_csv_path_units_names(root_path,
                                               ressource_nb)

        # Create root dir if not exists
        create_dir_if_not_exists(root_path)

        # Write the csv if it doesn't exists
        write_if_not_exists(data, csv_path)

    # General case: we want to store format data from the API
    else:
        # Create the csv path
        csv_path = create_csv_path(root_path,
                                ressource_nb,
                                start_date = start_date,
                                end_date = end_date,
                                eic_code = eic_code,
                                prod_type = prod_type,
                                prod_subtype = prod_subtype)

        # Create root dir if not exists
        create_dir_if_not_exists(root_path)

        # Again, write the csv if it doesn't exists already
        write_if_not_exists(data, csv_path)
