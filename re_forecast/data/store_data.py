import datetime
import csv
import os

from re_forecast.data.utils import create_csv_path, create_csv_path_units_names
from re_forecast.data.manage_data_storage import fill_register

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
                 start_date: str | None,
                 end_date: str | None,
                 eic_code: str | None,
                 prod_type: str | None,
                 prod_subtype: str | None,
                 store_units_names = False
                 ) -> None:
    """Store the format data into a csv, using the create_csv_path
    function and the write_csv function.
    If you set 'store_units_names' to true, the function will create
    a special path name to store the list of units names."""

    # Error handling: assure that data is a list
    if isinstance(data, list):

        # Case we want to store units names list
        if store_units_names:
            # First create root dir if not exists
            create_dir_if_not_exists(root_path)

            # Note: unlike for generation values, there is no register for units names
            # Create the csv path
            csv_path = create_csv_path_units_names(root_path,
                                                   ressource_nb)

            # Write the csv if it doesn't exists
            write_if_not_exists(data, csv_path)

        # General case: we want to store format data from the API
        else:
            # First create root dir if not exists
            create_dir_if_not_exists(root_path)

            # Create the csv path
            csv_path = create_csv_path(root_path,
                                       ressource_nb,
                                       start_date,
                                       end_date,
                                       eic_code,
                                       prod_type,
                                       prod_subtype)

            # Fill the register
            fill_register(ressource_nb,
                          start_date,
                          end_date,
                          eic_code,
                          prod_type,
                          prod_subtype)

            # Again, write the csv if it doesn't exists already
            write_if_not_exists(data, csv_path)

    # Case this isn't a list: the function format_data probably return 'None'
    else:
        print("The function format_data malfuncitoned, due to a problem in the API call")
        print(data)
