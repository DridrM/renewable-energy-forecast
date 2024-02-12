import datetime
import csv

from re_forecast.params import DATA_ENERGY_PRODUCTION_REGISTER
from re_forecast.data.store_data import create_csv_path


####################################################
# API calls function: params handling for API call #
####################################################


def handle_params_presence(**params) -> dict | None:
    """Return the dict of params without 'None'
    if there is at least one provided, else return None"""

    # If at least one of the params is not None:
    if any(params.values()):
        # Return the tuple without 'None'
        return {key: value for (key, value) in params.items() if value}

    # Else return None
    return None


def handle_datetime_limits(start_date: str,
                           end_date: str,
                           ressource_nb: int,
                           for_storage: bool,
                           dt_format = "%Y-%m-%d %H:%M:%S",
                           ressource_time_delta = {1: 155, 2: 7, 3: 14}, # According to RTE API doc
                           start_date_limits = {1: datetime.datetime(2014, 12, 15),
                                                2: datetime.datetime(2011, 12, 13),
                                                3: datetime.datetime(2017, 1, 1)}, # Same as above
                           end_date_limit = datetime.datetime.now()
                           ) -> dict:
    """Handle datetime limits and presence:
    - If at least one date not present: return a dict with 'None' as start and end dates
    - If start and end dates exceed limit dates: return a dict with 'None' as start and end dates
    - If start date is superior to end date: same as before
    - If the start date is before start date limit or if the end date is in the future:
          Same as before
    - If the timedelta between end and start are more than the limit fixed for the ressource asked:
          The end date is set to that limit
    - Else, the function return a dict with start and end date to the right API format
    Please respect the following datetime format: 'YYYY-MM-DD hh:mm:ss'.
    """

    # If at least one of the dates is not provided:
    if not all((start_date, end_date)):
        # Warning message
        print('At least one of the dates is not provided')
        # Return a dict with start and end date as 'None'
        return {'start_date': None, 'end_date': None}

    # Transform dates into datetime objects
    start_date_dt = datetime.datetime.strptime(start_date, dt_format)
    end_date_dt = datetime.datetime.strptime(end_date, dt_format)

    # Set the start date corresponding to the ressource
    start_date_limit = start_date_limits[ressource_nb]

    # Test if the start_date and the end_date does not exceed the limit dates
    if (start_date_dt < start_date_limit) or (end_date_dt > end_date_limit):
        # Warning message
        print("Start date & end date exceeds limits dates, default API call will be made")
        # Return a dict with start and end date as 'None'
        return {'start_date': None, 'end_date': None}

    # Test if end_date is not superior to start_date
    if not end_date_dt > start_date_dt:
        # Warning message
        print("End date is before the start date, default API call will be made")
        # Return a dict with start and end date as 'None'
        return {'start_date': None, 'end_date': None}

    # Else, handle these three cases coresponding to the three ressources
    # x is the number of days separating start and end date depending on the ressource we call
    x = ressource_time_delta[ressource_nb]

    # If the start date and the end date are more than x days appart
    if (end_date_dt - start_date_dt).days > x:
        # Create a new end date x days after start date
        end_date_dt_new = start_date_dt + datetime.timedelta(days = x)

        # Warning message
        print(f"End date is more than {x} days after the start date, end date will be reset to respect the limit")

        # Format the dates to API format and return the dates
        return {"start_date": format_dates(start_date_dt, for_storage),
                "end_date": format_dates(end_date_dt_new, for_storage)}

    # Format the dates to API format and return the dates
    return {"start_date": format_dates(start_date_dt, for_storage),
            "end_date": format_dates(end_date_dt, for_storage)}


def format_time_values(t: int) -> str:
    """Format int time values such as if they are
    inferior to 10, put a '0' in front of them"""

    # If the number is inferior to 10:
    if t < 10:
        # Put a '0' in front of it
        return f"0{t}"

    # Else return the number into string
    return f"{t}"


def format_dates(date: datetime.datetime,
                 for_storage = False,
                 delimiters = {False: {"date_time": "T", "tz": "+01:00"},
                               True: {"date_time": "_", "tz": ""}}
                 ) -> str:
    """Transform a datetime object into a string, with the right
    format accepted by RTE API.
    If the param 'storage' is set to true, the function return the date at the right
    format for the name of csv files used to store generation data"""

    # /!\ Option: Handle winter and summer time zones in France

    # Unpack all the time params and format them
    year = format_time_values(date.year)
    month = format_time_values(date.month)
    day = format_time_values(date.day)
    hour = format_time_values(date.hour)
    minute = format_time_values(date.minute)
    second = format_time_values(date.second)

    # Set the right delimiters if the dates are for the names of stored datas or not
    date_time_delimiter = delimiters[for_storage]["date_time"]
    tz_delimiter = delimiters[for_storage]["tz"]

    # Return API format by extracting time params
    return f"{year}-{month}-{day}{date_time_delimiter}{hour}:{minute}:{second}{tz_delimiter}"


def handle_params(ressource_nb: int,
                 start_date: str | None,
                 end_date: str | None,
                 eic_code: str | None,
                 prod_type: str | None,
                 prod_subtype: str | None) -> dict:
    """Pack together the datetime limits handling function and
    the params presence function.
    Return a dict of params with the right format for the RTE API call."""

    # First handle the dates
    dates = handle_datetime_limits(start_date,
                                   end_date,
                                   ressource_nb,
                                   for_storage = False)

    # Extract start and end date
    start_date = dates["start_date"]
    end_date = dates["end_date"]

    # Handle params presence
    params = handle_params_presence(start_date = start_date,
                                    end_date = end_date,
                                    eic_code = eic_code,
                                    prod_type = prod_type,
                                    prod_subtype = prod_subtype)

    return params


###############################################
# Data management functions: storage handling #
###############################################


def handle_params_storage(ressource_nb: int,
                          start_date: str | None,
                          end_date: str | None,
                          eic_code: str | None,
                          prod_type: str | None,
                          prod_subtype: str | None,
                          default_ressource_timedelta = {1: datetime.timedelta(hours = 23),
                                                         2: datetime.timedelta(hours = 23),
                                                         3: datetime.timedelta(hours = 23, minutes = 45)},
                          units_names_cols = {1: "prod_type",
                                              2: "eic_code",
                                              3: "prod_subtype"},
                          storage_params = {"start_date": None,
                                            "end_date": None,
                                            "eic_code": None,
                                            "prod_type": None,
                                            "prod_subtype": None}
                          ) -> dict:
    """Handle the consistency of the params used to fill the register and to
    create the csv paths for the downloaded generation datas.
    Return a dict with the params names as key, and their values
    How it behaves:
    - When all params are given in input: return a dict with the params names as key, and their values
    - When th dates doesn't respect the datetime limits: see the 'handle_datetimes_limits function
    - When no units names params are given: replace the unit name by 'all-units' and put it at the right key,
        other keys values are set to 'None'
    - When one unit name is given (the one corresponding to the ressource called): the unit name is affected to
        the right key in the params dict returned"""

    # Handle start date and end date
    dates = handle_datetime_limits(start_date,
                                   end_date,
                                   ressource_nb,
                                   for_storage = True)

    # Extract start and end date
    start_date = dates["start_date"]
    end_date = dates["end_date"]

    # In the case start_date or end_date are None, a default API call was made
    # and the API return values for 24 hours.
    if (start_date or end_date) is None:
        # If so the start date is the begening of the curent day
        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        start_date_dt = datetime.datetime(year = year, month = month, day = day)

        # The end date is the start date + the corresponding time delta
        end_date_dt = start_date_dt + default_ressource_timedelta[ressource_nb]

        # Transform the default start date and end date to the right format
        start_date = format_dates(start_date_dt, for_storage = True)
        end_date = format_dates(end_date_dt, for_storage = True)

    # Handle the presence of the units names with the handle_params_presence function
    units_names = handle_params_presence(eic_code = eic_code,
                                         prod_type = prod_type,
                                         prod_subtype = prod_subtype)

    # We first update the storage params dict with the start date and the end date
    storage_params["start_date"] = start_date
    storage_params["end_date"] = end_date

    # Choose the right column to put in the unit name
    col = units_names_cols[ressource_nb]

    # If units_names is 'None', this is a default API call and all the gen values
    # for all the units are returned
    if not units_names:
        storage_params[col] = "all-units"

        return storage_params

    # Else return the storage_params dict with the unit name filled in the right column
    match ressource_nb:
        case 1: storage_params[col] = prod_type
        case 2: storage_params[col] = eic_code
        case 3: storage_params[col] = prod_subtype

    return storage_params


def create_register(fields = ['creation_date',
                              'ressource',
                              'start_date',
                              'end_date',
                              'eic_code',
                              'prod_type',
                              'prod_subtype',
                              'file_name']
                    ) -> None:
    """Create a csv register to track generation data presence and infos.
    The register is saved at the same path as the data papth 'root_path'
    The infos are:
    - Date of creation of any generation data csv
    - The API ressource called
    - The params of the API call corresponding to a given generation data csv"""

    # Create the csv
    with open(DATA_ENERGY_PRODUCTION_REGISTER, mode = "w") as f:
        writer = csv.DictWriter(f, fieldnames = fields)

        writer.writeheader()


def fill_register(ressource_nb: int,
                  start_date: str | None,
                  end_date: str | None,
                  eic_code: str | None,
                  prod_type: str | None,
                  prod_subtype: str | None,
                  ressources_names = {1: "actual_generations_per_production_type",
                                      2: "actual_generations_per_unit",
                                      3: "generation_mix_15min_time_scale"}
                  ) -> None:
    """Fill the register with data generation date and params value"""

    # Create the row to append to the register
    row = handle_params_storage(ressource_nb,
                                start_date,
                                end_date,
                                eic_code,
                                prod_type,
                                prod_subtype)

    # Append to the row the ressource name
    row["ressource"] = ressources_names[ressource_nb]

    # Append to the row the creation date
    row["creation_date"] = datetime.datetime.now()

    # Append to the row the csv file name
    row["file_name"] = create_csv_path(ressource_nb,
                                       start_date,
                                       end_date,
                                       eic_code,
                                       prod_type,
                                       prod_subtype,
                                       return_csv_name = True)

    # Append a new line to the register
    with open(DATA_ENERGY_PRODUCTION_REGISTER, mode = 'a') as f:
        writer = csv.DictWriter(f)

        writer.writerow(row)


# def delete_generation_data()


# def delete_units_names()
