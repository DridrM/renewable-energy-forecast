import datetime
import time


####################################################
# API calls function: params handling for API call #
####################################################


def handle_params_presence(**params) -> dict | None:
    """Return the dict of params without 'None'
    if there is at least one provided, else return None"""

    # If at least one of the params is not None:
    if any(params.values()):
        # Return the dict without 'None'
        return {key: value for (key, value) in params.items() if value}

    # Else return None
    return None


def handle_datetime_limits(start_date: str,
                           end_date: str,
                           ressource_nb: int,
                           format_dates_mode: int,
                           dt_format = "%Y-%m-%d %H:%M:%S",
                           start_date_limits = {1: datetime.datetime(2014, 12, 15),
                                                2: datetime.datetime(2011, 12, 13),
                                                3: datetime.datetime(2017, 1, 1)}, # According to RTE API doc
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

    """
    This is archive code. It is no more needed as the slice_dates function solve the problem of
    the limits of days we can call for each ressource

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
        return {"start_date": format_dates(start_date_dt, format_dates_mode),
                "end_date": format_dates(end_date_dt_new, format_dates_mode)}
    """

    # Format the dates to API format and return the dates
    return {"start_date": format_dates(start_date_dt, format_dates_mode),
            "end_date": format_dates(end_date_dt, format_dates_mode)}


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
                 mode = 0,
                 delimiters = {0: {"date_time": "T", "tz": "+01:00"},
                               1: {"date_time": "_", "tz": ""},
                               2: {"date_time": " ", "tz": ""}}
                 ) -> str:
    """Transform a datetime object into a string, with the right
    format accepted by RTE API.
    If the param 'storage' is set to true, the function return the date at the right
    format for the name of csv files used to store generation data
    Mode correspond to the format mode:
    - 0: Format the date for the RTE API call
    - 1: Format the date for the store data names
    - 2: Format the date for the slice_dates function"""

    # /!\ Option: Handle winter and summer time zones in France

    # Unpack all the time params and format them
    year = format_time_values(date.year)
    month = format_time_values(date.month)
    day = format_time_values(date.day)
    hour = format_time_values(date.hour)
    minute = format_time_values(date.minute)
    second = format_time_values(date.second)

    # Set the right delimiters if the dates are for the names of stored datas or not
    date_time_delimiter = delimiters[mode]["date_time"]
    tz_delimiter = delimiters[mode]["tz"]

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
                                   format_dates_mode = 0)

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


def slice_dates(ressource_nb: int,
                start_date: str | None,
                end_date: str | None,
                dt_format = "%Y-%m-%d %H:%M:%S",
                ressource_time_delta = {1: 155, 2: 7, 3: 14}, # According to RTE API doc
                ressource_datapoint_timedelta = {1: datetime.timedelta(hours = 1),
                                                 2: datetime.timedelta(hours = 1),
                                                 3: datetime.timedelta(minutes = 15)}
                ) -> list:
    """Transform a time range defined by two dates by a list of sub-ranges, in order
    to respect the limit timedelta ranges asked from the API."""

    # We set the data point timedelta corresponding to the ressource called
    datapoint_timedelta = ressource_datapoint_timedelta[ressource_nb]

    # Set the timedelta _limit depending on the ressource called
    timedelta_limit = ressource_time_delta[ressource_nb]

    # Here, we have to handle the dates consistency
    dates = handle_datetime_limits(start_date,
                                   end_date,
                                   ressource_nb,
                                   format_dates_mode = 2)

    # If the dates are 'None', we already know that is a default API call
    # and we can return 'dates' directly
    if not all(dates.values()):
        return dates

    # Extract the dates from dates
    start_date_str = dates["start_date"]
    end_date_str = dates["end_date"]

    start_date_dt = datetime.datetime.strptime(start_date_str, dt_format)
    end_date_dt = datetime.datetime.strptime(end_date_str, dt_format)

    # Compute the timedelta between end and start date
    timedelta = end_date_dt - start_date_dt

    # Divide the timedelta number of times that the timedelta limit fits in the
    # time range (interval_nb)
    intervals_nb = timedelta.days // timedelta_limit

    # Transform timedelta_limit in datetime timedelta object
    timedelta_limit_dt = datetime.timedelta(days = timedelta_limit)

    # Create the timeranges list
    timeranges = list()

    # Iterate over the intervals_nb to create the time subranges if the interval_nb is more than 0
    if intervals_nb >= 1:
        for interval in range(intervals_nb):
            # Create a subrange dict
            subrange = dict()

            # Create the start_date and the end_date of this time range
            start_date_dt_sub = start_date_dt + interval * timedelta_limit_dt
            end_date_dt_sub = start_date_dt + (interval + 1) * timedelta_limit_dt

            # Fill the subrange dict
            # If the second sub start_date is filled, it is shifted so that there is no
            # overlaping point in the dataset downloaded via the API
            if interval >= 1:
                subrange["start_date"] = start_date_dt_sub + datapoint_timedelta

            # If this is the first start date, there is no shift
            else:
                subrange["start_date"] = start_date_dt_sub

            subrange["end_date"] = end_date_dt_sub

            # Add the subrange dict to the timeranges list
            timeranges.append(subrange)

        # Append the remaining time range
        remaining_start_date_dt = timeranges[-1]["end_date"] + datapoint_timedelta
        timeranges.append({"start_date": remaining_start_date_dt,
                           "end_date": end_date_dt})

    # If the intervals_nb is 0, just append the start and the end date
    else:
        timeranges.append({"start_date": start_date_dt,
                           "end_date": end_date_dt})

    return timeranges


def convert_timerange_to_str(timerange: dict,
                             format_dates_mode: int
                             ) -> dict:
    """Convert a timerange dict with start_date and end_date as datetime
    objects to a timerange dict with dates as strings with a given format."""

    # Extract dates
    start_date_dt = timerange["start_date"]
    end_date_dt = timerange["end_date"]

    # Format dates
    start_date_str = format_dates(start_date_dt, format_dates_mode)
    end_date_str = format_dates(end_date_dt, format_dates_mode)

    return {"start_date": start_date_str, "end_date": end_date_str}


###############################################
# Data management functions: storage handling #
###############################################


def create_csv_path(root_path: str,
                    ressource_nb: int,
                    start_date: str | None,
                    end_date: str | None,
                    eic_code: str | None,
                    prod_type: str | None,
                    prod_subtype: str | None,
                    ressources_names = {1: "actual_generations_per_production_type",
                                        2: "actual_generations_per_unit",
                                        3: "generation_mix_15min_time_scale"},
                    return_csv_name = False
                    ) -> str:
    """Create a csv path by adding the root path, the ressource name based on
    the ressource number you choose and the params one after the other"""

    # Create empty string to store params strings
    params_str = ""

    # Construct the dict of params with the handle_params_storage function
    params = handle_params_storage(ressource_nb,
                                   start_date,
                                   end_date,
                                   eic_code,
                                   prod_type,
                                   prod_subtype)

    # Iterate over the params dict to fill the params string
    for param in params.values():
        # Append only if not None
        if param:
            params_str += f"__{param}"

    # Create the csv_name str
    csv_name = f"{ressources_names[ressource_nb]}{params_str}.csv"

    # If the param return_csv_name is true, just return the params_str
    if return_csv_name:
        return csv_name

    # Concat the ressource name and the params_str into final csv_path
    csv_path = f"{root_path}/{csv_name}"

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

    # Reset storage_params at each call of the function
    # as the previous value for 'storage_params' remain in the function's name space
    storage_params_cp = storage_params.copy()

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
        start_date = format_dates(start_date_dt, mode = 1)
        end_date = format_dates(end_date_dt, mode = 1)

    # Handle the presence of the units names with the handle_params_presence function
    units_names = handle_params_presence(eic_code = eic_code,
                                         prod_type = prod_type,
                                         prod_subtype = prod_subtype)

    # We first update the storage params dict with the start date and the end date
    storage_params_cp["start_date"] = start_date
    storage_params_cp["end_date"] = end_date

    # Choose the right column to put in the unit name
    col = units_names_cols[ressource_nb]

    # If units_names is 'None', this is a default API call and all the gen values
    # for all the units are returned
    if not units_names:
        storage_params_cp[col] = "all-units"

        return storage_params_cp

    # Else return the storage_params dict with the unit name filled in the right column
    match ressource_nb:
        case 1: storage_params_cp[col] = prod_type
        case 2: storage_params_cp[col] = eic_code
        case 3: storage_params_cp[col] = prod_subtype

    return storage_params_cp


#############################################################
# Get data functions: functions used in the get data module #
#############################################################


def api_delay(func,
              minimal_call_timedeltas = {1: 900,
                                         2: 3600,
                                         3: 900},
              ressource_key = "ressource_nb",
              calls = list()
              ):
    """Decorator that block the execution of a API related function
    before a given delay"""
    def wrapper(**kwargs) -> any:
        """The wrapper return the function when two consecutive function call
        are more than x seconds appart, with x a defined timedelta"""

        # We append the call time to calls list
        calls.append(time.time())

        # Set the right minimal timedelta depending on the ressource
        ressource = kwargs[ressource_key]
        minimal_call_timedelta = minimal_call_timedeltas[ressource]

        # The function is return at the first call
        if len(calls) <= 1:

            return func(**kwargs)

        # The function is not return when the timedelta between two following
        # calls is less than the minimal timedelta. An error message is printed.
        elif calls[-1] - calls[-2] < minimal_call_timedelta:
            print(f"You cannot make two following {func.__name__} calls less than {minimal_call_timedelta}s appart.")

            return

        # In all other cases, the function is returned.
        else:

            return func(**kwargs)

    return wrapper
