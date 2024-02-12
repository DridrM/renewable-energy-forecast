import datetime


#######################################
# API calls function: params handling #
#######################################


def handle_params_presence(**params) -> tuple | None:
    """Return the tuple of params without 'None'
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
        return {"start_date": date_api_format(start_date_dt),
                "end_date": date_api_format(end_date_dt_new)}

    # Format the dates to API format and return the dates
    return {"start_date": date_api_format(start_date_dt),
            "end_date": date_api_format(end_date_dt)}


def format_time_values(t: int) -> str:
    """Format int time values such as if they are
    inferior to 10, put a '0' in front of them"""

    # If the number is inferior to 10:
    if t < 10:
        # Put a '0' in front of it
        return f"0{t}"

    # Else return the number into string
    return f"{t}"


def date_api_format(date: datetime.datetime) -> str:
    """Transform a datetime object into a string, with the right
    format accepted by RTE API"""

    # /!\ Option: handle winter and summer time zones in France

    # Unpack all the time params and format them
    year = format_time_values(date.year)
    month = format_time_values(date.month)
    day = format_time_values(date.day)
    hour = format_time_values(date.hour)
    minute = format_time_values(date.minute)
    second = format_time_values(date.second)

    # Return API format by extracting time params
    return f"{year}-{month}-{day}T{hour}:{minute}:{second}+01:00"


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
    dates = handle_datetime_limits(start_date, end_date, ressource_nb)

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


# def create_register()


# def fill_register()


# def delete_generation_data()


# def delete_units_names()
