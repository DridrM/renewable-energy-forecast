from re_forecast.params import JSON_LVL1_NOMENCLATURE, JSON_LVL2_UNITS_NOMENCLATURE, JSON_LVL2_VALUES_NOMENCLATURE, JSON_LVL3_UNITS_NOMENCLATURE


def extract_generation_units(json: dict,
                             ressource_nb: int,
                             nomenclature_lvl_1 = JSON_LVL1_NOMENCLATURE,
                             nomenclature_lvl_2 = JSON_LVL2_UNITS_NOMENCLATURE
                             ) -> list:
    """Extract the generation units names from the raw json send by RTE API.
    Note that by 'units', we group what the API doc call 'production type' for
    ressource 1 & 3 and 'units' for ressource 2.
    The function return a list of dicts, usefull to be used later with the
    csv.Dictwriter function."""

    # Choose the right keys depending on the ressource call
    key_lvl_1 = nomenclature_lvl_1[ressource_nb]
    key_lvl_2 = nomenclature_lvl_2[ressource_nb]

    # Error handling: when server return an error message
    # If the server has return an error message when we used the function 'get_rte_data',
    # This first step should not work
    try:
        # Extract the 'units' from the json
        units = json[key_lvl_1]

    # In the case of the server return an error, show the error message and return the json
    except:
        print("The server encounter an error when the function 'download_rte_data' call the API")
        return json

    # Instanciate empty list
    units_names = []

    # Iterate over the 'units'
    for unit in units:
        # Case ressource 2 call, it already append a dict with two items (name & eic code)
        if ressource_nb == 2:
            units_names.append(unit[key_lvl_2])

        # Case ressource 3 call
        elif ressource_nb == 3:
            # Append the type and the subtype
            type_ = key_lvl_2[0]
            subtype = key_lvl_2[1]
            units_names.append({type_: unit[type_], subtype: unit[subtype]})

        # For other ressources, we want to append a dict with one item
        # in order to use the csv.Dictwriter function later
        else:
            units_names.append({key_lvl_2: unit[key_lvl_2]})

    return units_names


def extract_generation_values(json: dict,
                              ressource_nb: int,
                              unit_name: str,
                              nomenclature_lvl_1 = JSON_LVL1_NOMENCLATURE,
                              nomenclature_lvl_2_units = JSON_LVL2_UNITS_NOMENCLATURE,
                              nomenclature_lvl_3_units = JSON_LVL3_UNITS_NOMENCLATURE,
                              nomenclature_lvl_2_values = JSON_LVL2_VALUES_NOMENCLATURE
                              ) -> list:
    """Extract the generation values from the raw json send by RTE API.
    Note: You MUST enter a unit name for this function"""

    # Choose the right keys depending on the ressource call
    key_lvl_1 = nomenclature_lvl_1[ressource_nb]
    key_lvl_2_units = nomenclature_lvl_2_units[ressource_nb]
    key_lvl_2_values = nomenclature_lvl_2_values[ressource_nb]
    key_lvl_3_units = nomenclature_lvl_3_units[ressource_nb]

    # Error handling: when server return an error message
    # If the server has return an error message when we used the function 'get_rte_data',
    # This first step should not work
    try:
        # Extract the 'units' from the json
        units = json[key_lvl_1]

    # In the case of the server return an error, show the error message
    except:
        print("The server encounter an error when the function 'download_rte_data' call the API")

        # In this case 'json' should be a dict containing an error message
        print(json)

        # Stop the function here
        return

    # Iterate over the units
    for unit in units:
        # /!\ The case where no unit_name is given is not cover in this function

        # Case if ressource number equal to 2
        # If the name of the unit correspond to unit_name
        if ressource_nb == 2 and unit[key_lvl_2_units][key_lvl_3_units] == unit_name:
            # Return the list of the generation values per hour
            return unit[key_lvl_2_values]

        # Case for ressource 3
        if ressource_nb == 3 and unit[key_lvl_2_units[1]] == unit_name:
            # Return the list of the generation values per hour
            return unit[key_lvl_2_values]

        # Case for ressources 1
        if ressource_nb == 1 and unit[key_lvl_2_units] == unit_name:
            # Return the list of the generation values per hour
            return unit[key_lvl_2_values]


def extract_all_generation_values(json: dict,
                                  ressource_nb: int,
                                  nomenclature_lvl_1 = JSON_LVL1_NOMENCLATURE,
                                  nomenclature_lvl_2_units = JSON_LVL2_UNITS_NOMENCLATURE,
                                  nomenclature_lvl_3_units = JSON_LVL3_UNITS_NOMENCLATURE,
                                  nomenclature_lvl_2_values = JSON_LVL2_VALUES_NOMENCLATURE
                                  ) -> list:
    """Return the generation values for all units names"""

    # Instanciate an empty list to add generation_values lists to
    generation_values_all = []

    # Choose the right keys depending on the ressource call
    key_lvl_1 = nomenclature_lvl_1[ressource_nb]
    key_lvl_2_units = nomenclature_lvl_2_units[ressource_nb]
    key_lvl_2_values = nomenclature_lvl_2_values[ressource_nb]
    key_lvl_3_units = nomenclature_lvl_3_units[ressource_nb]

    # Error handling: when server return an error message
    # If the server has return an error message when we used the function 'get_rte_data',
    # This first step should not work
    try:
        # Extract the 'units' from the json
        units = json[key_lvl_1]

    # In the case of the server return an error, show the error message
    except:
        print("The server encounter an error when the function 'download_rte_data' call the API")

        # In this case 'json' should be a dict containing an error message
        print(json)

        # Stop the function here
        return

    # Iterate over the units
    for unit in units:
        # Extract the values for one unit
        values = unit[key_lvl_2_values]

        # Append the unit name to the 'values' dict
        for value in values:
            # If the ressource 2 is called, extract and append the unit_name as following
            if ressource_nb == 1:
                unit_name = unit[key_lvl_2_units]
                value[key_lvl_2_units] = unit_name

            # If the ressource 2 is called, extract and append the unit_name as following
            elif ressource_nb == 2:
                unit_name = unit[key_lvl_2_units][key_lvl_3_units]
                value[key_lvl_3_units] = unit_name

            # For ressource 3
            else:
                unit_name = unit[key_lvl_2_units[0]]
                unit_subname = unit[key_lvl_2_units[1]]
                value[key_lvl_2_units[0]] = unit_name
                value[key_lvl_2_units[1]] = unit_subname

        # Add for each units its values to the list 'generation_values_all
        generation_values_all += values

    return generation_values_all
