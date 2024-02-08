def extract_generation_units(json: dict,
                             ressource_nb: int,
                             nomenclature_lvl_1 = {1: "actual_generations_per_production_type",
                                                   2: "actual_generations_per_unit",
                                                   3: "generation_mix_15min_time_scale"},
                             nomenclature_lvl_2 = {1: "production_type",
                                                   2: "unit",
                                                   3: ["production_type", "production_subtype"]}
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
                              nomenclature_lvl_1 = {1: "actual_generations_per_production_type",
                                                    2: "actual_generations_per_unit",
                                                    3: "generation_mix_15min_time_scale"},
                              nomenclature_lvl_2_units = {1: "production_type",
                                                          2: "unit",
                                                          3: "production_subtype"},
                              nomenclature_lvl_3_units = {1: None,
                                                          2: "eic_code",
                                                          3: None},
                              nomenclature_lvl_2_values = {1: "values",
                                                           2: "values",
                                                           3: "values"}
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

        # Case for ressources 1 and 3
        if unit[key_lvl_2_units] == unit_name:
            # Return the list of the generation values per hour
            return unit[key_lvl_2_values]


def extract_all_generation_values(json: dict,
                                  ressources_nb: int,
                                  units_names: list
                                  ) -> list:
    """Return the generation values for all units names"""

    # Instanciate an empty list to add generation_values lists to
    generation_values_all = []

    # Iterate over the list of units names
    for name in units_names:
        # Call the extract generation values function
        generation_values = extract_generation_values(json,
                                                      ressources_nb,
                                                      name)

        # Error handling: if type return is dict, the API call encounter an error
        # Print an error message and show the dict containing the error message
        if not isinstance(generation_values, list):
            print("The JSON return by the API is not at the right format, the API may encounter an issue")
            print(generation_values)

            # At this point the data return by the API is not at the right format,
            # the function stop
            return

        # Iterate over the values of generation_values
        for value in generation_values:
            # Modify in place the dict 'value': add key value pair with 'unit_name': name
            value['unit_name'] = name

        # Add the curent generation_values to generation_values_all
        generation_values_all += generation_values

    return generation_values_all
