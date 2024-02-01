def extract_generation_units(json: dict,
                             ressource_nb: int,
                             nomenclature_lvl_1 = {1: "actual_generations_per_production_type",
                                                   2: "actual_generations_per_unit",
                                                   3: "generation_mix_15min_time_scale"},
                             nomenclature_lvl_2 = {1: "production_type",
                                                   2: "unit",
                                                   3: "production_type"}
                             ) -> list:
    """To complete"""

    # Choose the right keys depending on the ressource call
    key_lvl_1 = nomenclature_lvl_1[ressource_nb]
    key_lvl_2 = nomenclature_lvl_2[ressource_nb]

    # Extract the 'units' from the json
    units = json[key_lvl_1]

    # Instanciate empty list
    units_names = []

    # Iterate over the 'units'
    for unit in units:
        # Case ressource 2 call, it already append a dict with two items (name & eic code)
        if ressource_nb == 2:
            units_names.append(unit[key_lvl_2])

        # For other ressources, we want to append a dict with one item
        # in order to use the csv.Dictwriter function later
        else:
            units_names.append({key_lvl_2: unit[key_lvl_2]})

    return units_names


def extract_generation_values():
    pass
