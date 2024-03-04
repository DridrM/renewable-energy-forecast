import os

########################
# Connexion to RTE API #
########################

# Base url for RTE API's ressources
BASE_URL = os.environ.get("BASE_URL")

# Ressource auth: Authorization
RESSOURCE_AUTH = os.environ.get("RESSOURCE_AUTH")
# Header for the ressource 1 query
CONTENT_TYPE = os.environ.get("CONTENT_TYPE")

# Ressource 1: Generation per production type data
RESSOURCE_1 = os.environ.get("RESSOURCE_1")

# Ressource 2: Generation per production unit data
RESSOURCE_2 = os.environ.get("RESSOURCE_2")

# Ressource 3: Generation per production type 15min time scale data
RESSOURCE_3 = os.environ.get("RESSOURCE_3")

# Client secret for the data ressources query
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")


################
# Data storage #
################

# Path to store the CSVs of the energy production
DATA_CSV_ENERGY_PRODUCTION_PATH = os.environ.get("DATA_CSV_ENERGY_PRODUCTION_PATH")

# Path to the register for energy production data
DATA_ENERGY_PRODUCTION_REGISTER = f"{DATA_CSV_ENERGY_PRODUCTION_PATH}/energy_production_register.csv"

# Metadata fields for energy production data. Columns of the register
METADATA_ENERGY_PRODUCTION_FIELDS = {1: 'hash_id',
                                     2: 'creation_date',
                                     3: 'ressource',
                                     4: 'start_date',
                                     5: 'end_date',
                                     6: 'eic_code',
                                     7: 'production_type',
                                     8: 'production_subtype',
                                     9: 'file_name'}

# Path to store meteo predcion CSVs
DATA_CSV_METEO_PATH = os.environ.get("DATA_CSV_METEO_PATH")
