import os
import datetime

##########################################
# Connexion to RTE API: Load data module #
##########################################

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


#########################
# Data formating module #
#########################

# Nomenclature of the first level of the JSON received from the RTE API
JSON_LVL1_NOMENCLATURE = {1: "actual_generations_per_production_type",
                          2: "actual_generations_per_unit",
                          3: "generation_mix_15min_time_scale"}

# Nomenclature of the second level (to access the units names)of the JSON received from the RTE API
JSON_LVL2_UNITS_NOMENCLATURE = {1: "production_type",
                                2: "unit",
                                3: ["production_type", "production_subtype"]}

# Nomenclature of the second level (to access the generation values) of the JSON received from the RTE API
JSON_LVL2_VALUES_NOMENCLATURE = {1: "values",
                                 2: "values",
                                 3: "values"}

# Nomenclature of the third level (to access details about the generation unit) of the JSON received from the RTE API
JSON_LVL3_UNITS_NOMENCLATURE = {1: None,
                                2: "eic_code",
                                3: None}


#######################
# Data storage module #
#######################

# Path to store the CSVs of the energy production
DATA_CSV_ENERGY_PRODUCTION_PATH = os.environ.get("DATA_CSV_ENERGY_PRODUCTION_PATH")

# Path to the register for energy production data
DATA_ENERGY_PRODUCTION_REGISTER = f"{DATA_CSV_ENERGY_PRODUCTION_PATH}/energy_production_register.csv"

# Metadata fields for energy production data. Columns of the register
METADATA_ENERGY_PRODUCTION_FIELDS = {1: 'creation_date',
                                     2: 'ressource',
                                     3: 'start_date',
                                     4: 'end_date',
                                     5: 'eic_code',
                                     6: 'production_type',
                                     7: 'production_subtype',
                                     8: 'file_name'}

# Path to store meteo predcion CSVs
DATA_CSV_METEO_PATH = os.environ.get("DATA_CSV_METEO_PATH")


################
# Utils module #
################

# 1/ Parameters for the functions used in the load_data module

# Limit start date accepted by the RTE API
API_START_DATE_LIMITS = {1: datetime.datetime(2014, 12, 15),
                         2: datetime.datetime(2011, 12, 13),
                         3: datetime.datetime(2017, 1, 1)} # See RTE API doc

# Limit end date accepted by the RTE API
API_END_DATE_LIMIT = datetime.datetime.now()

# User date time format useful to the datetime functions
INPUT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Delimiters to format datetimes str specifically used in the format_dates function
FORMAT_DATE_DATETIME_DELIMITERS = {0: {"date_time": "T", "tz": "+01:00"},
                                   1: {"date_time": "_", "tz": ""},
                                   2: {"date_time": " ", "tz": ""}}

# Maximal number of days depending on the ressource requested
RESSOURCES_MAXIMAL_TIME_DELTAS = {1: 155, 2: 7, 3: 14}, # According to RTE API doc

# Time span of one data point depending on the ressource call
RESSOURCES_DATA_POINT_TIME_SPAN = {1: datetime.timedelta(hours = 1),
                                   2: datetime.timedelta(hours = 1),
                                   3: datetime.timedelta(minutes = 15)}

# 2/ Parameters for the functions used in the manage_data_storage and the store_data modules

# Ressources names
RESSOURCES_NAMES = JSON_LVL1_NOMENCLATURE

# Designation of the units names to create the csv path of the units names files
UNITS_NAMES_FILE_PATH_DESIGNATION = {1: "production_type",
                                     2: "unit",
                                     3: "production_type_&_subtype"}

# Default end date for storage in the case a default API call was made
DEFAULT_END_DATE = {1: datetime.timedelta(hours = 23),
                    2: datetime.timedelta(hours = 23),
                    3: datetime.timedelta(hours = 23, minutes = 45)}

# Names of the units names cols used in the register
UNITS_NAMES_COLS = {1: "production_type",
                    2: "eic_code",
                    3: "production_subtype"}

# Initialisation dictionnary of the params cols for the register
PARAMS_COLS_INIT = {"start_date": None,
                    "end_date": None,
                    "eic_code": None,
                    "production_type": None,
                    "production_subtype": None}

# 3/ Parameters for the functions used in the get_data module

# Minimal intervals between two consecutive API calls depending on the ressource requested
RESSOURCES_MINIMAL_CALL_INTERVALS = {1: 900, 2: 3600, 3: 900}

# Ressource key used in the api_delay decorator to adapt wait time depending on the ressource
RESSOURCE_PARAM_NAME = "ressource_nb"
