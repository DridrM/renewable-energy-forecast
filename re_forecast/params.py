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


#####################
# Data utils module #
#####################

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
RESSOURCES_MAXIMAL_TIME_DELTAS = {1: 155, 2: 7, 3: 14} # According to RTE API doc

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
RESSOURCES_MINIMAL_CALL_INTERVALS = {1: 900, 2: 900, 3: 900}

# Ressource key, start date key and end date key
# used in the api_delay decorator to adapt wait time depending on the ressource
RESSOURCE_PARAM_NAME = "ressource_nb"
START_DATE_PARAM_NAME = "start_date"
END_DATE_PARAM_NAME = "end_date"

# Name of the function get rte data, to use in the api_delay decorator
FUNC_NAME_GET_RTE_DATA = "get_rte_data"

# State of the bypass for the api_delay function. If False, the delay is applied. If false, it is not applied
API_DELAY_BYPASS = True


#############################
# Preprocessing all modules #
#############################

# Names of the datetime columns to evauate
DATE_TIME_COLUMNS = ["start_date", "end_date", "updated_date"]


###########################################
# Preprocessing Check data quality module #
###########################################

# Quality thresholds that the time serie dataset must respects, the name of their check function
# and their message (why they don't respect the quality check)
DATA_QUALITY_THRESHOLDS = {"row_nb": (1000, "check_nb_row", "Unsufficient number of rows"),
                           "prop_missing_values": (0.3, "check_missing_values_prop", "Too many missing values in proportion"),
                           "max_empty_gap_duration": (50, "check_max_empty_gap_duration", "Max empty values gap duration above 50 hours")}


##############################
# Preprocessing Clean values #
##############################

# Columns to keep for the peeled_df function
PEELED_DF_KEEPED_COLUMNS = {"dt_column": "start_date_complete", "value_column": "value"}


#################################
# Preprocessing preprocess data #
#################################

# Name of the value column of the time serie df
VALUE_COL_NAME = "value"

# Minimum and maximum bound values for the time serie df
MIN_MAX_BOUND_VALUES = {"min_value": 0, "max_value": None}

# Parameters of the KNN imputation of missing values
KNN_IMPUTATION_MISSING_VALUES = {"param": 5, "nb_supervised_features": 24}


####################################
# Training train test split module #
####################################

# Names of the split values arguments
SPLIT_VALUES_ARGS_NAMES = ("train_split", "test_split")

# Bound values for the train test split proportions
TRAIN_TEST_SPLIT_PROP_BOUNDS = (0, 1)
