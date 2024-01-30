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
