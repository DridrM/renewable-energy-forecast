import os

########################
# Connexion to RTE API #
########################

# Base url for RTE API's ressources
BASE_URL = os.environ.get("BASE_URL")

# Ressource 1: Authorization
RESSOURCE_1 = os.environ.get("RESSOURCE_1")
# Header for the ressource 1 query
CONTENT_TYPE = os.environ.get("CONTENT_TYPE")

# Ressource 2: Generation per production type data
RESSOURCE_2 = os.environ.get("RESSOURCE_2")

# Client secret for the data ressources query
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
