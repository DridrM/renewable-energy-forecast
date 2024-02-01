import requests
from re_forecast.params import BASE_URL, RESSOURCE_AUTH, RESSOURCE_1, RESSOURCE_2, RESSOURCE_3, CONTENT_TYPE, CLIENT_SECRET
from re_forecast.data.utils import handle_params


def collect_rte_token(base_url: str,
                  ressource: str,
                  content_type: str,
                  client_secret: str) -> dict:
    """Query the (RTE) API with POST to collect an access token"""

    # Construct the url
    url = "{}{}".format(base_url, ressource)

    # Construct the headers dict
    headers = {"Authorization": f"Basic {client_secret}",
               "content-type": content_type}

    # Query the API with the post verb
    response = requests.post(url, headers = headers)

    # Return the JSON of the response
    return response.json()


def query_rte_api(token_infos: dict,
                  base_url: str,
                  ressource: str,
                  params = None) -> dict:
    """Query the RTE API with get to collect energy production data"""

    # Construct the url
    url = "{}{}".format(base_url, ressource)

    # Extract the token infos
    access_token = token_infos["access_token"]
    token_type = token_infos["token_type"]

    # Construct the headers dict
    headers = {"Authorization": f"{token_type} {access_token}"}

    # If the params dict is provided query with the params dict
    if params:
        response = requests.get(url, params = params, headers = headers)

    # Otherwise query without params
    else:
        response = requests.get(url, headers = headers)

    # Return the datas
    return response.json()


def get_rte_data(ressource_nb: int,
                 start_date = None,
                 end_date = None,
                 eic_code = None,
                 prod_type = None,
                 ressources_urls = {1: RESSOURCE_1,
                                    2: RESSOURCE_2,
                                    3: RESSOURCE_3}
                 ) -> dict:
    """Pack together the token collection, the params handling (including
    hangling presence, time limits and formating) and the final RTE API query.
    Notes:
    - For the dates, please use this format: 'YYYY-MM-DDThh:mm:ss'
    - For the eic code and the prod type, please refer to the dates (for now)
    """

    # Collect the rte access token
    token_infos = collect_rte_token(BASE_URL,
                                    RESSOURCE_AUTH,
                                    CONTENT_TYPE,
                                    CLIENT_SECRET)

    # Set the ressource given the ressource number
    ressource = ressources_urls[ressource_nb]

    # Set the params given the ressource number
    params = handle_params(ressource_nb,
                           start_date,
                           end_date,
                           eic_code,
                           prod_type)

    # Query the RTE API with ressource number given
    data = query_rte_api(token_infos,
                         BASE_URL,
                         ressource,
                         params = params)

    return data
