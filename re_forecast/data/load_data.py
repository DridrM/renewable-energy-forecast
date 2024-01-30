import requests


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


# def get_eic_code()


# def get_prod_type()


# def get_rte_data()
