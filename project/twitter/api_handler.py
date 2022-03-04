import logging
import os
from typing import Dict

import requests

from twitter.configuration import Configuration
from twitter.endpoint_type import EndpointType

logger = logging.getLogger(__name__)


def auth() -> str:
    """
    Get Environment Variable for the Authentication Token that's required for calling the Twitter API v2

    Returns
    -------
    str
        The Twitter token
    """
    return os.getenv("BEARER_TOKEN")


def create_headers() -> Dict:
    """
    Creates the HTTP headers required to call the Twitter v2 API using the Authentication Bearer Token

    Returns
    -------
    Dict
        A dictionary containing the header information for the HTTP request
    """
    headers = {"Authorization": f"Bearer {auth()}"}
    return headers


def append_config_params(params: Dict, endpoint: EndpointType, config: Configuration) -> Dict:
    """
    Appends the params in the YAML config to the params passed.

    Parameters
    ----------
    params
        The params to append to.
    endpoint
        The Endpoint Type to determine which params to get from the YAML config.
    config
        The YAML config object.

    Returns
    -------
    A dictionary with the concatenated params provided with the YAML params.

    """
    if endpoint.value == EndpointType.SEARCH.value:
        config_params = config.search_params
    elif endpoint.value == EndpointType.USERS.value:
        config_params = config.users_params
    else:
        raise NotImplemented("The Endpoint Type hasn't been implemented")

    return {**config_params, **params}


def connect_to_endpoint(url: str, headers: Dict, params: Dict, next_token: str = None) -> Dict:
    """
    Execute a HTTP Request to the Twitter v2 API and return the JSON response.
    Parameters
    ----------
    url
        The Twitter v2 API Endpoint.
    headers
        The HTTP Headers containing the Authentication token.
    params
        The params dictionary to use for the API endpoint.
    next_token
        The token for the next response page.

    Returns
    -------
    Dict
        The JSON response
    """
    params["next_token"] = next_token   # params object received from create_url function
    response = requests.request("GET", url, headers=headers, params=params)
    logger.info("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

