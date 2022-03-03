import os
from typing import Dict, List, Tuple

import requests


def auth() -> str:
    """
    Get Environment Variable for the Authentication Token that's required for calling the Twitter API v2

    Returns
    -------
    str
        The Twitter token
    """
    return os.getenv("BEARER_TOKEN")


def create_headers(bearer_token: str) -> Dict:
    """
    Creates the HTTP headers required to call the Twitter v2 API using the Authentication Bearer Token

    Parameters
    ----------
    bearer_token
        The Twitter Authentication token

    Returns
    -------
    Dict
        A dictionary containing the header information for the HTTP request
    """
    headers = {"Authorization": f"Bearer {bearer_token}"}
    return headers


def create_url(keyword: str, start_date: str, end_date: str, max_results: int = 10) -> Tuple[str, Dict[str, str]]:
    """
    Creates a URL endpoint to get recent tweets from the Twitter v2 API.

    Parameters
    ----------
    keyword
        The search query
    start_date
        Date to get tweets from. Must be a maximum of 7 days ago.
    end_date
        Date to get tweets until. Must be less than the current date time.
    max_results
        The maximum number of results for pagination.

    Returns
    -------
    str
        URL Endpoint for the Twitter v2 API
    """
    search_url = "https://api.twitter.com/2/tweets/search/recent"

    query_params = {"query": keyword,
                    "start_time": start_date,
                    "end_time": end_date,
                    "max_results": max_results,
                    "expansions": "author_id,in_reply_to_user_id,geo.place_id",
                    "tweet.fields": ("id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,"
                                     "public_metrics,referenced_tweets,reply_settings,source"),
                    "user.fields": "id,name,username,created_at,description,public_metrics,verified",
                    "place.fields": "full_name,id,country,country_code,geo,name,place_type,contained_within",
                    "next_token": {}}
    return search_url, query_params


def create_users_url(author_id_list: List):
    # Specify the usernames that you want to lookup below
    # You can enter up to 100 comma-separated values.
    author_ids = ",".join(author_id_list)
    # User fields are adjustable, options include:
    # created_at, description, entities, id, location, name,
    # pinned_tweet_id, profile_image_url, protected,
    # public_metrics, url, username, verified, and withheld
    url = f"https://api.twitter.com/2/users"

    query_params = {"ids": author_ids,
                    "expansions": "pinned_tweet_id",
                    "user.fields": "created_at,description,entities,id,location,name,pinned_tweet_id,"
                                   "profile_image_url,protected,url,username,verified,withheld"
                    }
    return url, query_params


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
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

