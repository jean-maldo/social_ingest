import datetime
import os
from typing import Dict, Tuple

import csv
import dateutil.parser
from pytz import utc
import requests
import time


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
                    "place.fields": "full_name,id,country,country_code,geo,name,place_type",
                    "next_token": {}}
    return search_url, query_params


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


def append_to_csv(json_response: any, file_name: str):
    """
    Parses the JSON Twitter API response and writes the results to a file.

    Parameters
    ----------
    json_response
        The JSON response from the Twitter v2 API.
    file_name
        The name of the CSV file to append results to.
    """
    # A counter variable
    counter = 0

    # Open OR create the target CSV file
    csv_file = open(file_name, "a", newline="", encoding="utf-8")
    csv_writer = csv.writer(csv_file)

    # Loop through each tweet
    for tweet in json_response.get("data"):

        # 1. Author ID
        author_id = tweet.get("author_id")

        # 2. Time created
        created_at = dateutil.parser.parse(tweet.get("created_at"))

        # 3. Geolocation
        try:
            geo = tweet.get("geo")
            geo = geo.get("place_id")
        except AttributeError:
            geo = None

        # 4. Tweet ID
        tweet_id = tweet.get("id")

        # 5. Language
        lang = tweet.get("lang")

        # 6. Tweet metrics
        retweet_count = tweet.get("public_metrics").get("retweet_count")
        reply_count = tweet.get("public_metrics").get("reply_count")
        like_count = tweet.get("public_metrics").get("like_count")
        quote_count = tweet.get("public_metrics").get("quote_count")

        # 7. source
        source = tweet.get("source")

        # 8. Tweet text
        text = tweet.get("text")

        # Assemble all data in a list
        res = [author_id, created_at, geo, tweet_id, lang, like_count, quote_count, reply_count, retweet_count, source,
               text]

        # Append the result to the CSV file
        csv_writer.writerow(res)
        counter += 1

    # When done, close the CSV file
    csv_file.close()

    # Print the number of tweets for this iteration
    print(f"{counter} Tweets added from this response")


def loop_search(
        keyword: str = "temblor lang:es",
        max_results: int = 100,
        max_count: int = 1000,
        csv_filename: str = "temblor_data_test.csv"
):
    """
    Generates the start and end date lists for the last 7 days. Loops through every day in the list to get the defined
    number of tweets per day. Creates/overwrite CSV file at start of run and then appends API results to the CSV file.
    Adds a 5 second delay between API calls to not spam the API.

    Parameters
    ----------
    keyword
        The search query for the Twitter v2 API. See API docs for more info on building search queries:
        https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query
    max_results
        Set max results per page for the API response
    max_count
        Max tweets per time period
    csv_filename
        The name of the file to write results to
    """
    # Inputs for tweets
    bearer_token = auth()
    headers = create_headers(bearer_token)
    base = datetime.datetime.now(utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start_list = [(base - datetime.timedelta(days=x)).strftime("%Y-%m-%dT%H:%M:%S.000Z") for x in range(7)]
    start_list.append(
        (datetime.datetime.now(utc) - datetime.timedelta(days=7) + datetime.timedelta(hours=1))
        .strftime("%Y-%m-%dT%H:%M:%S.000Z")
    )
    start_list.reverse()

    base = datetime.datetime.now(utc).replace(hour=23, minute=59, second=59, microsecond=999999)
    today_end = (datetime.datetime.now(utc) - datetime.timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_list = [(base - datetime.timedelta(days=x)).strftime("%Y-%m-%dT%H:%M:%S.000Z") for x in range(8)]
    end_list[0] = today_end
    end_list.reverse()

    # Total number of tweets we collected from the loop
    total_tweets = 0

    # Create file
    csv_file = open(csv_filename, "w", newline="", encoding="utf-8")
    csv_writer = csv.writer(csv_file)

    # Create headers for the data you want to save, in this example
    csv_writer.writerow(
        ["author_id", "created_at", "geo", "id", "lang", "like_count", "quote_count", "reply_count", "retweet_count",
         "source", "tweet"])
    csv_file.close()

    for i in range(0, len(start_list)):

        # Inputs
        count = 0  # Counting tweets per time period
        flag = True
        next_token = None

        while flag:
            # Check if max_count reached
            if count >= max_count:
                break
            print("-------------------")
            print("Token: ", next_token)
            url, params = create_url(keyword, start_list[i], end_list[i], max_results)
            json_response = connect_to_endpoint(url, headers, params, next_token)
            result_count = json_response["meta"]["result_count"]

            if "next_token" in json_response["meta"]:
                # Save the token to use for next call
                next_token = json_response["meta"]["next_token"]
                print("Next Token: ", next_token)
                if result_count is not None and result_count > 0 and next_token is not None:
                    print("Start Date: ", start_list[i])
                    append_to_csv(json_response, csv_filename)
                    count += result_count
                    total_tweets += result_count
                    print(f"Added {total_tweets} Tweets")
                    print("-------------------")
                    time.sleep(5)
            # If no next token exists
            else:
                if result_count is not None and result_count > 0:
                    print("-------------------")
                    print("Start Date: ", start_list[i])
                    append_to_csv(json_response, csv_filename)
                    count += result_count
                    total_tweets += result_count
                    print("Total # of Tweets added: ", total_tweets)
                    print("-------------------")
                    time.sleep(5)

                # Since this is the final request, turn flag to false to move to the next time period.
                flag = False
                next_token = None
            time.sleep(5)
    print("Total number of results: ", total_tweets)


if __name__ == "__main__":
    loop_search(
        keyword="terremoto lang:es",
        max_results=100,
        max_count=1000,
        csv_filename="terremoto_data_test.csv"
    )
