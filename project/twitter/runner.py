import logging
import re
from typing import Dict, List

import dateutil.parser
import pandas as pd
import time

from project.twitter.api_handler import append_config_params, connect_to_endpoint, create_headers
from twitter.config.configuration import config
from project.twitter.endpoint_type import EndpointType
from project.utilities import dates
from project.utilities.transformers import clean_locations

logger = logging.getLogger(__name__)


def append_to_csv(df_headers: List, json_response: any) -> (int, pd.DataFrame):
    """
    Parses the JSON Twitter API response, filters for tweets that have location data, and returns a DataFrame
    with the tweets that do.

    Parameters
    ----------
    df_headers
        The headers to use for the DataFrame
    json_response
        The JSON response from the Twitter v2 API.
    """
    # A counter variable
    counter = 0

    tweets = []
    # Loop through each tweet
    for tweet in json_response.get("data"):

        # 1. Author ID
        author_id = tweet.get("author_id")

        # 2. Time created
        created_at = dateutil.parser.parse(tweet.get("created_at"))

        # 3. Geolocation
        if "geo" in tweet.keys():
            geo = tweet.get("geo").get("place_id")
            try:
                lat = tweet.get("geo").get("coordinates").get("coordinates")[0]
                long = tweet.get("geo").get("coordinates").get("coordinates")[1]
            except AttributeError:
                continue

            place_name = None
            place_full_name = None
            place_country = None
            place_country_code = None

            includes = json_response.get("includes")
            if "places" in includes.keys():
                for place in includes.get("places"):
                    if place["id"] == geo:
                        place_name = place.get("name")
                        place_full_name = place.get("full_name")
                        place_country = place.get("country")
                        place_country_code = place.get("country_code")
        else:
            continue

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
        tweets.append([author_id, created_at, geo, lat, long, place_name, place_full_name, place_country,
                       place_country_code, tweet_id, lang, like_count, quote_count, reply_count, retweet_count,
                       source, text])

        # Append the result to the CSV file
        counter += 1

    try:
        df_tweets = pd.DataFrame(tweets, columns=df_headers)
        logger.info(f"{counter} Tweets added from this response")
    except ValueError as e:
        logger.error(e)
        df_tweets = pd.DataFrame(columns=df_headers)
    return counter, df_tweets


def search_tweets(
        keyword: str,
        csv_filename:  str,
        max_results: int = 100,
        max_count: int = 1000,
        days: int = 7
):
    """
    Loops through every day in the dates lists to get the defined number of tweets per day.
    Builds a DataFrame with results and writes 1 CSVs file for every day in the dates list.
    Adds a 2 second delay between API calls to not spam the API.

    Parameters
    ----------
    keyword
        The search query for the Twitter v2 API. See API docs for more info on building search queries:
        https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query
    csv_filename
        The name of the file to write results to.
    max_results
        Set max results per page for the API response.
    max_count
        Max tweets per time period.
    days
        The number of days from today to search tweets for.
    """
    # Total number of tweets we collected from the loop
    total_tweets = 0

    # Define DataFrame
    df_headers = ["author_id", "created_at", "geo", "lat", "long", "place_name", "place_full_name", "place_country",
                  "place_country_code", "id", "lang", "like_count", "quote_count", "reply_count", "retweet_count",
                  "source", "tweet"]

    start_list = dates.get_start_list(days)
    end_list = dates.get_end_list(days)

    for i in range(0, len(start_list)):

        # Inputs
        count = 0  # Counting tweets per time period
        flag = True
        next_token = None
        df_user_location = pd.DataFrame(columns=df_headers)

        while flag:
            # Check if max_count reached
            if count >= max_count:
                break
            logger.info(f"Token: {next_token}")

            url = config.search_url
            start_date = start_list[i].strftime("%Y-%m-%dT%H:%M:%S.000Z")
            end_date = end_list[i].strftime("%Y-%m-%dT%H:%M:%S.000Z")
            search_params = {
                "query": keyword,
                "start_time": start_date,
                "end_time": end_date,
                "max_results": max_results
            }

            params = append_config_params(search_params, EndpointType.SEARCH, config)
            json_response = connect_to_endpoint(url, create_headers(), params, next_token)
            result_count = json_response["meta"]["result_count"]

            if "next_token" in json_response["meta"]:
                # Save the token to use for next call
                next_token = json_response["meta"]["next_token"]
                logger.info(f"Next Token: {next_token}")
                if result_count is not None and result_count > 0 and next_token is not None:
                    logger.info(f"Start Date: {start_date}")
                    logger.info(f"End Date: {end_date}")
                    tweets_added, tweets_extracted = append_to_csv(df_headers, json_response)
                    df_user_location = pd.concat([df_user_location, tweets_extracted], ignore_index=True)
                    count += tweets_added
                    total_tweets += result_count
                    logger.info(f"Total # of Tweets scanned: {total_tweets}")
                    logger.info(f"Total # of Tweets with Geo data parsed: {count}")
                    time.sleep(2)
            # If no next token exists
            else:
                if result_count is not None and result_count > 0:
                    logger.info(f"Start Date: {start_date}")
                    logger.info(f"End Date: {end_date}")
                    tweets_added, tweets_extracted = append_to_csv(df_headers, json_response)
                    df_user_location = pd.concat([df_user_location, tweets_extracted], ignore_index=True)
                    count += tweets_added
                    total_tweets += result_count
                    logger.info(f"Total # of Tweets scanned: {total_tweets}")
                    logger.info(f"Total # of Tweets with Geo data parsed: {count}")
                    time.sleep(2)

                # Since this is the final request, turn flag to false to move to the next time period.
                flag = False
                next_token = None
            time.sleep(2)
        date_format = end_list[i].strftime("%Y%m%d")
        filename = f"project/data/{date_format}_{csv_filename}.csv"
        logger.info(f"Writing to {filename}")
        df_user_location.to_csv(filename)
    logger.info(f"Total number of results: {total_tweets}")


def get_author_locations(tweet_data_file: str) -> Dict:
    """
    Hits the twitter users API Endpoint to get location data to build a lookup dictionary. Uses a world cities
    reference file to validate country/cities.

    Parameters
    ----------
    tweet_data_file
        The file with twitter data that contains author_id column to get location data for.

    Returns
    -------
    A dictionary to be used as a lookup for author_id -> location data

    """
    df = pd.read_csv(tweet_data_file, dtype={'author id': object})
    author_ids = df["author id"].to_list()
    author_ids = [_id for _id in author_ids if re.match(r'^\d+$', _id) is not None]

    # Inputs for tweets
    headers = create_headers()

    user_locations = []

    for batch in range((len(author_ids)//100) + 1):
        batch_start = batch * 100
        batch_end = batch * 100 + 100
        batch_end = len(author_ids) if batch_end > len(author_ids) else batch_end
        logger.info(f"Batch {batch_start} to {batch_end}")

        url = config.users_url
        users_params = {
            "ids": author_ids[batch_start:batch_end]
        }
        params = append_config_params(users_params, EndpointType.USERS, config)
        json_response = connect_to_endpoint(url, headers, params, None)

        for user in json_response["data"]:
            user_locations.append([user.get("id"), user.get("location")])

    df_user_location = pd.DataFrame(user_locations, columns=["author_id", "location"])
    return clean_locations(df_user_location, "project/utilities/reference/world_cities.csv")


def tweets_add_locations(tweet_data_file: str = "project/data/tweet_data.csv"):
    """
    Calls the get_author_locations() function to build the lookup dictionary and then updates the provided
    file with location data.

    Parameters
    ----------
    tweet_data_file
        The file with twitter data that has author_id column.
    """
    df_tweets = pd.read_csv(tweet_data_file, dtype={'author id': object})

    user_location = get_author_locations(tweet_data_file)

    for index, row in df_tweets.iterrows():
        try:
            logger.debug(f"""looking up {row["author id"]}""")
            df_tweets.loc[index, "lat"] = user_location[row["author_id"]]["lat"]
            df_tweets.loc[index, "long"] = user_location[row["author_id"]]["long"]
            df_tweets.loc[index, "city"] = user_location[row["author_id"]]["city"]
            df_tweets.loc[index, "country"] = user_location[row["author_id"]]["country"]
        except KeyError:
            pass
            logger.warning(f"""{row["author id"]} not found in locations lookup""")
    df_tweets.to_csv(tweet_data_file)
