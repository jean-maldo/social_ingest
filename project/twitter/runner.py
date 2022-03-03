import re

import csv
import dateutil.parser
import pandas as pd
import time

from project.twitter.api_handler import append_config_params, connect_to_endpoint, create_headers
from project.twitter.configuration import config
from project.twitter.endpoint_type import EndpointType
from project.utilities import dates


def append_to_csv(json_response: any, file_name: str) -> int:
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
        res = [author_id, created_at, geo, lat, long, place_name, place_full_name, place_country, place_country_code,
               tweet_id, lang, like_count, quote_count, reply_count, retweet_count, source, text]

        # Append the result to the CSV file
        csv_writer.writerow(res)
        counter += 1

    # When done, close the CSV file
    csv_file.close()

    # Print the number of tweets for this iteration
    print(f"{counter} Tweets added from this response")
    return counter


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
    headers = create_headers()

    # Total number of tweets we collected from the loop
    total_tweets = 0

    # Create file
    csv_file = open(csv_filename, "w", newline="", encoding="utf-8")
    csv_writer = csv.writer(csv_file)

    # Create headers for the data you want to save, in this example
    csv_writer.writerow(
        ["author_id", "created_at", "geo", "lat", "long", "place_name", "place_full_name", "place_country",
         "place_country_code", "id", "lang", "like_count", "quote_count", "reply_count", "retweet_count",
         "source", "tweet"])
    csv_file.close()

    start_list = dates.get_start_list(7)
    end_list = dates.get_end_list(7)

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

            url = config.search_url
            search_params = {
                "query": keyword,
                "start_time": start_list[i],
                "end_time": end_list[i],
                "max_results": max_results
            }

            params = append_config_params(search_params, EndpointType.SEARCH, config)

            json_response = connect_to_endpoint(url, headers, params, next_token)
            result_count = json_response["meta"]["result_count"]

            if "next_token" in json_response["meta"]:
                # Save the token to use for next call
                next_token = json_response["meta"]["next_token"]
                print("Next Token: ", next_token)
                if result_count is not None and result_count > 0 and next_token is not None:
                    print("Start Date: ", start_list[i])
                    tweets_added = append_to_csv(json_response, csv_filename)
                    count += tweets_added
                    total_tweets += result_count
                    print("Total # of Tweets scanned: ", total_tweets)
                    print("Total # of Tweets with Geo data parsed: ", count)
                    print("-------------------")
                    time.sleep(5)
            # If no next token exists
            else:
                if result_count is not None and result_count > 0:
                    print("-------------------")
                    print("Start Date: ", start_list[i])
                    tweets_added = append_to_csv(json_response, csv_filename)
                    count += tweets_added
                    total_tweets += result_count
                    print("Total # of Tweets scanned: ", total_tweets)
                    print("Total # of Tweets with Geo data parsed: ", count)
                    print("-------------------")
                    time.sleep(5)

                # Since this is the final request, turn flag to false to move to the next time period.
                flag = False
                next_token = None
            time.sleep(5)
    print("Total number of results: ", total_tweets)


def get_locations():
    df = pd.read_csv("data/temblor_data.csv", dtype={'author id': object})
    author_ids = df["author id"].to_list()
    author_ids = [x for x in author_ids if re.match(r'^\d+$', x) is not None]

    # Inputs for tweets
    headers = create_headers()

    user_locations = []

    for batch in range((len(author_ids)//100) + 1):
        batch_start = batch*100
        batch_end = batch*100 + 100
        batch_end = len(author_ids) if batch_end > len(author_ids) else batch_end
        print(batch_start, batch_end)

        url = config.users_url
        users_params = {
            "ids": author_ids[batch_start:batch_end]
        }
        params = append_config_params(users_params, EndpointType.USERS, config)
        json_response = connect_to_endpoint(url, headers, params, None)

        for user in json_response["data"]:
            user_locations.append([user.get("id"), user.get("location")])

    df_user_location = pd.DataFrame(user_locations, columns=["Name", "Age"])

    df_user_location.to_csv("user_locations.csv")


def tweets_add_locations():
    df_locations = pd.read_csv("data/locations_clean.csv", dtype={'author_id': object})
    df_temblor = pd.read_csv("data/temblor_data.csv", dtype={'author id': object})
    user_location = {}

    for index, row in df_locations.iterrows():
        user_location[row["author_id"]] = {
            "lat": row["lat"],
            "long": row["long"],
            "city": row["city"],
            "country": row["country"]
        }

    for index, row in df_temblor.iterrows():
        try:
            # print(f"""looking up {row["author id"]}""")
            df_temblor.loc[index, "lat"] = user_location[row["author id"]]["lat"]
            df_temblor.loc[index, "long"] = user_location[row["author id"]]["long"]
            df_temblor.loc[index, "city"] = user_location[row["author id"]]["city"]
            df_temblor.loc[index, "country"] = user_location[row["author id"]]["country"]
            print("added")
        except KeyError:
            pass
            # print(f"""{row["author id"]} not found""")
