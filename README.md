# Twitter Ingest
This project started off as an implementation of the following 
[Toward Data Science](https://towardsdatascience.com/an-extensive-guide-to-collecting-tweets-from-twitter-api-v2-for-academic-research-using-python-3-518fcb71df2a)
article to ingest data from the Twitter v2 API.

It has now been tuned into a more Data Engineering focused data ingestion project that can call 2 endpoints with scope 
to easily add more endpoints.

## Setup
From a clean Python (virtual) environment add the project root to your PYTHONPATH e.g. below 
command for Unix based env:

```export PYTHONPATH=$(pwd)  # (bash shells)```

Install the requirements using the following command in terminal

```pip install -r requirements.txt```

The code uses the Twitter Bearer Token for Authentication. This token should be exported as an Env var 
with the name ```BEARER_TOKEN``` e.g.

```export BEARER_TOKEN=my-twitter-token```

## Usage
Currently, the code can be run with 2 commands in the CLI for 2 Twitter endpoints:
* search
* add-locations

The `search` command will hit the Twitter search API to retrieve tweets. The `add-locations` command 
will hit the Twitter users API to retrieve location data and append to the existing file with 
twitter data.

You can run the following to get info on the options that can be passed to execute the above two commands.

```python project/main.py search --help```

A sample command run for the search API is:

```python project/main.py search --query "earthquake lang:en" --filename tweets_test --days 3```
