# Twitter Ingest
Sample code to ingest data from the Twitter v2 API

## Setup
Install the requirements using the following command in terminal

```pip install -r requirements.txt```

The code uses the Twitter Bearer Token for Authentication. This token should be exported to as an Env var 
with the name ```BEARER_TOKEN``` e.g.

```export BEARER_TOKEN=my-twitter-token```

## Usage
The main function runs the ```loop_search``` which starts the twitter ingestion process. 
This function takes 4 arguments:
* keyword - this is the Twitter API query
* max_results - this is the max number of API results to return
* max_count - this is the max results per day
* csv_filename - the file name to save the results to
