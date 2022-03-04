import argparse

from project.twitter.runner import loop_search, tweets_add_locations

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Python Twitter v2 API to CSV")
    parser.add_argument('--search', action='store_true')
    parser.add_argument('--add-locations', action='store_true')

    args = parser.parse_args()

    if args.search:
        loop_search(
            keyword="earthquake lang:en",
            max_results=100,
            max_count=1000,
            csv_filename="project/data/earthquake_data_test.csv"
        )
    elif args.add_locations:
        tweets_add_locations()
