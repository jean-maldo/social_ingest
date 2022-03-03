import argparse

from project.utilities.twitter.runner import loop_search, get_locations

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Python Twitter v2 API to CSV")
    parser.add_argument('--search', action='store_true')
    parser.add_argument('--user_location', action='store_true')

    args = parser.parse_args()

    if args.search:
        loop_search(
            keyword="earthquake lang:en",
            max_results=100,
            max_count=1000,
            csv_filename="project/data/earthquake_data_test.csv"
        )
    elif args.user_location:
        get_locations()
