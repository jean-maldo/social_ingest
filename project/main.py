import argparse
import logging

from project.twitter.runner import search_tweets, tweets_add_locations

if __name__ == "__main__":
    logging_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=logging_format
    )

    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Python Twitter v2 API to CSV")

    sub_parser = parser.add_subparsers(dest='command')
    search = sub_parser.add_parser('search', help="Hit the Twitter search API")
    add_locations = sub_parser.add_parser('add-locations', help="Hit the Twitter users API")

    search.add_argument('--query', type=str, required=True, help="The search query for the Twitter v2 API")
    search.add_argument('--max-results', type=int, required=False, default=100, help="Set max results per page")
    search.add_argument('--max-count', type=int, required=False, default=1000, help="Max tweets per time period")
    search.add_argument('--filename', type=str, required=True, help="File name to write results to (no extension)")
    search.add_argument('--days', type=int, required=False, default=7, help="Max days to get data for")

    add_locations.add_argument('--filename', type=str, required=True, help="File name to update (no extension)")

    args = parser.parse_args()

    if args.command == 'search':
        logger.info(f"Search Args: {args}")
        search_tweets(
            keyword=args.query,
            max_results=args.max_results,
            max_count=args.max_count,
            csv_filename=args.filename,
            days=args.days
        )
    elif args.command == 'add_locations':
        logger.info(f"Users Args: {args}")
        tweets_add_locations(tweet_data_file="")
