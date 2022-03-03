import logging
import os
import yaml

logger = logging.getLogger(__name__)


class Configuration:
    def __init__(self):
        print(os.getcwd())
        with open("project/twitter/configuration.yaml", "r") as f:
            conf = yaml.safe_load(f)

        self._conf = conf
        try:
            self.search_url = conf["endpoints"]["search"]["url"]
        except KeyError:
            logger.error("The search url string is missing from the config YAML")
            raise

        try:
            self.users_url = conf["endpoints"]["users"]["url"]
        except KeyError:
            logger.error("The search url string is missing from the config YAML")
            raise

        self._conf = conf
        try:
            self.search_params = conf["endpoints"]["search"]["params"]
        except KeyError:
            logger.info("No params for search endpoint; default empty Dict")
            self.search_params = {}

        self._conf = conf
        try:
            self.users_params = conf["endpoints"]["users"]["params"]
        except KeyError:
            logger.info("No params for users endpoint; default empty Dict")
            self.users_params = {}


config = Configuration()
