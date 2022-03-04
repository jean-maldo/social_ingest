import logging
import re
from typing import List, Dict

import pandas as pd
import unicodedata

from project.utilities.world_cities import WorldCities

logger = logging.getLogger(__name__)


def _get_country_city(countries: List, location: str) -> (str, str):
    """
    Parse city and country from location field by checking if a reference country is in the location provided
    and pulling it out.

    Parameters
    ----------
    countries
        List of countries to loop through
    location
        The location string to parse
    """
    city_lookup = strip_accents(location.lower().strip())
    country_lookup = ""

    for country in countries:
        country_clean = country.lower().strip()
        if country_clean in city_lookup:
            city_lookup = city_lookup.replace(country_clean, "")
            country_lookup = country_clean

    return country_lookup, re.sub(r'[^\w\s]', "", city_lookup)


def clean_locations(user_locations: pd.DataFrame, world_cities_file: str) -> Dict:
    """
    Loop through locations to extract and clean the country and city fields as well as lat long values using
    a world_cities file as a reference table.

    Parameters
    ----------
    user_locations
        The user locations DataFrame to get location data for.
    world_cities_file
        The reference table to use for country, city, and lat long values.
    """
    df_locations = user_locations.dropna()

    wc = WorldCities(world_cities_file)
    country_lookup, city_lookup = _get_country_city(wc.countries, df_locations["location"])

    user_location = {}
    for index, row in df_locations.iterrows():
        country_cities_lookup = wc.country_city_ref
        try:
            lookup_value = country_lookup.strip() + city_lookup.strip()
            user_location[row["author_id"]] = {
                "lat": country_cities_lookup[lookup_value]["lat"],
                "long": country_cities_lookup[lookup_value]["long"],
                "city": city_lookup,
                "country": country_lookup
            }
        except KeyError:
            logger.warning(f"{country_lookup} and {city_lookup} not found")
    return user_location


def strip_accents(string_value: str) -> str:
    """
    Replace accented characters e.g. é with e

    Parameters
    ----------
    string_value
        The string to replace.

    Returns
    -------
    The string without accented characters.

    """
    return ''.join(c for c in unicodedata.normalize('NFD', string_value)
                   if unicodedata.category(c) != 'Mn')
