import logging
import re
from typing import List

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


def clean_locations(user_locations_file: str, world_cities_file: str) -> pd.DataFrame:
    """
    Loop through locations to extract and clean the country and city fields as well as lat long values using
    a world_cities file as a reference table.

    Parameters
    ----------
    user_locations_file
        The user locations file to get location data for.
    world_cities_file
        The reference table to use for country, city, and lat long values.
    """
    df = pd.read_csv(user_locations_file)
    df_locations = df.dropna()

    wc = WorldCities(world_cities_file)
    country_lookup, city_lookup = _get_country_city(wc.countries, df["location"])

    for index, row in df_locations.iterrows():
        country_cities_lookup = wc.country_city_ref
        try:
            lookup_value = country_lookup.strip() + city_lookup.strip()
            df_locations.loc[index, "lat"] = country_cities_lookup[lookup_value]["lat"]
            df_locations.loc[index, "long"] = country_cities_lookup[lookup_value]["long"]
            df_locations.loc[index, "city"] = city_lookup
            df_locations.loc[index, "country"] = country_lookup
        except KeyError:
            logger.warning(f"{country_lookup} and {city_lookup} not found")
    return df_locations


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
