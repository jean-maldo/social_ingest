import pandas as pd


class WorldCities:

    def __init__(self, file_path):
        self.df_cities = pd.read_csv(file_path)
        self._clean_country_city()
        self._build_country_city_ref()
        self.countries = set(self.df_cities["country"].to_list())
        self.country_city_ref = self._build_country_city_ref()

    def _clean_country_city(self):
        self.df_cities['country'] = self.df_cities['country'].str.lower()
        self.df_cities['city_ascii'] = self.df_cities['city_ascii'].str.lower()

    def _build_country_city_ref(self):
        cities_lookup = {}
        for index, row in self.df_cities.iterrows():
            cities_lookup[row["country"] + row["city_ascii"]] = {
                "lat": row["lat"],
                "long": row["lng"]
            }
        return cities_lookup
