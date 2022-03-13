from utilities.world_cities import WorldCities


def test_build_lookup():
    country_city_ref = WorldCities("tests/utilities/mock/sample_world_cities.csv")
    assert country_city_ref.countries == {"china", "egypt", "morocco", "spain", "namibia", "ireland", "united states",
                                          "france", "new zealand", "germany"}
    assert country_city_ref.country_city_ref == {"chinabeijing": {"lat": 39.9050,"lon": 116.3914},
                                                 "egyptcairo": {"lat": 30.0561,"lon": 31.2394},
                                                 "moroccotahla": {"lat": 34.0476,"lon": -4.4289},
                                                 "spaincamas": {"lat": 37.4020,"lon": -6.0332},
                                                 "namibiaotjiwarongo": {"lat": -20.4642,"lon": 16.6528},
                                                 "irelandtralee": {"lat": 52.2675,"lon": -9.6962},
                                                 "united statesmeredith": {"lat": 43.6301,"lon": -71.5018},
                                                 "francefenain": {"lat": 50.3658,"lon": 3.3006},
                                                 "new zealandwarkworth": {"lat": -36.4000,"lon": 174.6667},
                                                 "germanynackenheim": {"lat": 49.9153,"lon": 8.3389}}
