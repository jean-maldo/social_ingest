import pandas as pd

from utilities.transformers import clean_locations, strip_accents


def test_clean_locations():
    df_locations = pd.DataFrame([["1", "camas, spain"],
                                 ["2", "United states - Meredith"],
                                 ["3", "Nackenheim (Germany)"]],
                                columns=["author_id", "location"])

    result_dict = clean_locations(df_locations, "tests/utilities/mock/sample_world_cities.csv")

    expected_dict = {"1": {"lat": 37.4020, "lon": -6.0332, "city": "camas", "country": "spain"},
                     "2": {"lat": 43.6301, "lon": -71.5018, "city": "meredith", "country": "united states"},
                     "3": {"lat": 49.9153, "lon": 8.3389, "city": "nackenheim", "country": "germany"}}
    assert  result_dict == expected_dict

def test_strip_accents():
    accents_list = ["También", "hôtel", "Löwe"]

    result = [strip_accents(w) for w in accents_list]

    assert result == ["Tambien", "hotel", "Lowe"]
