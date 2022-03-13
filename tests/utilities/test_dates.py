import datetime

from project.utilities.dates import get_end_list, get_start_list

def test_get_start_list():
    result_start_dates = get_start_list(3, datetime.datetime(2022, 3, 13))
    assert result_start_dates == [
        datetime.datetime(2022, 3, 13),
        datetime.datetime(2022, 3, 12),
        datetime.datetime(2022, 3, 11)
    ]

def test_get_end_list():
    result_start_dates = get_end_list(3, datetime.datetime(2022, 3, 13, 23, 59, 59, 999999))
    assert result_start_dates == [
        datetime.datetime(2022, 3, 13, 23, 59, 59, 999999),
        datetime.datetime(2022, 3, 12, 23, 59, 59, 999999),
        datetime.datetime(2022, 3, 11, 23, 59, 59, 999999)
    ]
