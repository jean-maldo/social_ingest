import datetime
from typing import List

from pytz import utc


def get_start_list(days: int) -> List[datetime.datetime]:
    """
    Gets a list of start datetimes for the last 7 days.

    Parameters
    ----------
    days
        The number of days to produce datetimes for.

    Returns
    -------
    A list with the starting datetimes
    """
    base = datetime.datetime.now(utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start_list = [(base - datetime.timedelta(days=x)) for x in range(days)]
    start_list.append(
        (datetime.datetime.now(utc) - datetime.timedelta(days=days) + datetime.timedelta(hours=1))
    )
    start_list.reverse()

    return start_list


def get_end_list(days: int):
    """
    Gets a list of end datetimes for the last 7 days.

    Parameters
    ----------
    days
        The number of days to produce datetimes for.

    Returns
    -------
    A list with the ending datetimes
    """
    base = datetime.datetime.now(utc).replace(hour=23, minute=59, second=59, microsecond=999999)
    today_end = (datetime.datetime.now(utc) - datetime.timedelta(minutes=1))
    end_list = [(base - datetime.timedelta(days=x)) for x in range(days + 1)]
    end_list[0] = today_end
    end_list.reverse()

    return end_list
