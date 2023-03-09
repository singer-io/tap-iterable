import datetime

import pytz


def epoch_to_datetime_string(milliseconds):
    """Function to convert epoch time to datetime """
    datetime_string = None
    try:
        datetime_string = datetime.datetime.fromtimestamp(milliseconds / 1000.0, pytz.timezone("UTC")).strftime(
            '%Y-%m-%d %H:%M:%S.%f')
    except TypeError:
        # If fails, it means format already datetime string.
        datetime_string = milliseconds
    return datetime_string
