import datetime
from typing import Dict

import pytz

CASE_SENSITIVE_FIELD_MAP = {
    # Conflicting field --> Field with `_` as a prefix
    "Industry": "_industry",
    "offers.Intro APR": "_offers.intro APR",
}


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


def transform_case_sensitive_fields(record):
    """Function to transform case-sensitive fields to be prefixed with `_`."""

    for field in CASE_SENSITIVE_FIELD_MAP:
        if field in record:
            record[CASE_SENSITIVE_FIELD_MAP[field]] = record.pop(field)
    return record
