#!/usr/bin/env python3

#
# Module dependencies.
#

import json
import sys
import singer
from singer import metadata
from tap_iterable.iterable import Iterable
from tap_iterable.discover import discover_streams
from tap_iterable.sync import sync
from tap_iterable.context import Context


logger = singer.get_logger()


REQUIRED_CONFIG_KEYS = [
    "api_key",
    "start_date",
    "api_window_in_days"
]


def discover(client):
    logger.info("Starting discover")
    catalog = {"streams": discover_streams(client)}
    json.dump(catalog, sys.stdout, indent=2)
    logger.info("Finished discover")

@singer.utils.handle_top_exception(logger)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    creds = {
        "start_date": parsed_args.config['start_date'],
        "api_key": parsed_args.config['api_key'],
        "api_window_in_days": parsed_args.config['api_window_in_days']
    }

    client = Iterable(**creds)
    Context.config = parsed_args.config

    if parsed_args.discover:
        discover(client)
    elif parsed_args.catalog:
        state = parsed_args.state or {}
        sync(client, parsed_args.catalog, state)
