#
# Module dependencies.
#

import os
import json
import singer
import sys
from tap_iterable.streams import STREAMS


def discover_streams(client):
  streams = []

  for stream in STREAMS.values():
    stream_client = stream(client)
    stream_schema = stream_client.load_schema()
    streams.append({
      'stream': stream_client.name,
      'tap_stream_id': stream_client.name,
      'schema': stream_schema,
      'metadata': stream_client.load_metadata(stream_schema)
      }
    )

  return streams
