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
    s = stream(client)

    # If stream is `users`, then get dynamic fields via API.
    if s.name == "users":
      res = s.client.get_user_fields()
      schema = convert_to_schema(res["fields"])
    else:
      schema = s.load_schema()

    streams.append({'stream': s.name, 'tap_stream_id': s.name, 'schema': schema, 'metadata': s.load_metadata(schema)})
  return streams


def get_field_type_schema(field_type):
    if field_type == "boolean":
        return {"type": ["null", "boolean"]}

    if field_type == "date":
        return {"type": ["null", "string"], "format": "date-time"}

    if field_type == "long":
      return {"type": ["null", "integer"]}

    if field_type == "double":
        return {"type": ["null", "number"]}

    if field_type == "object":
        return {"anyOf": [{"type": ["null", "object"]}, {"type": ["null", "array"], "items": {"type": ["null", "object"]}}]}

    return {"type": ["null", "string"]}


def convert_to_schema(fields):
    schema = {
      "type": ["null", "object"],
      "properties": {}
    }
    for field_name, field_type in fields.items():
        schema['properties'][field_name] = get_field_type_schema(field_type)
    return schema

