#
# Module dependencies.
#

import json
import singer
import singer.metrics as metrics
from singer import metadata
from singer import Transformer

LOGGER = singer.get_logger()

def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)

def get_selected_streams(catalog):
    selected_stream_names = []
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        if stream_is_selected(mdata):
            selected_stream_names.append(stream.tap_stream_id)
    return selected_stream_names

def sync(client, catalog, state):
    selected_stream_names = get_selected_streams(catalog)

    for stream in catalog.streams:
        stream_name = stream.tap_stream_id

        mdata = metadata.to_map(stream.metadata)

        if stream_name not in selected_stream_names:
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream.schema.to_dict(), key_properties)
        LOGGER.info("%s: Starting sync", stream_name)
        instance = STREAMS[stream_name](client)
        instance.stream = stream
        counter_value = sync_stream(state, instance)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)
    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
    LOGGER.info("Finished sync")


def sync_stream(state, instance):
    stream = instance.stream

    with metrics.record_counter(stream.tap_stream_id) as counter:
        for (stream, record) in instance.sync(state):
            counter.increment()

            with Transformer(integer_datetime_fmt="unix-milliseconds-integer-datetime-parsing") as transformer:
                record = transformer.transform(record, stream.schema.to_dict(), metadata.to_map(stream.metadata))
            singer.write_record(stream.tap_stream_id, record)

        if instance.replication_method == "INCREMENTAL":
            singer.write_state(state)

        return counter.value
