#
# Module dependencies.
#

import singer
from tap_iterable.streams import STREAMS
from tap_iterable.exceptions import IterableForbiddenError


LOGGER = singer.get_logger()


def _apply_access_checks(client, accessible_streams: list) -> None:
    """
    Probe each stream for read access and remove inaccessible streams
    (and their children) from accessible_streams in place.
    Note: check_access() always returns True for child streams, so this loop
    effectively identifies only inaccessible parent streams by design.
    Child stream removal is handled separately by _prune_inaccessible_children().
    Raises IterableForbiddenError if no parent streams are accessible.
    """
    inaccessible_streams = [
        stream_name
        for stream_name, stream_cls in STREAMS.items()
        if stream_name in accessible_streams
        and not stream_cls(client=client).check_access()
    ]

    for stream_name in inaccessible_streams:
        accessible_streams.remove(stream_name)

    _prune_inaccessible_children(accessible_streams)

    if inaccessible_streams:
        remaining_parents = [
            name for name in accessible_streams
            if not getattr(STREAMS[name], 'parent', None)
        ]
        if not remaining_parents:
            raise IterableForbiddenError(
                "HTTP-error-code: 403, Error: The account credentials supplied do not have 'read' access to any "
                "of the streams supported by the tap. Data collection cannot be initiated due to lack of permissions."
            )
        LOGGER.warning(
            "The account credentials supplied do not have 'read' access to the following stream(s): %s. "
            "These streams have been excluded from the catalog.",
            ", ".join(inaccessible_streams),
        )


def _prune_inaccessible_children(accessible_streams: list) -> None:
    """
    Remove child streams from the catalog whose parent stream was excluded.
    Mutates accessible_streams in place.
    """
    for name, stream_cls in list(STREAMS.items()):
        parent = getattr(stream_cls, 'parent', None)
        if name in accessible_streams and parent and parent not in accessible_streams:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                name, parent,
            )
            accessible_streams.remove(name)


def discover_streams(client):
    accessible_streams = list(STREAMS.keys())
    _apply_access_checks(client, accessible_streams)

    streams = []
    for stream_name in accessible_streams:
        stream_cls = STREAMS[stream_name]
        stream_client = stream_cls(client)
        stream_schema = stream_client.load_schema()
        streams.append({
            'stream': stream_client.name,
            'tap_stream_id': stream_client.name,
            'schema': stream_schema,
            'metadata': stream_client.load_metadata(stream_schema),
        })

    return streams
