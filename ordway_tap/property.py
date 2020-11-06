from typing import Optional, List
from singer.metadata import (
    get_standard_metadata,
    to_map as mdata_to_map,
    write as mdata_write,
    to_list as mdata_to_list,
)
from .streams import AVAILABLE_STREAMS


def get_key_properties(tap_stream_id: str) -> Optional[List[str]]:
    """ Retrieves a stream's key_properties """

    key_properties = AVAILABLE_STREAMS[tap_stream_id].key_properties

    if not key_properties:
        return None

    return key_properties


def get_replication_method(tap_stream_id: str) -> Optional[str]:
    return getattr(AVAILABLE_STREAMS[tap_stream_id], "replication_method", None)


def get_replication_key(tap_stream_id: str) -> Optional[str]:
    return getattr(AVAILABLE_STREAMS[tap_stream_id], "replication_key", None)


def get_stream_metadata(tap_stream_id, schema_dict):
    stream_def = AVAILABLE_STREAMS[tap_stream_id]

    metadata = get_standard_metadata(
        schema=schema_dict,
        key_properties=get_key_properties(tap_stream_id),
        valid_replication_keys=stream_def.valid_replication_keys,
    )

    metadata = mdata_to_map(metadata)

    for field_name in schema_dict["properties"].keys():
        # selected-by-default doesn't currently work as intended.
        # A PR has been made for a fix, but it hasn't been merged yet:
        # https://github.com/singer-io/singer-python/pull/121
        # Writing regardless for when PR is merged.
        metadata = mdata_write(
            metadata, ("properties", field_name), "selected-by-default", True
        )

    metadata = mdata_write(metadata, (), "selected", True)

    return mdata_to_list(metadata)
