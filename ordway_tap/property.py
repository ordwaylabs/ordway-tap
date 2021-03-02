from typing import List, Optional
from singer.metadata import get_standard_metadata
from .streams import AVAILABLE_STREAMS


def get_key_properties(tap_stream_id: str) -> Optional[List[str]]:
    """ Retrieves a stream's key_properties """

    key_properties = AVAILABLE_STREAMS[tap_stream_id].key_properties

    if len(key_properties) == 0:  # type: ignore
        return None

    return key_properties  # type: ignore


def get_replication_method(tap_stream_id: str) -> Optional[str]:
    """ Retrieves a stream's default replication method """

    return getattr(AVAILABLE_STREAMS[tap_stream_id], "replication_method", None)


def get_replication_key(tap_stream_id: str) -> Optional[str]:
    """ Retrieves a stream's default replication key """

    return getattr(AVAILABLE_STREAMS[tap_stream_id], "replication_key", None)


def get_stream_metadata(tap_stream_id, schema_dict) -> List:
    """ Generates a stream's default metadata """

    stream_def = AVAILABLE_STREAMS[tap_stream_id]

    return get_standard_metadata(
        schema=schema_dict,
        key_properties=get_key_properties(tap_stream_id),
        valid_replication_keys=stream_def.valid_replication_keys,
    )
