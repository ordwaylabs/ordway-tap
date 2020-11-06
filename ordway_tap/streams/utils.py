from typing import Any
from .base import Substream, Stream, ResponseSubstream, EndpointSubstream
from .definitions import AVAILABLE_STREAMS

# TODO Use me
def check_dependency_conflicts(catalog: "Catalog") -> bool:
    """ Checks whether a substream is selected when its parent stream isn't """

    for tap_stream_id, stream in AVAILABLE_STREAMS.items():
        if stream is Substream or (stream is Stream and not stream.has_substreams):
            continue

        if catalog.get_stream(tap_stream_id).is_selected():
            continue

        for substream in stream.substreams:
            if catalog.get_stream(substream.tap_stream_id).is_selected():
                return True

    return False


def is_substream(obj: Any) -> bool:
    """ Checks if obj is a Substream """

    try:
        return isinstance(obj, (ResponseSubstream, EndpointSubstream)) or issubclass(
            obj,
            (ResponseSubstream, EndpointSubstream),
        )
    except TypeError:
        return False
