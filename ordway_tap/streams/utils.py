from typing import TYPE_CHECKING, Any
from .base import Substream
from .definitions import AVAILABLE_STREAMS
from .exceptions import DependencyConflict

if TYPE_CHECKING:
    from singer.catalog import Catalog


def check_dependency_conflicts(catalog: "Catalog") -> None:
    """ Raises a DependencyConflict exception when a substream is selected when its parent stream isn't """

    for tap_stream_id, stream in AVAILABLE_STREAMS.items():
        if is_substream(stream) or (
            hasattr(stream, "has_substreams") and not stream.has_substreams  # type: ignore
        ):
            continue

        if catalog.get_stream(tap_stream_id).is_selected():
            continue

        # It doesn't seem to properly consider is_substream, but
        # perhaps I'm missing something.
        for substream in stream.substream_definitions:  # type: ignore
            if catalog.get_stream(substream.tap_stream_id).is_selected():
                raise DependencyConflict(
                    f'Stream "{stream.tap_stream_id}" cannot be deselected when its child stream "{substream.tap_stream_id}" is selected'
                )


def is_substream(obj: Any) -> bool:
    """ Checks if obj is a Substream """

    try:
        return isinstance(obj, Substream) or issubclass(
            obj,
            Substream,
        )
    except TypeError:
        return False
