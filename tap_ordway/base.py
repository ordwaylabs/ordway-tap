from typing import TYPE_CHECKING, Any, Dict, NamedTuple, Optional, Union

if TYPE_CHECKING:
    from datetime import datetime
    from .streams.base import Stream, Substream


class DataContext(NamedTuple):
    """ Context for a record's response data """

    tap_stream_id: str
    stream: Union["Stream", "Substream"]
    filter_datetime: "datetime"
    parent_record: Optional[Dict[str, Any]] = None
