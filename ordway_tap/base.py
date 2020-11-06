from typing import Union, Optional, Dict, Any, NamedTuple


class DataContext(NamedTuple):
    """ Context for a record's response data """

    tap_stream_id: str
    stream: Union["Stream", "EndpointStream"]
    filter_datetime: "datetime"
    parent_record: Optional[Dict[str, Any]] = None