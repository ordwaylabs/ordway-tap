from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union
from time import time
from inflection import underscore
from singer.bookmarks import get_bookmark
from singer.messages import ActivateVersionMessage, RecordMessage, write_message
from singer.utils import now, strptime_to_utc
import tap_ordway.configs

if TYPE_CHECKING:
    from datetime import datetime
    from .streams.base import StreamABC


def get_company_id():
    """ Gets the configured company ID """

    api_credentials = tap_ordway.configs.api_credentials
    return underscore(api_credentials["company"])


def print_record(
    tap_stream_id: str, record: Dict[str, Any], version: Optional[int] = None
):
    """ Writes record data to stdout """

    write_message(
        RecordMessage(tap_stream_id, record, version, time_extracted=now())
    )  # pragma: no cover


def get_full_table_version() -> int:
    """ Generates a version for FULL_TABLE streams """

    return int(time() * 1000)


def is_first_run(tap_stream_id: str, state: Dict[str, Any]) -> bool:
    """Checks bookmarks to determine if its a stream's first run"""

    value = get_bookmark(state, tap_stream_id, "wrote_initial_activate_version", default=False) 

    if not isinstance(value, bool):
        return True

    return not value


def get_filter_datetime(
    stream: "StreamABC", start_date: str, state: Dict[str, Any]
) -> "datetime":
    """Retrieves the filter datetime for a stream based on the
    configured "start_date" and the state's bookmarks
    """

    filter_datetime_str = start_date

    if stream.is_valid_incremental:
        filter_datetime_str = get_bookmark(
            state, stream.tap_stream_id, stream.replication_key, filter_datetime_str
        )

    filter_datetime = strptime_to_utc(filter_datetime_str)

    return filter_datetime


def write_activate_version(tap_stream_id: str, version: Optional[int]) -> None:
    """ Writes an ACTIVATE_VERSION message to stdout """

    write_message(ActivateVersionMessage(tap_stream_id, version))  # pragma: no cover


def denest(obj: Dict[str, Any], path: Tuple[str, ...]) -> List[Dict[str, Any]]:
    """Recursively denest a dictionary
    Example:
        denest({
            "plans": [{
                "charge": {
                    "id": 1
                }
            }, {
                "charge": {
                    "id": 2
                }
            }]
        }, ("plans", "charge"))
        Returns:
        [{
            "id": 1
        }, {
            "id": 2
        }]
    """

    if len(path) == 0:
        return [obj]

    results: List[Any] = []

    val = obj.get(path[0])

    if val is None:
        return results

    if isinstance(val, list):
        for elem in val:
            results = results + denest(elem, path[1:])  # type: ignore
    elif isinstance(val, dict):
        if len(path) == 1:
            results.append(val)
        else:
            results = results + denest(val, path[1:])  # type: ignore

    return results
