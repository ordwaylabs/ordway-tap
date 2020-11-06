from typing import TYPE_CHECKING, Dict, Tuple, List, Any, Optional
import decimal
from time import time
from singer.utils import now, strptime_to_utc
from singer.messages import RecordMessage, ActivateVersionMessage, write_message
from inflection import underscore
import ordway_tap.configs

if TYPE_CHECKING:
    from datetime import datetime
    from .streams.base import StreamABC


def get_company_id():
    api_credentials = ordway_tap.configs.api_credentials
    return underscore(api_credentials["x_company"])


def print_record(
    tap_stream_id: str, record: Dict[str, Any], version: Optional[int] = None
):
    """ Writes record data to stdout """

    write_message(RecordMessage(tap_stream_id, record, version, time_extracted=now()))


def get_full_table_version() -> int:
    return int(time() * 1000)


def get_version(
    stream: "StreamABC", start_date: str, filter_datetime: Optional["datetime"] = None
) -> Optional[int]:
    is_first_run = filter_datetime <= strptime_to_utc(start_date)

    if stream.is_valid_incremental:
        if is_first_run:
            return 1

        return None

    return get_full_table_version()


def write_activate_version(tap_stream_id: str, version: Optional[int]) -> None:
    write_message(ActivateVersionMessage(tap_stream_id, version))


def convert_to_decimal(value):
    try:
        return decimal.Decimal(value)
    except (decimal.InvalidOperation, TypeError):
        return 0


def format_date_string(value):
    if value == "-":
        return None
    return value or None


def format_array(value):
    return value or None


def format_boolean(value):
    if value == "-":
        return False
    return value


def denest(obj: Dict[str, Any], path: Tuple[str]) -> List[Dict[str, Any]]:
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
