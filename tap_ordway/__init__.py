#!/usr/bin/env python3
from typing import TYPE_CHECKING, Any, Dict, Optional, Union
import json
import os
from _datetime import datetime
from singer import get_logger
from singer.bookmarks import set_currently_syncing, write_bookmark
from singer.catalog import Catalog, CatalogEntry
from singer.messages import write_schema, write_state
from singer.schema import Schema
from singer.utils import handle_top_exception, parse_args, strptime_to_utc
import tap_ordway.configs as TAP_CONFIG
from .api.consts import DEFAULT_API_VERSION
from .property import (
    get_key_properties,
    get_replication_key,
    get_replication_method,
    get_stream_metadata,
)
from .streams import AVAILABLE_STREAMS, check_dependency_conflicts, is_substream
from .utils import (
    get_filter_datetime,
    get_full_table_version,
    get_version,
    print_record,
    write_activate_version,
)

if TYPE_CHECKING:
    from .base import DataContext
    from .streams.base import Stream, Substream


REQUIRED_CONFIG_KEYS = [
    "company",
    "api_key",
    "user_email",
    "user_token",
    "start_date",
]
LOGGER = get_logger()


def _get_abs_path(path: str) -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas() -> Dict[str, Any]:
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(_get_abs_path("schemas")):
        path = _get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover() -> Catalog:
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = get_stream_metadata(stream_id, schema.to_dict())
        key_properties = get_key_properties(stream_id)

        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=get_replication_key(stream_id),
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=get_replication_method(stream_id),
            )
        )
    return Catalog(streams)


def filter_record(record: Dict[str, Any], context: "DataContext") -> bool:
    """Filter hook for ensuring records are less than filter_datetime for
    streams that don't support filtering by updated_date via Ordway's API
    """

    record_updated_date = record.get("updated_date")

    if record_updated_date is None:
        LOGGER.debug(
            "Skipping record for stream '%s': updated_date is None or non-existent",
            context.tap_stream_id,
        )

        return False

    if strptime_to_utc(record_updated_date) <= context.filter_datetime:
        LOGGER.debug(
            "Skipping record for stream '%s': %s is <= %s",
            context.tap_stream_id,
            record_updated_date,
            context.filter_datetime,
        )

        return True

    return False


def handle_record(
    tap_stream_id: str,
    record: Dict[str, Any],
    stream_def: Union["Stream", "Substream"],
    stream_version: Optional[int],
    state: Dict[str, Any],
) -> Dict[str, Any]:
    """Handles a single record's emission"""

    print_record(tap_stream_id, record, version=stream_version)

    if not is_substream(stream_def):
        state = set_currently_syncing(state, tap_stream_id)

    if not stream_def.is_valid_incremental:
        return state

    replication_key = stream_def.replication_key

    # mypy ignoring is_valid_incremental above
    bookmark_date = record.get(replication_key)  # type: ignore

    if bookmark_date is None:
        LOGGER.warning(
            'State not updated. Replication key "%s" not found in record for stream "%s": %s',
            replication_key,
            tap_stream_id,
            record,
        )

        return state

    LOGGER.debug("Adding bookmark for %s at %s", tap_stream_id, bookmark_date)

    state = write_bookmark(
        state,
        tap_stream_id,
        replication_key,
        bookmark_date,
    )

    write_state(state)

    return state


_STREAM_DEFS = Dict[str, Union["Stream", "Substream"]]  # pylint: disable=invalid-name
_STREAM_VERSIONS = Dict[str, Optional[int]]  # pylint: disable=invalid-name

# Could be refactored
# pylint: disable=too-many-arguments
def prepare_stream(
    tap_stream_id: str,
    stream_defs: _STREAM_DEFS,
    stream_versions: _STREAM_VERSIONS,
    catalog: Catalog,
    config: Dict[str, Any],
    state: Dict[str, Any],
) -> datetime:
    """Prepares a stream and any of its substreams by instantiating them and
    handling their preliminary Singer messages
    """

    # mypy isn't properly considering is_substream
    stream_def: "Stream" = AVAILABLE_STREAMS[tap_stream_id](catalog, config, filter_record)  # type: ignore
    stream_defs[stream_def.tap_stream_id] = stream_def

    if stream_def.has_substreams:
        stream_def.instantiate_substreams(catalog, filter_record)

        for substream_def in stream_def.substreams:
            if not substream_def.is_selected:
                LOGGER.info('Skipping sub-stream "%s"', substream_def.tap_stream_id)

                continue

            # ignored type errors below seem to be caused by same issue as
            # https://github.com/python/mypy/issues/8993
            stream_defs[substream_def.tap_stream_id] = substream_def
            substream_version = get_full_table_version()
            stream_versions[substream_def.tap_stream_id] = substream_version

            write_schema(
                stream_name=substream_def.tap_stream_id,
                schema=substream_def.schema_dict,
                key_properties=substream_def.key_properties,
            )

            # Substreams and their parent stream are all FULL_TABLE, so
            # no need to check substream bookmarks.
            write_activate_version(substream_def.tap_stream_id, substream_version)

    write_schema(
        stream_name=stream_def.tap_stream_id,
        schema=stream_def.schema_dict,
        key_properties=stream_def.key_properties,
    )

    filter_datetime = get_filter_datetime(stream_def, config["start_date"], state)
    stream_version = get_version(stream_def, config["start_date"], filter_datetime)
    stream_versions[stream_def.tap_stream_id] = stream_version

    if stream_version is not None:
        write_activate_version(
            stream_def.tap_stream_id,
            stream_version,
        )

    return filter_datetime


def sync(config: Dict[str, Any], state: Dict[str, Any], catalog: Catalog) -> None:
    # For looking up Catalog-configured streams more efficiently
    # later Singer stores catalog entries as a list and iterates
    # over it with .get_stream()
    stream_defs: Dict[str, Union["Stream", "Substream"]] = {}
    stream_versions: Dict[str, Optional[int]] = {}

    check_dependency_conflicts(catalog)

    for stream in catalog.get_selected_streams(state):
        if is_substream(AVAILABLE_STREAMS[stream.tap_stream_id]):
            LOGGER.info(
                'Skipping substream "%s" until parent stream is reached',
                stream.tap_stream_id,
            )

            continue

        LOGGER.info("Syncing stream: %s", stream.tap_stream_id)

        filter_datetime = prepare_stream(
            stream.tap_stream_id, stream_defs, stream_versions, catalog, config, state
        )
        stream_def = stream_defs[stream.tap_stream_id]

        LOGGER.info("Querying since: %s", filter_datetime)

        for tap_stream_id, record in stream_def.sync(filter_datetime):  # type: ignore
            state = handle_record(
                tap_stream_id,
                record,
                stream_defs[tap_stream_id],
                stream_versions[tap_stream_id],
                state,
            )

        write_state(state)

    state = set_currently_syncing(state, None)
    write_state(state)


def set_global_config(config: Dict[str, Any]) -> None:
    """Sets global configuration variables"""

    # Set global configuration variables
    TAP_CONFIG.api_credentials = {
        "company": config["company"],
        "api_key": config["api_key"],
        "user_email": config["user_email"],
        "user_token": config["user_token"],
    }

    company_token = config.get("company_token")
    if company_token is not None:
        TAP_CONFIG.api_credentials["company_token"] = company_token

    TAP_CONFIG.api_version = config.get("api_version", DEFAULT_API_VERSION)
    TAP_CONFIG.staging = config.get("staging", False)
    TAP_CONFIG.api_url = config.get("api_url")
    TAP_CONFIG.start_date = config["start_date"]
    TAP_CONFIG.rate_limit_rps = config.get("rate_limit_rps")

    if (
        isinstance(TAP_CONFIG.rate_limit_rps, (int, float))
        and TAP_CONFIG.rate_limit_rps <= 0
    ):
        raise ValueError(
            "`rate_limit_rps` must be set to `null` or a number GREATER THAN 0"
        )


@handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = parse_args(REQUIRED_CONFIG_KEYS)

    set_global_config(args.config)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        TAP_CONFIG.catalog = catalog

        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
