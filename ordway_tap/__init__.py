#!/usr/bin/env python3
import os
import json
import singer
from typing import TYPE_CHECKING, Dict, Any, List
from _datetime import datetime
from singer import utils, metadata
from singer.utils import (
    handle_top_exception,
    parse_args,
    check_config,
    strptime_to_utc,
    strftime,
)
from singer.messages import write_state, write_message, RecordMessage
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.bookmarks import get_bookmark, write_bookmark, set_currently_syncing
import ordway_tap.kafka_consumer
import ordway_tap.configs as TAP_CONFIG
from .property import (
    get_key_properties,
    get_stream_metadata,
    get_replication_method,
    get_replication_key,
)
from .streams import AVAILABLE_STREAMS, check_dependency_conflicts, is_substream
from .utils import (
    print_record,
    get_version,
    get_full_table_version,
    write_activate_version,
)
from .api.consts import DEFAULT_API_VERSION

if TYPE_CHECKING:
    from .streams.base import StreamABC


REQUIRED_CONFIG_KEYS = ["api_credentials", "start_date"]
REQUIRED_API_CREDENTIAL_KEYS = [
    "x_company",
    "x_api_key",
    "x_user_email",
    "x_user_token",
]
LOGGER = singer.get_logger()


def get_abs_path(path: str) -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas() -> Dict[str, Any]:
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
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


def sync(config: Dict[str, Any], state: Dict[str, Any], catalog: Catalog) -> None:
    # For looking up Catalog-configured streams more efficiently
    # later Singer stores catalog entries as a list and iterates
    # over it with .get_stream()
    stream_defs = {}
    stream_versions = {}

    for stream in catalog.get_selected_streams(state):
        if is_substream(AVAILABLE_STREAMS[stream.tap_stream_id]):
            LOGGER.info(
                'Skipping substream "%s" until parent stream is reached',
                stream.tap_stream_id,
            )

            continue

        LOGGER.info("Syncing stream: %s", stream.tap_stream_id)

        stream_def = AVAILABLE_STREAMS[stream.tap_stream_id](catalog, config)
        stream_defs[stream_def.tap_stream_id] = stream_def

        if stream_def.has_substreams:
            stream_def.instantiate_substreams(catalog)

            for substream_def in stream_def.substreams:
                stream_defs[substream_def.tap_stream_id] = substream_def
                substream_version = get_full_table_version()
                stream_versions[substream_def.tap_stream_id] = substream_version

                singer.write_schema(
                    stream_name=substream_def.tap_stream_id,
                    schema=substream_def.schema_dict,
                    key_properties=substream_def.key_properties,
                )

                # Substreams and their parent stream are all FULL_TABLE, so
                # no need to check substream bookmarks.
                write_activate_version(substream_def.tap_stream_id, substream_version)

        singer.write_schema(
            stream_name=stream_def.tap_stream_id,
            schema=stream_def.schema_dict,
            key_properties=stream_def.key_properties,
        )

        filter_datetime = config["start_date"]

        if stream_def.is_valid_incremental:
            filter_datetime = get_bookmark(
                state, stream.tap_stream_id, stream_def.replication_key, filter_datetime
            )

        filter_datetime = strptime_to_utc(filter_datetime)

        LOGGER.info("Querying since: %s", filter_datetime)

        stream_version = get_version(stream_def, config["start_date"], filter_datetime)
        stream_versions[stream_def.tap_stream_id] = stream_version

        if stream_version is not None:
            write_activate_version(
                stream_def.tap_stream_id,
                stream_version,
            )

        for tap_stream_id, record in stream_def.sync(filter_datetime):
            print_record(tap_stream_id, record, version=stream_versions[tap_stream_id])

            if not stream_defs[tap_stream_id].is_valid_incremental:
                continue

            bookmark_date = record.get(stream_defs[tap_stream_id].replication_key)

            if bookmark_date is None:
                LOGGER.warning(
                    'State not updated. Replication key "%s" not found in record for stream "%s": %s',
                    stream_defs[tap_stream_id].replication_key,
                    tap_stream_id,
                    record,
                )

                continue

            LOGGER.info("Adding bookmark for %s at %s", tap_stream_id, bookmark_date)

            if not is_substream(stream_defs[tap_stream_id]):
                state = set_currently_syncing(state, tap_stream_id)

            state = write_bookmark(
                state,
                tap_stream_id,
                stream_def.replication_key,
                bookmark_date,
            )

            write_state(state)

        write_state(state)


@handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = parse_args(REQUIRED_CONFIG_KEYS)
    # Check API credential keys
    check_config(args.config["api_credentials"], REQUIRED_API_CREDENTIAL_KEYS)

    # Set global configuration variables
    TAP_CONFIG.api_credentials = args.config["api_credentials"]
    TAP_CONFIG.api_version = args.config.get("api_version", DEFAULT_API_VERSION)
    TAP_CONFIG.staging = args.config.get("staging", False)
    TAP_CONFIG.api_url = args.config.get("api_url", None)

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
