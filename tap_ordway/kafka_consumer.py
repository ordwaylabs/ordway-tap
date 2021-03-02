from typing import TYPE_CHECKING, Any, Dict, Optional, Union
import json
from inflection import pluralize, underscore
from kafka import KafkaConsumer
from singer import get_logger, write_state
import tap_ordway.configs as TAP_CONFIG
from tap_ordway import filter_record, handle_record, prepare_stream
from tap_ordway.base import DataContext
from tap_ordway.streams import EndpointSubstream, ResponseSubstream, Stream
from tap_ordway.utils import get_filter_datetime

if TYPE_CHECKING:
    from datetime import datetime
    from singer.catalog import CatalogEntry  # pylint: disable=ungrouped-imports

LOGGER = get_logger()

# Not entirely sure how this is intended to be used
# in the future, but we need the tap's configuration
# if we do.
def listen_topic(config, state):
    kafka_credentials = TAP_CONFIG.kafka_credentials
    consumer = KafkaConsumer(
        kafka_credentials["topic"],
        group_id=kafka_credentials["group_id"],
        bootstrap_servers=kafka_credentials["bootstrap_servers"],
        client_id=kafka_credentials["client_id"],
        ssl_cafile=kafka_credentials["ssl_cafile"],
        ssl_check_hostname=False,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=kafka_credentials["username"],
        sasl_plain_password=kafka_credentials["password"],
    )
    stream_defs: Dict[str, Union["Stream", "Substream"]] = {}
    stream_versions: Dict[str, Optional[int]] = {}
    stream_setup: Dict[str, bool] = {}

    for message in consumer:
        json_message = json.loads(message.value)
        tap_stream_id = pluralize(underscore(json_message["object"]))

        if not stream_setup.get(tap_stream_id, False):
            filter_datetime = prepare_stream(
                tap_stream_id,
                stream_defs,
                stream_versions,
                TAP_CONFIG.catalog,
                config,
                state,
            )

            LOGGER.info("Syncing stream '%s' since %s", tap_stream_id, filter_datetime)
        else:
            filter_datetime = get_filter_datetime(
                stream_defs[tap_stream_id], TAP_CONFIG.start_date, state
            )

        process_stream(
            stream_defs[tap_stream_id],
            stream_versions[tap_stream_id],
            state,
            json_message,
            filter_datetime,
        )


def process_stream(
    stream_def: Union[Stream, ResponseSubstream, EndpointSubstream],
    stream_version: Optional[int],
    state: Dict[str, Any],
    json_message: Dict[str, Any],
    filter_datetime: "datetime",
) -> None:
    LOGGER.info("Message: %s", json.dumps(json_message))
    stream_id = pluralize(underscore(json_message["object"]))

    record = json_message["record"]
    # Filter based off of the message timestamp or
    # the replication key?
    if filter_record(
        record,
        DataContext(
            tap_stream_id=stream_id, stream=stream_def, filter_datetime=filter_datetime
        ),
    ):
        return None

    state = handle_record(stream_id, record, stream_def, stream_version, state)

    # Make sure stream is selected for record to print
    if stream_def.is_selected:
        if isinstance(stream_def, Stream):
            for substream in stream_def.substreams:
                # Can't handle EndpointSubstream's like this -
                # I'm assuming the producer is pushing the data
                # in a similar way to the API?
                if not substream.is_selected:
                    continue
                if not isinstance(substream, ResponseSubstream):
                    continue

                # .sync_sub_records performs transformations, so not necessary
                # to invoke ourselves here
                for tap_substream_id, sub_record in stream_def.sync_sub_records(
                    substream, record, filter_datetime
                ):
                    state = handle_record(
                        tap_substream_id, sub_record, stream_def, stream_version, state
                    )

            with stream_def.transformer_class() as transformer:
                for record in transformer.transform(
                    record,
                    stream_def.schema_dict,
                    context=DataContext(
                        stream=stream_def,
                        filter_datetime=filter_datetime,
                        tap_stream_id=stream_id,
                    ),
                    metadata=stream_def.mapped_metadata,
                ):
                    state = handle_record(
                        stream_id, record, stream_def, stream_version, state
                    )

        elif isinstance(stream_def, EndpointSubstream):
            # This assumes the data being consumed is akin to
            # the API. As in - /customer/<id>/notes is separated
            # into its own individual message
            context = DataContext(
                tap_stream_id=stream_def.tap_stream_id,
                stream=stream_def,
                filter_datetime=filter_datetime,
            )

            with stream_def.transformer_class() as transformer:
                records = transformer.transform(
                    record,
                    stream_def.schema_dict,
                    context=context,
                    metadata=stream_def.mapped_metadata,
                )

                for record in records:
                    state = handle_record(
                        stream_id, record, stream_def, stream_version, state
                    )

        write_state(state)

    return None
