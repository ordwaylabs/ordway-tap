#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import ordway_tap.property
import ordway_tap.api_sync
import ordway_tap.configs
import ordway_tap.kafka_consumer
from _datetime import datetime


REQUIRED_CONFIG_KEYS = ['api_credentials', 'kafka_credentials']
REQUIRED_API_CREDENTIAL_KEYS = ['x_company', 'x_company_token', 'x_user_email', 'x_user_token', 'endpoint']
REQUIRED_KAFKA_CREDENTIAL_KEYS = ['topic', 'group_id', 'bootstrap_servers', 'client_id', 'ssl_cafile']
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def state_hash():
    state = {}
    for filename in os.listdir(get_abs_path('schemas')):
        file_raw = filename.replace('.json', '')
        state[file_raw] = { 'synced': False, 'last_synced': '' }
    return state


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = property.get_stream_metadata(schema)
        key_properties = property.get_key_properties(stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def sync(config, state, catalog):
    """ Sync data from tap source """
    if not bool(state):
        state = state_hash()
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info('Syncing stream:' + stream.tap_stream_id)

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        current_time = datetime.utcnow().isoformat(sep='T', timespec='milliseconds')
        if not state.get(stream.tap_stream_id, {}).get('synced', False):
            # Sync records via API if replication method is FULL_TABLE
            api_sync.sync(stream.tap_stream_id)
            state[stream.tap_stream_id] = {'synced': True, 'last_synced': current_time}
            singer.write_state(state)

    # Realtime stream from Kafka
    kafka_consumer.listen_topic(state)
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    # Check API credential keys
    utils.check_config(args.config['api_credentials'], REQUIRED_API_CREDENTIAL_KEYS)
    # Check Kafka credential keys
    utils.check_config(args.config['kafka_credentials'], REQUIRED_KAFKA_CREDENTIAL_KEYS)

    configs.api_credentials = args.config['api_credentials']
    configs.kafka_credentials = args.config['kafka_credentials']

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
        configs.catalog = catalog
        sync(args.config, args.state, catalog)


if __name__ == '__main__':
    main()
