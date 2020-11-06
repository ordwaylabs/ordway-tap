# TODO Integrate changes
"""
import ordway_tap.configs
import json
from typing import TYPE_CHECKING, Dict, Any
from kafka import KafkaConsumer
from inflection import pluralize, underscore
from singer import write_state, get_logger
from singer.transform import Transformer
from singer.metadata import to_map as mdata_to_map
from ordway_tap.utils import print_record
from ordway_tap.record import (
    invoice,
    subscription,
    customer,
    payment,
    credit,
    refund,
    billing_schedule,
    revenue_schedule,
    product,
    order,
)

if TYPE_CHECKING:
    from singer.catalog import CatalogEntry

LOGGER = get_logger()

map_methods = {
    "customers": {"multi": False, "method": customer.map_customer_response},
    "subscriptions": {"multi": True, "method": subscription.map_subscription_response},
    "invoices": {"multi": True, "method": invoice.map_invoice_response},
    "payments": {"multi": False, "method": payment.map_payment_response},
    "credits": {"multi": False, "method": credit.map_credit_response},
    "refunds": {"multi": False, "method": refund.map_refund_response},
    "billing_schedules": {"multi": False, "method": billing_schedule.map_bs_response},
    "revenue_schedules": {"multi": False, "method": revenue_schedule.map_rs_response},
    "products": {"multi": False, "method": product.map_product_response},
    "orders": {"multi": True, "method": order.map_order_response},
}


def listen_topic(state):
    kafka_credentials = ordway_tap.configs.kafka_credentials
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
    for message in consumer:
        process_stream(state, message)


def _handle_record(
    stream: "CatalogEntry", record: Dict[str, Any], transformer: Transformer
):
    transformed_record = transformer.transform(
        record, stream.schema.to_dict(), metadata=mdata_to_map(stream.metadata)
    )

    print_record(stream.tap_stream_id, transformed_record)


def _handle_record_message(stream: "CatalogEntry", record_message: Dict[str, Any]):
    map_method = map_methods[stream.tap_stream_id]
    mapper = map_method["method"]

    with Transformer() as transformer:
        if map_method["multi"]:
            for record in mapper(record_message):
                _handle_record(stream, record, transformer)
        else:
            _handle_record(stream, mapper(record_message), transformer)


def process_stream(state, message):
    json_message = json.loads(message.value)
    LOGGER.info("Message: %s", json.dumps(json_message))
    stream_id = pluralize(underscore(json_message["object"]))
    stream_state = state.get(stream_id, {})
    if stream_state.get("last_synced", "") > json_message["time"]:
        LOGGER.info(
            "Skipping message due to previous timestamp: %d", json_message["time"]
        )
        return False
    stream = ordway_tap.configs.catalog.get_stream(stream_id)
    # Make sure stream is selected for record to print
    if stream and stream.is_selected():
        _handle_record_message(stream, json_message["record"])

        # Write the state message
        stream_state["last_synced"] = json_message["time"]
        state[stream_id] = stream_state
        write_state(state)
"""
