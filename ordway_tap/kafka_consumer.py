import inflection
from kafka import KafkaConsumer
import ordway_tap.configs
import json
import singer
from ordway_tap.api_sync.utils import print_record
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

LOGGER = singer.get_logger()

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


def process_stream(state, message):
    json_message = json.loads(message.value)
    LOGGER.info("Message: " + json.dumps(json_message))
    stream_id = inflection.pluralize(inflection.underscore(json_message["object"]))
    stream_state = state.get(stream_id, {})
    if stream_state.get("last_synced", "") > json_message["time"]:
        LOGGER.info(
            "Skipping message due to previous timestamp: " + json_message["time"]
        )
        return False
    stream = ordway_tap.configs.catalog.get_stream(stream_id)
    # Make sure stream is selected for record to print
    if stream and stream.is_selected():
        map_method = map_methods[stream_id]
        if map_method["multi"]:
            for record in map_method["method"](json_message["record"]):
                print_record(stream_id, record)
        else:
            print_record(stream_id, map_method["method"](json_message["record"]))
        # Write the state message
        stream_state["last_synced"] = json_message["time"]
        state[stream_id] = stream_state
        singer.write_state(state)
