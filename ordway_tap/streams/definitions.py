from typing import TYPE_CHECKING
from singer import get_logger
from .base import Stream, ResponseSubstream, EndpointSubstream
from ..transformers import (
    RecordTransformer,
    BillingScheduleTransformer,
    CreditTransformer,
    CustomerTransformer,
    InvoiceTransformer,
    OrderTransformer,
    PaymentTransformer,
    StatementTransformer,
    SubscriptionTransformer,
)
from ..api import RequestHandler

if TYPE_CHECKING:
    from datetime import datetime

LOGGER = get_logger()

# TODO pre_hook handles a lot of these transformations
# remove later

# Was working with filtering, but now isn't?
class BillingRuns(Stream):
    """Billing Runs stream"""

    tap_stream_id = "billing_runs"
    key_properties = ["billing_run_id"]
    valid_replication_keys = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer = RecordTransformer
    request_handler = RequestHandler("/billing_runs", sort="name")


# FIXME Can't sort by updated_date
class BillingSchedules(Stream):
    """Billing Schedules stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/billing-schedules
    """

    tap_stream_id = "billing_schedules"
    key_properties = ["billing_schedule_id"]
    transformer = BillingScheduleTransformer
    request_handler = RequestHandler("/billing_schedules", sort="id")


class Credits(Stream):
    """Credits stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/credits
    """

    tap_stream_id = "credits"
    key_properties = ["credit_id"]
    transformer = CreditTransformer
    request_handler = RequestHandler("/credits")


class Contacts(ResponseSubstream):
    """Contacts stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/contacts
    """

    tap_stream_id = "contacts"
    key_properties = ["contact_id"]
    valid_replication_keys = []
    replication_key = None
    replication_method = "FULL_TABLE"
    path = ("contacts",)
    transformer = RecordTransformer


class PaymentMethods(EndpointSubstream):
    """Payment methods stream"""

    tap_stream_id = "payment_methods"
    key_properties = ["payment_method_id"]
    valid_replication_keys = []
    replication_key = None
    replication_method = "FULL_TABLE"
    request_handler = RequestHandler("/customers/{id}/payment_methods", sort=None)
    transformer = RecordTransformer


class CustomerNotes(EndpointSubstream):
    """Customer notes stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/customer-notes
    """

    tap_stream_id = "customer_notes"
    key_properties = ["customer_note_id"]
    valid_replication_keys = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer = RecordTransformer
    request_handler = RequestHandler("/customers/{id}/customer_notes")


# INCREMENTAL is allowed if substreams aren't selected.
class Customers(Stream):
    """Customers stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/customers
    """

    tap_stream_id = "customers"
    key_properties = ["customer_id"]
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer = CustomerTransformer
    request_handler = RequestHandler("/customers")
    substreams = [Contacts]


class Invoices(Stream):
    """Invoices stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/invoices
    """

    tap_stream_id = "invoices"
    key_properties = ["invoice_id"]
    transformer = InvoiceTransformer
    request_handler = RequestHandler("/invoices")


class Orders(Stream):
    """Orders stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/orders
    """

    tap_stream_id = "orders"
    key_properties = ["order_id"]
    transformer = OrderTransformer
    request_handler = RequestHandler("/orders")


class Payments(Stream):
    """Payments stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/payments
    """

    tap_stream_id = "payments"
    key_properties = ["payment_id"]
    transformer = PaymentTransformer
    request_handler = RequestHandler("/payments")


class Products(Stream):
    """Products stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/products
    """

    tap_stream_id = "products"
    key_properties = ["product_id"]
    transformer = RecordTransformer
    request_handler = RequestHandler("/products")


class Refunds(Stream):
    """Refunds stream"""

    tap_stream_id = "refunds"
    key_properties = ["refund_id"]
    transformer = RecordTransformer
    request_handler = RequestHandler("/refunds")


class RevenueSchedules(Stream):
    """Revenue schedules stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/revenue-schedules
    """

    tap_stream_id = "revenue_schedules"
    key_properties = ["revenue_schedule_id"]
    transformer = RecordTransformer
    request_handler = RequestHandler("/revenue_schedules", page_size=500)


class Subscriptions(Stream):
    """Subscriptions stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/subscriptions
    """

    tap_stream_id = "subscriptions"
    key_properties = [
        "subscription_id",
    ]
    transformer = SubscriptionTransformer
    request_handler = RequestHandler("/subscriptions")


class Charges(ResponseSubstream):
    """Charges stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/charges
    """

    tap_stream_id = "charges"
    key_properties = ["charge_id"]
    valid_replication_keys = []
    replication_key = None
    replication_method = "FULL_TABLE"
    path = ("charges",)
    transformer = RecordTransformer


class Plans(Stream):
    """Plans stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/plans
    """

    tap_stream_id = "plans"
    substreams = [Charges]
    key_properties = ["plan_id"]
    valid_replication_keys = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer = RecordTransformer
    request_handler = RequestHandler("/plans")


class PaymentRuns(Stream):
    """Payment runs stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/payment-runs
    """

    tap_stream_id = "payment_runs"
    key_properties = ["payment_run_id"]
    valid_replication_keys = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer = RecordTransformer
    request_handler = RequestHandler("/payment_runs")


# Can't filter by updated_date or sort
class RevenueRules(Stream):
    """Revenue rules stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/revenue-rules
    """

    tap_stream_id = "revenue_rules"
    key_properties = ["revenue_rule_id"]
    valid_replication_keys = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer = RecordTransformer
    request_handler = RequestHandler("/revenue_rules", sort=None)


class ChartOfAccounts(Stream):
    """Chart of Accounts stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/chart-of-accounts
    """

    tap_stream_id = "chart_of_accounts"
    key_properties = ["code"]
    valid_replication_keys = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer = RecordTransformer
    request_handler = RequestHandler("/chart_of_accounts", sort=None)


class Webhooks(Stream):
    """Webhooks stream """

    tap_stream_id = "webhooks"
    key_properties = ["name"]
    valid_replication_keys = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer = RecordTransformer
    request_handler = RequestHandler("/webhooks", sort=None)


# FIXME Not being sorted in ascending order by Ordway?
class Statements(Stream):
    """Statements stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/statements
    """

    tap_stream_id = "statements"
    key_properties = ["statement_id"]
    transformer = StatementTransformer
    request_handler = RequestHandler("/statements")


# FIXME Should be capable of running in INCREMENTAL, but
# can't filter by updated_date for some reason.
class Coupons(Stream):
    """Coupons stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/coupons
    """

    tap_stream_id = "coupons"
    key_properties = ["coupon_id"]
    valid_replication_keys = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer = RecordTransformer
    request_handler = RequestHandler("/coupons")


class Usages(Stream):
    """Usages stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/usages
    """

    tap_stream_id = "usages"
    key_properties = ["usage_id"]
    transformer = RecordTransformer
    request_handler = RequestHandler("/usages")


AVAILABLE_STREAMS = {
    "billing_runs": BillingRuns,
    "billing_schedules": BillingSchedules,
    "customers": Customers,
    "invoices": Invoices,
    "credits": Credits,
    "orders": Orders,
    "payments": Payments,
    "products": Products,
    "refunds": Refunds,
    "revenue_schedules": RevenueSchedules,
    "subscriptions": Subscriptions,
    "plans": Plans,
    "charges": Charges,
    "payment_runs": PaymentRuns,
    "revenue_rules": RevenueRules,
    "chart_of_accounts": ChartOfAccounts,
    "webhooks": Webhooks,
    "statements": Statements,
    "coupons": Coupons,
    "usages": Usages,
    "contacts": Contacts,
    "payment_methods": PaymentMethods,
    "customer_notes": CustomerNotes,
}
