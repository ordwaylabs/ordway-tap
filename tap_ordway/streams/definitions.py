from typing import TYPE_CHECKING, Dict, Sequence, Type, Union
from singer import get_logger
from ..api import RequestHandler
from ..transformers import (
    BillingScheduleTransformer,
    CustomerTransformer,
    InvoiceTransformer,
    OrderTransformer,
    RecordTransformer,
    SubscriptionTransformer,
)
from .base import EndpointSubstream, ResponseSubstream, Stream

if TYPE_CHECKING:
    from .base import Substream

LOGGER = get_logger()


class BillingRuns(Stream):
    """Billing Runs stream"""

    tap_stream_id = "billing_runs"
    key_properties = ["billing_run_id", "company_id"]
    valid_replication_keys: Sequence[str] = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer_class = RecordTransformer
    request_handler = RequestHandler("/billing_runs", sort="name")


class BillingSchedules(Stream):
    """Billing Schedules stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/billing-schedules
    """

    tap_stream_id = "billing_schedules"
    key_properties = ["billing_schedule_id", "company_id"]
    valid_replication_keys: Sequence[str] = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer_class = BillingScheduleTransformer
    request_handler = RequestHandler("/billing_schedules", sort="id")


class Credits(Stream):
    """Credits stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/credits
    """

    tap_stream_id = "credits"
    key_properties = ["credit_id", "company_id"]
    transformer_class = RecordTransformer
    request_handler = RequestHandler("/credits", sort="updated_date,id")


class Contacts(ResponseSubstream):
    """Contacts stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/contacts
    """

    tap_stream_id = "contacts"
    key_properties = ["contact_id", "company_id"]
    valid_replication_keys: Sequence[str] = []
    replication_key = None
    replication_method = "FULL_TABLE"
    path = ("contacts",)
    transformer_class = RecordTransformer


class PaymentMethods(EndpointSubstream):
    """Payment methods stream"""

    tap_stream_id = "payment_methods"
    key_properties = ["payment_method_id", "company_id"]
    valid_replication_keys: Sequence[str] = []
    replication_key = None
    replication_method = "FULL_TABLE"
    request_handler = RequestHandler("/customers/{id}/payment_methods")
    transformer_class = RecordTransformer


class CustomerNotes(EndpointSubstream):
    """Customer notes stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/customer-notes
    """

    tap_stream_id = "customer_notes"
    key_properties = ["customer_note_id", "company_id"]
    valid_replication_keys: Sequence[str] = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer_class = RecordTransformer
    request_handler = RequestHandler("/customers/{id}/customer_notes")


# INCREMENTAL is allowed if substreams aren't selected.
class Customers(Stream):
    """Customers stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/customers
    """

    tap_stream_id = "customers"
    key_properties = ["customer_id", "company_id"]
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer_class = CustomerTransformer
    request_handler = RequestHandler("/customers")
    substream_definitions = [Contacts, CustomerNotes, PaymentMethods]


class Invoices(Stream):
    """Invoices stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/invoices
    """

    tap_stream_id = "invoices"
    key_properties = ["invoice_id", "company_id", "invoice_line_no"]
    transformer_class = InvoiceTransformer
    request_handler = RequestHandler("/invoices", sort="updated_date,id")


class Orders(Stream):
    """Orders stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/orders
    """

    tap_stream_id = "orders"
    key_properties = ["order_id", "company_id", "order_line_no"]
    transformer_class = OrderTransformer
    request_handler = RequestHandler("/orders", sort="updated_date,id")


class Payments(Stream):
    """Payments stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/payments
    """

    tap_stream_id = "payments"
    key_properties = ["payment_id", "company_id"]
    transformer_class = RecordTransformer
    request_handler = RequestHandler("/payments", sort="updated_date,id")


class Products(Stream):
    """Products stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/products
    """

    tap_stream_id = "products"
    key_properties = ["product_id", "company_id"]
    transformer_class = RecordTransformer
    request_handler = RequestHandler("/products", sort="updated_date,id")


class Refunds(Stream):
    """Refunds stream"""

    tap_stream_id = "refunds"
    key_properties = ["refund_id", "company_id"]
    transformer_class = RecordTransformer
    request_handler = RequestHandler("/refunds", sort="updated_date,id")


class RevenueSchedules(Stream):
    """Revenue schedules stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/revenue-schedules
    """

    tap_stream_id = "revenue_schedules"
    key_properties = ["revenue_schedule_id", "company_id"]
    transformer_class = RecordTransformer
    request_handler = RequestHandler(
        "/revenue_schedules", page_size=500, sort="updated_date,id"
    )


class Subscriptions(Stream):
    """Subscriptions stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/subscriptions
    """

    tap_stream_id = "subscriptions"
    key_properties = ["subscription_id", "subscription_line_id", "company_id"]
    transformer_class = SubscriptionTransformer
    replication_method = "INCREMENTAL"
    request_handler = RequestHandler("/subscriptions", sort="updated_date,id")


class Charges(ResponseSubstream):
    """Charges stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/charges
    """

    tap_stream_id = "charges"
    key_properties = ["charge_id", "company_id"]
    valid_replication_keys: Sequence[str] = []
    replication_key = None
    replication_method = "FULL_TABLE"
    path = ("charges",)
    transformer_class = RecordTransformer


class Plans(Stream):
    """Plans stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/plans
    """

    tap_stream_id = "plans"
    substream_definitions = [Charges]
    key_properties = ["plan_id", "company_id"]
    valid_replication_keys: Sequence[str] = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer_class = RecordTransformer
    request_handler = RequestHandler("/plans")


class PaymentRuns(Stream):
    """Payment runs stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/payment-runs
    """

    tap_stream_id = "payment_runs"
    key_properties = ["payment_run_id", "company_id"]
    valid_replication_keys: Sequence[str] = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer_class = RecordTransformer
    request_handler = RequestHandler("/payment_runs")


class RevenueRules(Stream):
    """Revenue rules stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/revenue-rules
    """

    tap_stream_id = "revenue_rules"
    key_properties = ["revenue_rule_id", "company_id"]
    valid_replication_keys: Sequence[str] = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer_class = RecordTransformer
    request_handler = RequestHandler("/revenue_rules", sort=None)


class ChartOfAccounts(Stream):
    """Chart of Accounts stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/chart-of-accounts
    """

    tap_stream_id = "chart_of_accounts"
    key_properties = ["code", "company_id"]
    valid_replication_keys: Sequence[str] = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer_class = RecordTransformer
    request_handler = RequestHandler("/chart_of_accounts", sort=None)


class Webhooks(Stream):
    """Webhooks stream """

    tap_stream_id = "webhooks"
    key_properties = ["name", "company_id"]
    valid_replication_keys: Sequence[str] = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer_class = RecordTransformer
    request_handler = RequestHandler("/webhooks", sort=None)


class Statements(Stream):
    """Statements stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/statements
    """

    tap_stream_id = "statements"
    key_properties = ["statement_id", "company_id"]
    transformer_class = RecordTransformer
    request_handler = RequestHandler("/statements", sort="updated_date,statement_id")


class Coupons(Stream):
    """Coupons stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/coupons
    """

    tap_stream_id = "coupons"
    key_properties = ["coupon_id", "company_id"]
    valid_replication_keys: Sequence[str] = []
    replication_key = None
    replication_method = "FULL_TABLE"
    transformer_class = RecordTransformer
    request_handler = RequestHandler("/coupons", sort="id")


class Usages(Stream):
    """Usages stream

    Ordway Documentation: https://ordwaylabs.api-docs.io/v1/models/usages
    """

    tap_stream_id = "usages"
    key_properties = ["usage_id", "company_id"]
    transformer_class = RecordTransformer
    request_handler = RequestHandler("/usages", sort="updated_date,id")


AVAILABLE_STREAMS: Dict[str, Union[Type[Stream], Type["Substream"]]] = {
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
