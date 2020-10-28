from ordway_tap.api_sync import (
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

sync_methods = {
    "customers": customer.sync,
    "subscriptions": subscription.sync,
    "invoices": invoice.sync,
    "payments": payment.sync,
    "credits": credit.sync,
    "refunds": refund.sync,
    "billing_schedules": billing_schedule.sync,
    "revenue_schedules": revenue_schedule.sync,
    "products": product.sync,
    "orders": order.sync,
}


def sync(stream, timestamp):
    sync_methods[stream](timestamp)
