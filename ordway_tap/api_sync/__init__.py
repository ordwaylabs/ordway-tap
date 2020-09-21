from ordway_tap.api_sync import invoice, subscription, customer, payment, credit, refund

sync_methods = {
    'customers': customer.sync,
    'subscriptions': subscription.sync,
    'invoices': invoice.sync,
    'payments': payment.sync,
    'credits': credit.sync,
    'refunds': refund.sync
}


def sync(stream):
    sync_methods[stream]()
