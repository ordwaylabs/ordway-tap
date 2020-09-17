from ordway_tap.api_sync import invoice, subscription, customer

sync_methods = {
    'customers': customer.sync,
    'subscriptions': subscription.sync,
    'invoices': invoice.sync
}


def sync(stream):
    sync_methods[stream]()
