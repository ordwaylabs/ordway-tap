from singer.transform import Transformer
from singer.metadata import to_map as mdata_to_map
from ordway_tap.api_sync.utils import print_record
from ordway_tap.api_sync import invoice, subscription, customer, payment, credit, refund, billing_schedule,\
                                revenue_schedule, product, order

sync_methods = {
    'customers': customer.sync,
    'subscriptions': subscription.sync,
    'invoices': invoice.sync,
    'payments': payment.sync,
    'credits': credit.sync,
    'refunds': refund.sync,
    'billing_schedules': billing_schedule.sync,
    'revenue_schedules': revenue_schedule.sync,
    'products': product.sync,
    'orders': order.sync
}

def sync(stream, timestamp):
    with Transformer() as transformer:
        for record in sync_methods[stream.tap_stream_id](timestamp):
            transformed_record = transformer.transform(
                record,
                stream.schema.to_dict(),
                metadata=mdata_to_map(stream.metadata)
            )
            
            print_record(stream.tap_stream_id, transformed_record)
