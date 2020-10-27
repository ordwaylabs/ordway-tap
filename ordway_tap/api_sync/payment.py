from ordway_tap.api_sync.api import get_index_data
from ordway_tap.record.payment import map_payment_response


def sync(timestamp):
    for payment_response in get_index_data('/api/v1/payments', params={'updated_date>': timestamp}):
        yield map_payment_response(payment_response)
