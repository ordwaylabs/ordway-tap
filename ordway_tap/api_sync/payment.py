from ordway_tap.api_sync.api import get_index_data
from ordway_tap.api_sync.utils import print_record
from ordway_tap.record.payment import map_payment_response


def sync():
    for payment_response in get_index_data('/api/v1/payments'):
        print_record('payments', map_payment_response(payment_response))
