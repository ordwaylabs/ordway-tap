from ordway_tap.api_sync.api import get_index_data
from ordway_tap.record.order import map_order_response


def sync(timestamp):
    for order_response in get_index_data('/api/v1/orders', params={'updated_date>': timestamp}):
        for order_with_lines in map_order_response(order_response):
            yield order_with_lines
