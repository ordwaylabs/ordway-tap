from ordway_tap.api_sync.api import get_index_data
from ordway_tap.api_sync.utils import print_record
from ordway_tap.record.product import map_product_response


def sync(timestamp):
    for product_response in get_index_data('/api/v1/products', params={'updated_date>': timestamp}):
        print_record('products', map_product_response(product_response))
