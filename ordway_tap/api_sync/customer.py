from ordway_tap.api_sync.api import get_index_data
from ordway_tap.api_sync.utils import print_record
from ordway_tap.record.customer import map_customer_response


def sync(timestamp):
    for customer_response in get_index_data('/api/v1/customers', params={'updated_date>': timestamp}):
        print_record('customers', map_customer_response(customer_response))
