from ordway_tap.api_sync.api import get_index_data
from ordway_tap.record.customer import map_customer_response


def sync(timestamp):
    for customer_response in get_index_data('/api/v1/customers', params={'updated_date>': timestamp}):
        yield map_customer_response(customer_response)
