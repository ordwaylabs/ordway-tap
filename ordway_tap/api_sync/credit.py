from ordway_tap.api_sync.api import get_index_data
from ordway_tap.record.credit import map_credit_response


def sync(timestamp):
    for credit_response in get_index_data('/api/v1/credits', params={'updated_date>': timestamp}):
        yield map_credit_response(credit_response)
