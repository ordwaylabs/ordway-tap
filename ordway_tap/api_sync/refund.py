from ordway_tap.api_sync.api import get_index_data
from ordway_tap.record.refund import map_refund_response


def sync(timestamp):
    for refund_response in get_index_data('/api/v1/refunds', params={'updated_date>': timestamp}):
        yield map_refund_response(refund_response)
