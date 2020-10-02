from ordway_tap.api_sync.api import get_index_data
from ordway_tap.api_sync.utils import print_record
from ordway_tap.record.credit import map_credit_response


def sync():
    for credit_response in get_index_data('/api/v1/credits'):
        print_record('credits', map_credit_response(credit_response))
