from ordway_tap.api_sync.api import get_index_data
from ordway_tap.api_sync.utils import print_record
from ordway_tap.record.billing_schedule import map_bs_response


def sync(timestamp):
    for bs_response in get_index_data('/api/v1/billing_schedules', params={'updated_date>': timestamp}):
        print_record('billing_schedules', map_bs_response(bs_response))
