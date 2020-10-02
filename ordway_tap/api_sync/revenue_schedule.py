from ordway_tap.api_sync.api import get_index_data
from ordway_tap.api_sync.utils import print_record
from ordway_tap.record.revenue_schedule import map_rs_response


def sync():
    for rs_response in get_index_data('/api/v1/revenue_schedules'):
        print_record('revenue_schedules', map_rs_response(rs_response))
