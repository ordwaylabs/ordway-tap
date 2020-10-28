from ordway_tap.api_sync.api import get_index_data
from ordway_tap.api_sync.utils import print_record
from ordway_tap.record.subscription import map_subscription_response


def sync(timestamp):
    for subscription_response in get_index_data(
        "/api/v1/subscriptions", params={"updated_date>": timestamp}
    ):
        for subscription_with_plan in map_subscription_response(subscription_response):
            print_record("subscriptions", subscription_with_plan)
