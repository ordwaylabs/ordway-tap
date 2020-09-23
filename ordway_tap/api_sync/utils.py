import decimal
import singer
from inflection import underscore
import ordway_tap.configs


def get_company_id():
    api_credentials = ordway_tap.configs.api_credentials
    return underscore(api_credentials['x_company'])


def print_record(stream, record):
    singer.write_records(stream, [record])


def convert_to_decimal(value):
    try:
        return decimal.Decimal(value)
    except (decimal.InvalidOperation, TypeError) as error:
        return 0


def format_date_string(value):
    if value == '-':
        return None
    return value or None


def format_array(value):
    return value or None
