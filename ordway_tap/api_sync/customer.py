from ordway_tap.api_sync.api import get_index_data
from ordway_tap.api_sync.utils import get_company_id, print_record, convert_to_decimal


def sync():
    for customer_response in get_index_data('/api/v1/customers'):
        print_record('customers', map_customer_response(customer_response))


def map_customer_response(customer_response):
    return {
        'company_id': get_company_id(),
        'customer_id': customer_response['id'],
        'name': customer_response['name'],
        'customer_type': customer_response['customer_type'],
        'description': customer_response['description'],
        'parent_customer': customer_response['parent_customer'],
        'website': customer_response['website'],
        'payment_terms': customer_response['payment_terms'],
        'billing_cycle_day': customer_response['billing_cycle_day'],
        'billing_contact_id': customer_response['billing_contact_id'],
        'shipping_contact_id': customer_response['shipping_contact_id'],
        'status': customer_response['status'],
        'billing_batch': customer_response['customer_type'],
        'tax_exempt': customer_response['tax_exempt'],
        'balance': convert_to_decimal(customer_response['balance']),
        'auto_pay': customer_response['auto_pay'],
        'currency': customer_response['currency'],
        'edit_auto_pay': customer_response['edit_auto_pay'],
        'price_book_id': customer_response['price_book_id'],
        'cmrr': convert_to_decimal(customer_response['cmrr']),
        'discounted_cmrr': convert_to_decimal(customer_response['discounted_cmrr']),
        'delivery_preferences': customer_response['delivery_preferences'],
        'created_by': customer_response['created_by'],
        'updated_by': customer_response['updated_by'],
        'created_date': customer_response['created_date'],
        'updated_date': customer_response['updated_date'],
        'custom_fields': customer_response['custom_fields']
    }
