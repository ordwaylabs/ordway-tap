from ordway_tap.api_sync.utils import get_company_id, convert_to_decimal


def map_refund_response(refund_response):
    return {
        'company_id': get_company_id(),
        'refund_id': refund_response.get('id'),
        'customer_id': refund_response.get('customer_id'),
        'refund_date': refund_response.get('refund_date'),
        'refund_amount': convert_to_decimal(refund_response.get('refund_amount')),
        'refund_type': refund_response.get('refund_type'),
        'notes': refund_response.get('notes'),
        'status': refund_response.get('refund_status'),
        'payment_id': refund_response.get('payment_id'),
        'currency': refund_response.get('currency'),
        'reference_number': refund_response.get('reference_number'),
        'created_by': refund_response.get('created_by'),
        'updated_by': refund_response.get('updated_by'),
        'created_date': refund_response.get('created_date'),
        'updated_date': refund_response.get('updated_date'),
        'custom_fields': refund_response.get('custom_fields')
    }
