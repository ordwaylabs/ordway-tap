from ordway_tap.api_sync.utils import get_company_id, convert_to_decimal, format_boolean


def map_payment_response(payment_response):
    return {
        'company_id': get_company_id(),
        'payment_id': payment_response.get('id'),
        'customer_id': payment_response.get('customer_id'),
        'payment_date': payment_response.get('payment_date'),
        'notes': payment_response.get('notes'),
        'status': payment_response.get('status'),
        'payment_amount': convert_to_decimal(payment_response.get('payment_amount')),
        'fee_amount': convert_to_decimal(payment_response.get('fee_amount')),
        'applied_amount': convert_to_decimal(payment_response.get('applied_amount')),
        'unapplied_amount': convert_to_decimal(payment_response.get('unapplied_amount')),
        'refunded_amount': convert_to_decimal(payment_response.get('refunded_amount')),
        'payment_type': payment_response.get('payment_type'),
        'payment_method': payment_response.get('payment_method'),
        'reference_number': payment_response.get('reference_number'),
        'auto_apply': format_boolean(payment_response.get('auto_apply')),
        'retried_attempts': payment_response.get('retried_attempts'),
        'invoices': payment_response.get('invoices'),
        'refunds': payment_response.get('refunds'),
        'gl_account': payment_response.get('gl_account'),
        'created_by': payment_response.get('created_by'),
        'updated_by': payment_response.get('updated_by'),
        'created_date': payment_response.get('created_date'),
        'updated_date': payment_response.get('updated_date'),
        'custom_fields': payment_response.get('custom_fields')
    }
