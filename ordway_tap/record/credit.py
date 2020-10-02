from ordway_tap.api_sync.utils import get_company_id, convert_to_decimal


def map_credit_response(credit_response):
    return {
        'company_id': get_company_id(),
        'credit_id': credit_response.get('id'),
        'customer_id': credit_response.get('customer_id'),
        'credit_date': credit_response.get('credit_date'),
        'notes': credit_response.get('notes'),
        'status': credit_response.get('status'),
        'credit_amount': convert_to_decimal(credit_response.get('credit_amount')),
        'applied_amount': convert_to_decimal(credit_response.get('applied_amount')),
        'unapplied_amount': convert_to_decimal(credit_response.get('unapplied_amount')),
        'currency': credit_response.get('currency'),
        'reference_number': credit_response.get('reference_number'),
        'auto_apply': credit_response.get('auto_apply'),
        'gl_account': credit_response.get('gl_account'),
        'invoices': credit_response.get('invoices'),
        'refunds': credit_response.get('refunds'),
        'created_by': credit_response.get('created_by'),
        'updated_by': credit_response.get('updated_by'),
        'created_date': credit_response.get('created_date'),
        'updated_date': credit_response.get('updated_date'),
        'custom_fields': credit_response.get('custom_fields')
    }
