from ordway_tap.api_sync.api import get_index_data
from ordway_tap.api_sync.utils import get_company_id, print_record, convert_to_decimal


def sync():
    for invoice_response in get_index_data('/api/v1/invoices'):
        for invoice in map_invoice_response(invoice_response):
            print_record('invoices', invoice)


def map_invoice_response(invoice_response):
    invoice_lines = invoice_response.get('line_items', [])
    for invoice_line in invoice_lines:
        yield {
            'company_id': get_company_id(),
            'invoice_id': invoice_response.get('id'),
            'invoice_line_no': invoice_line.get('line_no'),
            'customer_id': invoice_response.get('customer_id'),
            'billing_contact': invoice_response.get('billing_contact'),
            'shipping_contact': invoice_response.get('shipping_contact'),
            'customer_name': invoice_response.get('customer_name'),
            'invoice_date': invoice_response.get('invoice_date'),
            'due_date': invoice_response.get('due_date'),
            'billing_run_id': invoice_response.get('billing_run_id'),
            'subtotal': invoice_response.get('subtotal'),
            'invoice_tax': invoice_response.get('invoice_tax'),
            'invoice_amount': invoice_response.get('invoice_amount'),
            'paid_amount': invoice_response.get('paid_amount'),
            'balance': invoice_response.get('balance'),
            'status': invoice_response.get('status'),
            'notes': invoice_response.get('notes'),
            'currency': invoice_response.get('currency'),
            'payment_terms': invoice_response.get('payment_terms'),
            'subscription_id': invoice_line.get('subscription_id'),
            'product_id': invoice_line.get('product_id'),
            'product_name': invoice_line.get('product_name'),
            'plan_id': invoice_line.get('plan_id'),
            'plan_name': invoice_line.get('plan_name'),
            'charge_id': invoice_line.get('charge_id'),
            'subscription_line_id': invoice_line.get('subscription_line_id'),
            'charge_name': invoice_line.get('charge_name'),
            'charge_type': invoice_line.get('charge_type'),
            'description': invoice_line.get('description'),
            'start_date': invoice_line.get('start_date'),
            'end_date': invoice_line.get('end_date'),
            'unit_price': invoice_line.get('unit_price'),
            'list_price': invoice_line.get('list_price'),
            'list_price_base': invoice_line.get('list_price_base'),
            'taxable': invoice_line.get('taxable'),
            'quantity': invoice_line.get('quantity'),
            'discount': invoice_line.get('discount'),
            'effective_price': invoice_line.get('effective_price'),
            'line_tax': invoice_line.get('line_tax'),
            'tax_lines': invoice_line.get('tax_lines'),
            'applied_tiers': invoice_line.get('applied_tiers'),
            'created_by': invoice_response.get('created_by'),
            'updated_by': invoice_response.get('updated_by'),
            'created_date': invoice_response.get('created_date'),
            'updated_date': invoice_response.get('updated_date'),
            'custom_fields': invoice_response.get('line_no'),
            'line_custom_fields': invoice_line.get('custom_fields')
        }
