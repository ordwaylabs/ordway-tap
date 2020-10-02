from ordway_tap.api_sync.utils import get_company_id, convert_to_decimal


def map_order_response(order_response):
    order_lines = order_response.get('line_items', [])
    for order_line in order_lines:
        yield {
            'company_id': get_company_id(),
            'order_id': order_response.get('id'),
            'order_line_no': order_line.get('line_no'),
            'customer_id': order_response.get('customer_id'),
            'invoice_id': order_response.get('invoice_id'),
            'order_date': order_response.get('order_date'),
            'status': order_response.get('status'),
            'order_amount': convert_to_decimal(order_response.get('order_amount')),
            'separate_invoice': order_response.get('separate_invoice'),
            'currency': order_response.get('currency'),
            'notes': order_response.get('notes'),
            'product_id': order_line.get('product_id'),
            'product_name': order_line.get('product_name'),
            'description': order_line.get('description'),
            'unit_price': convert_to_decimal(order_line.get('unit_price')),
            'quantity': convert_to_decimal(order_line.get('quantity')),
            'discount': convert_to_decimal(order_line.get('discount')),
            'effective_price': convert_to_decimal(order_line.get('effective_price')),
            'created_by': order_response.get('created_by'),
            'updated_by': order_response.get('updated_by'),
            'created_date': order_response.get('created_date'),
            'updated_date': order_response.get('updated_date'),
            'custom_fields': order_response.get('custom_fields')
        }
