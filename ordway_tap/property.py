KEY_PROPERTIES_MAP = {
    'customers': ['company_id', 'customer_id'],
    'subscriptions': ['company_id', 'subscription_id', 'subscription_line_id'],
    'invoices': ['company_id', 'invoice_id', 'invoice_line_no'],
    'payments': ['company_id', 'payment_id'],
    'credits': ['company_id', 'credit_id'],
    'refunds': ['company_id', 'refund_id'],
    'billing_schedules': ['company_id', 'billing_schedule_id'],
    'revenue_schedules': ['company_id', 'revenue_schedule_id'],
    'products': ['company_id', 'product_id'],
    'orders': ['company_id', 'order_id', 'order_line_no']
}


def get_key_properties(stream):
    key_properties = KEY_PROPERTIES_MAP[stream]
    if not key_properties:
        return None
    return key_properties


def get_stream_metadata(schema):
    metadata = [{
        'metadata': {
            'inclusion': 'automatic',
            'selected': 'true'
        },
        'breadcrumb': []
    }]
    # schema_dict = schema.to_dict()
    # for column in schema_dict['properties'].keys():
    #     metadata.append({
    #       'metadata': {
    #         'inclusion': 'automatic'
    #       },
    #       'breadcrumb': ['properties', column]
    #     })
    return metadata
