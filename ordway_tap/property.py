from singer.metadata import get_standard_metadata, to_map as mdata_to_map, write as mdata_write, to_list as mdata_to_list

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


def get_stream_metadata(tap_stream_id, schema_dict):
    metadata = get_standard_metadata(
        schema=schema_dict,
        key_properties=KEY_PROPERTIES_MAP.get(tap_stream_id),
        valid_replication_keys=["updated_date"],
    )

    metadata = mdata_to_map(metadata)

    for field_name in schema_dict['properties'].keys():
        # selected-by-default doesn't currently work as intended. 
        # A PR has been made for a fix, but it hasn't been merged yet:
        # https://github.com/singer-io/singer-python/pull/121
        # Writing regardless for when PR is merged.
        metadata = mdata_write(
            metadata, ("properties", field_name), "selected-by-default", True
        )

    metadata = mdata_write(metadata, (), "selected", True)

    return mdata_to_list(metadata)
    