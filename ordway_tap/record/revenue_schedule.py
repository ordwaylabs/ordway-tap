from ordway_tap.api_sync.utils import get_company_id, convert_to_decimal


def map_rs_response(rs_response):
    return {
        'company_id': get_company_id(),
        'revenue_schedule_id': rs_response.get('id'),
        'customer_id': rs_response.get('customer_id'),
        'source_transaction': rs_response.get('source_transaction'),
        'product_id': rs_response.get('product_id'),
        'product_name': rs_response.get('product_name'),
        'plan_id': rs_response.get('plan_id'),
        'plan_name': rs_response.get('plan_name'),
        'charge_id': rs_response.get('charge_id'),
        'charge_name': rs_response.get('charge_name'),
        'charge_type': rs_response.get('charge_type'),
        'charge_timing': rs_response.get('charge_timing'),
        'total_revenue': convert_to_decimal(rs_response.get('total_revenue')),
        'recognized_revenue': convert_to_decimal(rs_response.get('recognized_revenue')),
        'unrecognized_revenue': convert_to_decimal(rs_response.get('unrecognized_revenue')),
        'start_date': rs_response.get('start_date'),
        'end_date': rs_response.get('end_date'),
        'schedule_lines': rs_response.get('schedule_lines'),
        'created_by': rs_response.get('created_by'),
        'updated_by': rs_response.get('updated_by'),
        'created_date': rs_response.get('created_date'),
        'updated_date': rs_response.get('updated_date')
    }
