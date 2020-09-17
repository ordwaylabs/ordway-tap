from ordway_tap.api_sync.api import get_index_data
from ordway_tap.api_sync.utils import get_company_id, print_record, convert_to_decimal


def sync():
    for subscription_response in get_index_data('/api/v1/subscriptions'):
        for subscription_with_plan in map_subscription_response(subscription_response):
            print_record('subscriptions', subscription_with_plan)


def map_subscription_response(subscription_response):
    subscription_plans = subscription_response.get('plans')
    for subscription_plan in subscription_plans:
        yield {
            "company_id": get_company_id(),
            "subscription_id": subscription_response.get('id'),
            "subscription_line_id": subscription_plan.get('subscription_line_id'),
            "customer_id": subscription_response.get('customer_id'),
            "bill_contact_id": subscription_response.get('bill_contact_id'),
            "shipping_contact_id": subscription_response.get('shipping_contact_id'),
            "status": subscription_response.get('status'),
            "billing_start_date": subscription_response.get('billing_start_date'),
            "service_start_date": subscription_response.get('service_start_date'),
            "order_placed_at": subscription_response.get('order_placed_at'),
            "contract_effective_date": subscription_response.get('contract_effective_date'),
            "cancellation_date": subscription_response.get('cancellation_date'),
            "auto_renew": subscription_response.get('auto_renew'),
            "currency": subscription_response.get('currency'),
            "payment_terms": subscription_response.get('payment_terms'),
            "cmrr": convert_to_decimal(subscription_response.get('cmrr')),
            "discounted_cmrr": convert_to_decimal(subscription_response.get('discounted_cmrr')),
            "separate_invoice": subscription_response.get('separate_invoice'),
            "notes": subscription_response.get('notes'),
            "version": subscription_response.get('version'),
            "version_type": subscription_response.get('version_type'),
            "contract_term": subscription_response.get('contract_term'),
            "renewal_term": subscription_response.get('renewal_term'),
            "tcv": convert_to_decimal(subscription_response.get('tcv')),
            "product_id": subscription_plan.get('product_id'),
            "product_name": subscription_plan.get('product_name'),
            "plan_name": subscription_plan.get('plan_name'),
            "charge_id": subscription_plan.get('charge_id'),
            "charge_name": subscription_plan.get('charge_name'),
            "pricing_model": subscription_plan.get('pricing_model'),
            "list_price": convert_to_decimal(subscription_plan.get('list_price')),
            "price_base": subscription_plan.get('price_base'),
            "quantity": convert_to_decimal(subscription_plan.get('quantity')),
            "included_units": convert_to_decimal(subscription_plan.get('included_units')),
            "discount": convert_to_decimal(subscription_plan.get('discount')),
            "effective_price": convert_to_decimal(subscription_plan.get('effective_price')),
            "charge_type": subscription_plan.get('charge_type'),
            "billing_period": subscription_plan.get('billing_period'),
            "unit_of_measure": subscription_plan.get('unit_of_measure'),
            "current_period_start_date": subscription_plan.get('current_period_start_date'),
            "current_period_end_date": subscription_plan.get('current_period_end_date'),
            "billing_schedule_id": subscription_plan.get('billing_schedule_id'),
            "revenue_schedule_id": subscription_plan.get('revenue_schedule_id'),
            "charge_timing": subscription_plan.get('charge_timing'),
            "billing_period_start_alignment": subscription_plan.get('billing_period_start_alignment'),
            "billing_day": subscription_plan.get('billing_day'),
            "prorate_partial_periods": subscription_plan.get('prorate_partial_periods'),
            "backcharge_current_period": subscription_plan.get('backcharge_current_period'),
            "prepayment_periods": convert_to_decimal(subscription_plan.get('prepayment_periods')),
            "renewal_increment_percent": convert_to_decimal(subscription_plan.get('renewal_increment_percent')),
            "override_renewal_increment_percent": subscription_plan.get('override_renewal_increment_percent'),
            "charge_end_date": subscription_plan.get('charge_end_date'),
            "created_by": subscription_response.get('created_by'),
            "updated_by": subscription_response.get('updated_by'),
            "created_date": subscription_response.get('created_date'),
            "updated_date": subscription_response.get('updated_date'),
            "custom_fields": subscription_response.get('custom_fields'),
            "charge_custom_fields": subscription_plan.get('custom_fields')
        }

