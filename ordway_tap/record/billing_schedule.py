from ordway_tap.api_sync.utils import get_company_id, convert_to_decimal


def map_bs_response(bs_response):
    key_metrics = bs_response.get("key_metrics", {})
    return {
        "company_id": get_company_id(),
        "billing_schedule_id": bs_response.get("id"),
        "customer_id": bs_response.get("customer_id"),
        "customer_name": bs_response.get("customer_name"),
        "subscription_id": bs_response.get("subscription_id"),
        "product_id": bs_response.get("product_id"),
        "product_name": bs_response.get("product_name"),
        "charge_id": bs_response.get("charge_id"),
        "charge_name": bs_response.get("charge_name"),
        "charge_type": bs_response.get("charge_type"),
        "charge_timing": bs_response.get("charge_timing"),
        "start_date": bs_response.get("start_date"),
        "end_date": bs_response.get("end_date"),
        "monthly_recurring_revenue": convert_to_decimal(
            key_metrics.get("monthly_recurring_revenue")
        ),
        "annual_contract_revenue": convert_to_decimal(
            key_metrics.get("annual_contract_revenue")
        ),
        "total_contract_revenue": convert_to_decimal(
            key_metrics.get("total_contract_revenue")
        ),
        "amount_invoiced": convert_to_decimal(key_metrics.get("amount_invoiced")),
        "currency": bs_response.get("currency"),
        "schedule_lines": bs_response.get("schedule_lines"),
        "created_by": bs_response.get("created_by"),
        "updated_by": bs_response.get("updated_by"),
        "created_date": bs_response.get("created_date"),
        "updated_date": bs_response.get("updated_date"),
    }
