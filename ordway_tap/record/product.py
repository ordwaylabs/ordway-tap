from ordway_tap.api_sync.utils import get_company_id, convert_to_decimal


def map_product_response(product_response):
    return {
        "company_id": get_company_id(),
        "product_id": product_response.get("id"),
        "name": product_response.get("name"),
        "sku": product_response.get("sku"),
        "status": product_response.get("status"),
        "taxable": product_response.get("taxable"),
        "description": product_response.get("description"),
        "price": convert_to_decimal(product_response.get("price")),
        "currency": product_response.get("currency"),
        "income_account": product_response.get("income_account"),
        "deferred_revenue_enabled": product_response.get("deferred_revenue_enabled"),
        "revenue_rule_id": product_response.get("revenue_rule_id"),
        "recognition_start_date": product_response.get("recognition_start_date"),
        "order_journal_entry_enabled": product_response.get(
            "order_journal_entry_enabled"
        ),
        "order_debit_account": product_response.get("order_debit_account"),
        "order_credit_account": product_response.get("order_credit_account"),
        "invoice_journal_entry_enabled": product_response.get(
            "invoice_journal_entry_enabled"
        ),
        "invoice_debit_account": product_response.get("invoice_debit_account"),
        "invoice_credit_account": product_response.get("invoice_credit_account"),
        "revenue_journal_entry_enabled": product_response.get(
            "revenue_journal_entry_enabled"
        ),
        "revenue_debit_account": product_response.get("revenue_debit_account"),
        "revenue_credit_account": product_response.get("revenue_credit_account"),
        "created_by": product_response.get("created_by"),
        "updated_by": product_response.get("updated_by"),
        "created_date": product_response.get("created_date"),
        "updated_date": product_response.get("updated_date"),
        "custom_fields": product_response.get("custom_fields"),
    }
