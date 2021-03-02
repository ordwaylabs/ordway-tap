from typing import Any, Dict
from ..base import DataContext
from .base import RecordTransformer


class BillingScheduleTransformer(RecordTransformer):
    def pre_transform(self, data: Dict[str, Any], context: DataContext):
        super().pre_transform(data, context)

        key_metrics = data.get("key_metrics", {})
        data.update(
            {
                "monthly_recurring_revenue": key_metrics.get(
                    "monthly_recurring_revenue"
                ),
                "annual_contract_revenue": key_metrics.get("annual_contract_revenue"),
                "total_contract_revenue": key_metrics.get("total_contract_revenue"),
                "amount_invoiced": key_metrics.get("amount_invoiced"),
            }
        )

        return data


class CustomerTransformer(RecordTransformer):
    def pre_transform(self, data: Dict[str, Any], context: DataContext):
        super().pre_transform(data, context)

        data["billing_batch"] = data.get("customer_type")
        del data["customer_type"]

        return data


class InvoiceTransformer(RecordTransformer):
    def pre_transform(self, data: Dict[str, Any], context: DataContext):
        super().pre_transform(data, context)

        invoice_lines = data.get("line_items", [])

        for invoice_line in invoice_lines:
            invoice_line.update(
                {
                    "invoice_id": data.get("invoice_id"),
                    "company_id": data.get("company_id"),
                    "invoice_line_no": invoice_line.get("line_no"),
                    "customer_id": data.get("customer_id"),
                    "billing_contact": data.get("billing_contact"),
                    "shipping_contact": data.get("shipping_contact"),
                    "customer_name": data.get("customer_name"),
                    "invoice_date": data.get("invoice_date"),
                    "due_date": data.get("due_date"),
                    "billing_run_id": data.get("billing_run_id"),
                    "subtotal": data.get("subtotal"),
                    "invoice_tax": data.get("invoice_tax"),
                    "invoice_amount": data.get("invoice_amount"),
                    "paid_amount": data.get("paid_amount"),
                    "balance": data.get("balance"),
                    "status": data.get("status"),
                    "notes": data.get("notes"),
                    "currency": data.get("currency"),
                    "payment_terms": data.get("payment_terms"),
                    "start_date": data.get("start_date"),
                    "end_date": data.get("end_date"),
                    "applied_tiers": invoice_line.get("applied_tiers"),
                    "custom_fields": data.get("custom_fields"),
                    "line_custom_fields": invoice_line.get("custom_fields"),
                    "updated_date": data.get("updated_date"),
                    "created_date": data.get("created_date"),
                    "created_by": data.get("created_by"),
                    "updated_by": data.get("updated_by"),
                }
            )

            yield invoice_line


class OrderTransformer(RecordTransformer):
    def pre_transform(self, data: Dict[str, Any], context: DataContext):
        super().pre_transform(data, context)

        order_lines = data.get("line_items", [])
        for order_line in order_lines:
            order_line.update(
                {
                    "order_id": data.get("order_id"),
                    "company_id": data.get("company_id"),
                    "order_line_no": order_line.get("line_no"),
                    "customer_id": data.get("customer_id"),
                    "invoice_id": data.get("invoice_id"),
                    "order_date": data.get("order_date"),
                    "status": data.get("status"),
                    "order_amount": data.get("order_amount"),
                    "separate_invoice": data.get("separate_invoice"),
                    "currency": data.get("currency"),
                    "notes": data.get("notes"),
                    "created_by": data.get("created_by"),
                    "updated_by": data.get("updated_by"),
                    "created_date": data.get("created_date"),
                    "updated_date": data.get("updated_date"),
                    "custom_fields": data.get("custom_fields"),
                }
            )

            del order_line["line_no"]

            yield order_line


# May want to eventually change to
# simply keep subscriptions since we
# already have a plan stream
class SubscriptionTransformer(RecordTransformer):
    def pre_transform(self, data: Dict[str, Any], context: DataContext):
        super().pre_transform(data, context)

        subscription_plans = data.get("plans", [])

        for subscription_plan in subscription_plans:
            subscription_plan.update(
                {
                    "subscription_id": data.get("subscription_id"),
                    "company_id": data.get("company_id"),
                    "customer_id": data.get("customer_id"),
                    "bill_contact_id": data.get("bill_contact_id"),
                    "shipping_contact_id": data.get("shipping_contact_id"),
                    "status": data.get("status"),
                    "billing_start_date": data.get("billing_start_date"),
                    "service_start_date": data.get("service_start_date"),
                    "order_placed_at": data.get("order_placed_at"),
                    "contract_effective_date": data.get("contract_effective_date"),
                    "cancellation_date": data.get("cancellation_date"),
                    "auto_renew": data.get("auto_renew"),
                    "currency": data.get("currency"),
                    "payment_terms": data.get("payment_terms"),
                    "cmrr": data.get("cmrr"),
                    "discounted_cmrr": data.get("discounted_cmrr"),
                    "separate_invoice": data.get("separate_invoice"),
                    "notes": data.get("notes"),
                    "version": data.get("version"),
                    "version_type": data.get("version_type"),
                    "contract_term": data.get("contract_term"),
                    "renewal_term": data.get("renewal_term"),
                    "tcv": data.get("tcv"),
                    "created_by": data.get("created_by"),
                    "updated_by": data.get("updated_by"),
                    "created_date": data.get("created_date"),
                    "updated_date": data.get("updated_date"),
                    "custom_fields": data.get("custom_fields"),
                    "charge_custom_fields": subscription_plan.get("custom_fields"),
                }
            )

            yield subscription_plan
