from typing import TYPE_CHECKING, Union, Dict, List, Any
from .base import RecordTransformer
from ..utils import convert_to_decimal, format_boolean, format_date_string, format_array
from ..base import DataContext

if TYPE_CHECKING:
    from datetime import datetime


class BillingScheduleTransformer(RecordTransformer):
    def pre_transform(
        self, data: Union[Dict[str, Any], List[Dict[str, Any]]], context: DataContext
    ):
        super().pre_transform(data, context)

        key_metrics = data.get("key_metrics", {})
        data.update(
            {
                "monthly_recurring_revenue": convert_to_decimal(
                    key_metrics.get("monthly_recurring_revenue")
                ),
                "annual_contract_revenue": convert_to_decimal(
                    key_metrics.get("annual_contract_revenue")
                ),
                "total_contract_revenue": convert_to_decimal(
                    key_metrics.get("total_contract_revenue")
                ),
                "amount_invoiced": convert_to_decimal(
                    key_metrics.get("amount_invoiced")
                ),
            }
        )

        return data


class CreditTransformer(RecordTransformer):
    def pre_transform(
        self, data: Union[Dict[str, Any], List[Dict[str, Any]]], context: DataContext
    ):
        super().pre_transform(data, context)

        data.update(
            {
                "credit_amount": convert_to_decimal(data.get("credit_amount")),
                "applied_amount": convert_to_decimal(data.get("applied_amount")),
                "unapplied_amount": convert_to_decimal(data.get("unapplied_amount")),
                "auto_apply": format_boolean(data.get("auto_apply")),
            }
        )

        return data


class CustomerTransformer(RecordTransformer):
    def pre_transform(
        self, data: Union[Dict[str, Any], List[Dict[str, Any]]], context: DataContext
    ):
        super().pre_transform(data, context)

        data.update(
            {
                "billing_contact_id": data.get("billing_contact_id", ""),
                "shipping_contact_id": data.get("shipping_contact_id", ""),
                "billing_batch": data["customer_type"],
                "balance": convert_to_decimal(data["balance"]),
                "cmrr": convert_to_decimal(data["cmrr"]),
                "discounted_cmrr": convert_to_decimal(data["discounted_cmrr"]),
            }
        )

        del data["customer_type"]

        return data


class InvoiceTransformer(RecordTransformer):
    def pre_transform(
        self, data: Union[Dict[str, Any], List[Dict[str, Any]]], context: DataContext
    ):
        invoice_lines = data.get("line_items", [])

        for invoice_line in invoice_lines:
            super().pre_transform(invoice_line, context)

            invoice_line.update(
                {
                    "invoice_line_no": invoice_line.get("line_no"),
                    "customer_id": data.get("customer_id"),
                    "billing_contact": data.get("billing_contact"),
                    "shipping_contact": data.get("shipping_contact"),
                    "customer_name": data.get("customer_name"),
                    "invoice_date": data.get("invoice_date"),
                    "due_date": data.get("due_date"),
                    "billing_run_id": data.get("billing_run_id"),
                    "subtotal": convert_to_decimal(data.get("subtotal")),
                    "invoice_tax": convert_to_decimal(data.get("invoice_tax")),
                    "invoice_amount": convert_to_decimal(data.get("invoice_amount")),
                    "paid_amount": convert_to_decimal(data.get("paid_amount")),
                    "balance": convert_to_decimal(data.get("balance")),
                    "status": data.get("status"),
                    "notes": data.get("notes"),
                    "currency": data.get("currency"),
                    "payment_terms": data.get("payment_terms"),
                    "start_date": format_date_string(invoice_line.get("start_date")),
                    "end_date": format_date_string(invoice_line.get("end_date")),
                    "unit_price": convert_to_decimal(invoice_line.get("unit_price")),
                    "list_price": convert_to_decimal(invoice_line.get("list_price")),
                    "quantity": convert_to_decimal(invoice_line.get("quantity")),
                    "discount": convert_to_decimal(invoice_line.get("discount")),
                    "effective_price": convert_to_decimal(
                        invoice_line.get("effective_price")
                    ),
                    "line_tax": convert_to_decimal(invoice_line.get("line_tax")),
                    "applied_tiers": format_array(invoice_line.get("applied_tiers")),
                    "custom_fields": data.get(
                        "line_no"
                    ),  # FIXME Is this correct? Doesn't seem intentional.
                    "line_custom_fields": invoice_line.get("custom_fields"),
                    "updated_date": data.get("updated_date"),
                    "created_date": data.get("created_date"),
                    "created_by": data.get("created_by"),
                    "updated_by": data.get("updated_by"),
                }
            )

            del invoice_line["line_no"]

            yield invoice_line


class OrderTransformer(RecordTransformer):
    def pre_transform(
        self, data: Union[Dict[str, Any], List[Dict[str, Any]]], context: DataContext
    ):
        order_lines = data.get("line_items", [])
        for order_line in order_lines:
            super().pre_transform(order_line, context)

            order_line.update(
                {
                    "order_line_no": order_line.get("line_no"),
                    "customer_id": data.get("customer_id"),
                    "invoice_id": data.get("invoice_id"),
                    "order_date": data.get("order_date"),
                    "status": data.get("status"),
                    "order_amount": convert_to_decimal(data.get("order_amount")),
                    "separate_invoice": data.get("separate_invoice"),
                    "currency": data.get("currency"),
                    "notes": data.get("notes"),
                    "unit_price": convert_to_decimal(order_line.get("unit_price")),
                    "quantity": convert_to_decimal(order_line.get("quantity")),
                    "discount": convert_to_decimal(order_line.get("discount")),
                    "effective_price": convert_to_decimal(
                        order_line.get("effective_price")
                    ),
                    "created_by": data.get("created_by"),
                    "updated_by": data.get("updated_by"),
                    "created_date": data.get("created_date"),
                    "updated_date": data.get("updated_date"),
                    "custom_fields": data.get("custom_fields"),
                }
            )

            del order_line["line_no"]

            yield order_line


class PaymentTransformer(RecordTransformer):
    def pre_transform(
        self, data: Union[Dict[str, Any], List[Dict[str, Any]]], context: DataContext
    ):
        super().pre_transform(data, context)

        data.update(
            {
                "payment_amount": convert_to_decimal(data.get("payment_amount")),
                "fee_amount": convert_to_decimal(data.get("fee_amount")),
                "applied_amount": convert_to_decimal(data.get("applied_amount")),
                "unapplied_amount": convert_to_decimal(data.get("unapplied_amount")),
                "refunded_amount": convert_to_decimal(data.get("refunded_amount")),
                "auto_apply": format_boolean(data.get("auto_apply")),
            }
        )

        return data


class StatementTransformer(RecordTransformer):
    def pre_transform(
        self, data: Union[Dict[str, Any], List[Dict[str, Any]]], context: DataContext
    ):
        super().pre_transform(data, context)

        data["template_id"] = data.get("template_id")

        return data


class SubscriptionTransformer(RecordTransformer):
    def pre_transform(
        self, data: Union[Dict[str, Any], List[Dict[str, Any]]], context: DataContext
    ):
        subscription_plans = data.get("plans", [])

        for subscription_plan in subscription_plans:
            super().pre_transform(subscription_plan, context)

            subscription_plan.update(
                {
                    "customer_id": data.get("customer_id"),
                    "bill_contact_id": data.get("bill_contact_id"),
                    "shipping_contact_id": data.get("shipping_contact_id"),
                    "status": data.get("status"),
                    "billing_start_date": format_date_string(
                        data.get("billing_start_date")
                    ),
                    "service_start_date": format_date_string(
                        data.get("service_start_date")
                    ),
                    "order_placed_at": format_date_string(data.get("order_placed_at")),
                    "contract_effective_date": format_date_string(
                        data.get("contract_effective_date")
                    ),
                    "cancellation_date": format_date_string(
                        data.get("cancellation_date")
                    ),
                    "auto_renew": data.get("auto_renew"),
                    "currency": data.get("currency"),
                    "payment_terms": data.get("payment_terms"),
                    "cmrr": convert_to_decimal(data.get("cmrr")),
                    "discounted_cmrr": convert_to_decimal(data.get("discounted_cmrr")),
                    "separate_invoice": data.get("separate_invoice"),
                    "notes": data.get("notes"),
                    "version": data.get("version"),
                    "version_type": data.get("version_type"),
                    "contract_term": data.get("contract_term"),
                    "renewal_term": data.get("renewal_term"),
                    "tcv": convert_to_decimal(data.get("tcv")),
                    "list_price": convert_to_decimal(
                        subscription_plan.get("list_price")
                    ),
                    "quantity": convert_to_decimal(subscription_plan.get("quantity")),
                    "included_units": convert_to_decimal(
                        subscription_plan.get("included_units")
                    ),
                    "discount": convert_to_decimal(subscription_plan.get("discount")),
                    "effective_price": convert_to_decimal(
                        subscription_plan.get("effective_price")
                    ),
                    "prepayment_periods": convert_to_decimal(
                        subscription_plan.get("prepayment_periods")
                    ),
                    "renewal_increment_percent": convert_to_decimal(
                        subscription_plan.get("renewal_increment_percent")
                    ),
                    "charge_end_date": format_date_string(
                        subscription_plan.get("charge_end_date")
                    ),
                    "created_by": data.get("created_by"),
                    "updated_by": data.get("updated_by"),
                    "created_date": data.get("created_date"),
                    "updated_date": data.get("updated_date"),
                    "custom_fields": data.get("custom_fields"),
                    "charge_custom_fields": subscription_plan.get("custom_fields"),
                }
            )

            yield subscription_plan
