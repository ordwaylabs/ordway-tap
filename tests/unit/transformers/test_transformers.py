from unittest import TestCase
from unittest.mock import MagicMock, patch
from tap_ordway.transformers import (
    BillingScheduleTransformer,
    CustomerTransformer,
    InvoiceTransformer,
    OrderTransformer,
    SubscriptionTransformer,
)


class TransformerBaseTestCase(TestCase):
    def setUp(self):
        self.get_company_id_patcher = patch(
            "tap_ordway.transformers.base.get_company_id"
        )
        self.mock_get_company_id = self.get_company_id_patcher.start()

    def tearDown(self):
        self.get_company_id_patcher.stop()


class BillingScheduleTransformerTestCase(TransformerBaseTestCase):
    def test_pre_transform(self):
        transformer = BillingScheduleTransformer()

        results = transformer.pre_transform(
            {
                "key_metrics": {
                    "monthly_recurring_revenue": "100.5",
                    "annual_contract_revenue": "50.4",
                    "total_contract_revenue": "765.3",
                    "amount_invoiced": "5",
                }
            },
            MagicMock(tap_stream_id="billing_schedules"),
        )

        self.assertIn("company_id", results)
        self.assertEqual(results["monthly_recurring_revenue"], "100.5")
        self.assertEqual(results["annual_contract_revenue"], "50.4")
        self.assertEqual(results["total_contract_revenue"], "765.3")
        self.assertEqual(results["amount_invoiced"], "5")


class CustomerTransformerTestCsae(TransformerBaseTestCase):
    def test_pre_transform(self):
        transformer = CustomerTransformer()

        results = transformer.pre_transform(
            {
                "balance": "750.89",
                "cmrr": "1.4",
                "discounted_cmrr": "-4",
                "customer_type": "bar",
            },
            MagicMock(tap_stream_id="customers"),
        )

        self.assertIn("company_id", results)
        self.assertNotIn("customer_type", results)

        self.assertEqual(results["billing_batch"], "bar")
        self.assertEqual(results["balance"], "750.89")
        self.assertEqual(results["cmrr"], "1.4")
        self.assertEqual(results["discounted_cmrr"], "-4")


class InvoiceTransformerTestCase(TransformerBaseTestCase):
    def test_pre_transform(self):
        transformer = InvoiceTransformer()

        results = transformer.pre_transform(
            {
                "id": "INV-001",
                "customer_id": "CUST-001",
                "billing_contact": "CNT-001",
                "shipping_contact": "CNT-001",
                "subtotal": "-50.4",
                "line_items": [
                    {
                        "unit_price": "-50.4",
                        "list_price": "6.00",
                        "quantity": "1",
                        "discount": None,
                        "effective_price": "-50.4",
                        "line_tax": "5",
                        "applied_tiers": None,
                    }
                ],
            },
            MagicMock(tap_stream_id="invoices"),
        )

        for result in results:
            self.assertEqual(result["invoice_id"], "INV-001")
            self.assertEqual(result["customer_id"], "CUST-001")
            self.assertEqual(result["billing_contact"], "CNT-001")
            self.assertEqual(result["shipping_contact"], "CNT-001")
            self.assertEqual(result["subtotal"], "-50.4")

            # Line item values
            self.assertEqual(result["unit_price"], "-50.4")
            self.assertEqual(result["list_price"], "6.00")
            self.assertEqual(result["quantity"], "1")
            self.assertIsNone(result["discount"])
            self.assertEqual(result["effective_price"], "-50.4")
            self.assertEqual(result["line_tax"], "5")
            self.assertIsNone(result["applied_tiers"])


class OrderTransformerTestCase(TransformerBaseTestCase):
    def test_pre_transform(self):
        transformer = OrderTransformer()

        results = transformer.pre_transform(
            {
                "id": "ORD-001",
                "customer_id": "CUST-001",
                "invoice_id": "INV-001",
                "order_date": "2020-01-01",
                "status": "Active",
                "order_amount": "-50.4",
                "separate_invoice": False,
                "currency": "USD",
                "notes": "L",
                "created_by": "example@example.com",
                "updated_by": "example@example.com",
                "updated_date": "2020-01-01",
                "created_date": "2020-01-01",
                "line_items": [{"line_no": "1"}, {"line_no": "2"}],
            },
            MagicMock(tap_stream_id="orders"),
        )

        for result in results:
            self.assertEqual(result["order_id"], "ORD-001")
            self.assertEqual(result["customer_id"], "CUST-001")
            self.assertEqual(result["invoice_id"], "INV-001")
            self.assertEqual(result["order_date"], "2020-01-01")
            self.assertEqual(result["status"], "Active")
            self.assertEqual(result["order_amount"], "-50.4")
            self.assertFalse(result["separate_invoice"])
            self.assertEqual(result["currency"], "USD")
            self.assertEqual(result["notes"], "L")
            self.assertEqual(result["created_by"], "example@example.com")
            self.assertEqual(result["updated_by"], "example@example.com")
            self.assertEqual(result["updated_date"], "2020-01-01")
            self.assertEqual(result["created_date"], "2020-01-01")

            self.assertIn("order_line_no", result)


class SubscriptionTransformerTestCase(TransformerBaseTestCase):
    def test_pre_transform(self):
        transformer = SubscriptionTransformer()

        results = transformer.pre_transform(
            {
                "id": "SUB-001",
                "customer_id": "CUST-001",
                "bill_contact_id": "CONTACT-001",
                "shipping_contact_id": "CONTACT-001",
                "status": "Active",
                "currency": "USD",
                "cmrr": "-5",
                "discounted_cmrr": "-55",
                "created_by": "example@example.com",
                "updated_by": "example@example.com",
                "updated_date": "2020-01-01",
                "created_date": "2020-01-01",
                "plans": [
                    {"id": "PLN-001", "custom_fields": {"foo": "bar"}},
                    {"id": "PLN-002", "custom_fields": {"foo": "bar"}},
                ],
            },
            MagicMock(tap_stream_id="subscriptions"),
        )

        for result in results:
            self.assertEqual(result["subscription_id"], "SUB-001")
            self.assertEqual(result["customer_id"], "CUST-001")
            self.assertEqual(result["bill_contact_id"], "CONTACT-001")
            self.assertEqual(result["shipping_contact_id"], "CONTACT-001")
            self.assertEqual(result["status"], "Active")
            self.assertEqual(result["currency"], "USD")
            self.assertEqual(result["cmrr"], "-5")
            self.assertEqual(result["discounted_cmrr"], "-55")
            self.assertEqual(result["created_by"], "example@example.com")
            self.assertEqual(result["updated_by"], "example@example.com")
            self.assertEqual(result["updated_date"], "2020-01-01")
            self.assertEqual(result["created_date"], "2020-01-01")
            self.assertDictEqual(result["charge_custom_fields"], {"foo": "bar"})
