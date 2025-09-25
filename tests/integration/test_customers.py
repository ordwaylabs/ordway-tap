from unittest.mock import patch
from singer.messages import ActivateVersionMessage, RecordMessage, SchemaMessage
from base import DEFAULT_MASKED_RESPONSE_FIELDS, BaseOrdwayTestCase
from testing_tools.mask import Format, StrFormat
from testing_tools.utils import alter_nested_value

MASKED_RESPONSE_FIELDS = {
    "balance": Format.AUTO,
    "cmrr": Format.AUTO,
    "discounted_cmrr": Format.AUTO,
    "pay_now_url": StrFormat.RANDOM,
    "update_payment_method_url": StrFormat.RANDOM,
    "open_invoices_url": StrFormat.RANDOM,
    "customer_id": StrFormat.RANDOM,
    "id": StrFormat.RANDOM,
}

MASKED_RESPONSE_FIELDS.update(DEFAULT_MASKED_RESPONSE_FIELDS)


def _remove_custom_fields(response):
    for item in response:
        alter_nested_value(("custom_fields",), item, {})
        alter_nested_value(
            (
                "contacts",
                "custom_fields",
            ),
            item,
            {},
        )


class CustomersTestCase(BaseOrdwayTestCase):
    START_DATE = "2020-10-16"
    SELECTED_STREAMS = ("customers",)

    MASKED_RESPONSE_FIELDS = MASKED_RESPONSE_FIELDS
    MASKED_RESPONSE_FORMATS = [StrFormat.EMAIL]
    RESPONSE_MUTATORS = (_remove_custom_fields,)

    @classmethod
    def setUpClass(cls):
        with patch("tap_ordway.utils.time", return_value=123):
            super().setUpClass()

    def test_records_follow_schema(self):
        self.assertRecordMessagesFollowSchema()

    def test_stream_version(self):
        self.assertStreamVersion("customers", 123000)

    def test_message_count(self):
        self.assertStreamInOutput("customers")
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=RecordMessage, tap_stream_id="customers"
                    )
                )
            ),
            19,
            msg='"customers" stream should emit 19 RECORDs',
        )
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=ActivateVersionMessage, tap_stream_id="customers"
                    )
                )
            ),
            2,
        )
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=SchemaMessage, tap_stream_id="customers"
                    )
                )
            ),
            1,
        )
        self.assertMessageCountEqual(22, "customers")

    def test_records_included(self):
        self.assertMessagesIncludesAll(
            [
                RecordMessage(
                    "customers",
                    record={
                        "name": "i9wztyc",
                        "description": None,
                        "parent_customer": None,
                        "website": None,
                        "payment_terms": "Due on Receipt",
                        "billing_cycle_day": "16th Day Of The Month",
                        "billing_contact_id": "CT-88158",
                        "shipping_contact_id": "CT-88158",
                        "status": "Active",
                        "billing_batch": "business",
                        "tax_exempt": False,
                        "balance": 50713.0,
                        "auto_pay": True,
                        "currency": "USD",
                        "edit_auto_pay": True,
                        "price_book_id": None,
                        "cmrr": 22286.0,
                        "discounted_cmrr": 20131.0,
                        "delivery_preferences": {"print": True, "email": True},
                        "created_by": "e5ybe1ko@example.com",
                        "updated_by": "4w4qt377@example.com",
                        "created_date": "2020-10-16T19:31:19.502000Z",
                        "updated_date": "2020-10-16T19:32:54.563000Z",
                        "pay_now_url": "at6q5masw265um515ispd5uskustcos2lx0",
                        "update_payment_method_url": "xwqlbzehivqjo30p",
                        "open_invoices_url": "4p5tjvzqhp",
                        "custom_fields": {},
                        "company_id": "test_company",
                        "customer_id": "N7v1duJIv8rvrUr",
                    },
                    version=123000,
                ),
                RecordMessage(
                    "customers",
                    record={
                        "name": "jgyh40yp9tk6ybxqtx6zaqcsmgx",
                        "description": None,
                        "parent_customer": None,
                        "website": None,
                        "payment_terms": "Due on Receipt",
                        "billing_cycle_day": "16th Day Of The Month",
                        "billing_contact_id": "CT-88174",
                        "shipping_contact_id": "CT-88174",
                        "status": "Active",
                        "billing_batch": "business",
                        "tax_exempt": False,
                        "balance": 21026.0,
                        "auto_pay": True,
                        "currency": "USD",
                        "edit_auto_pay": True,
                        "price_book_id": None,
                        "cmrr": 2623.0,
                        "discounted_cmrr": 39803.0,
                        "delivery_preferences": {"print": True, "email": True},
                        "created_by": "rwn7ekl6@example.com",
                        "updated_by": "u82ahdc4@example.com",
                        "created_date": "2020-10-16T21:21:41.016000Z",
                        "updated_date": "2020-10-16T23:02:54.851000Z",
                        "pay_now_url": "ha5jvh8qagng5oxv",
                        "update_payment_method_url": "t0dltueq7b9da3uab4gxyff",
                        "open_invoices_url": "mdebzyainmov44",
                        "custom_fields": {},
                        "company_id": "test_company",
                        "customer_id": "kkwBsUdKdC1JBS2",
                    },
                    version=123000,
                ),
                RecordMessage(
                    "customers",
                    record={
                        "name": "d4px1rsg3jdqtghwkcz7vz5hap2nf8d79332ob2x",
                        "description": None,
                        "parent_customer": None,
                        "website": None,
                        "payment_terms": "Due on Receipt",
                        "billing_cycle_day": "15th Day Of The Month",
                        "billing_contact_id": "CT-88162",
                        "shipping_contact_id": "CT-88162",
                        "status": "Active",
                        "billing_batch": "business",
                        "tax_exempt": False,
                        "balance": 1772.0,
                        "auto_pay": True,
                        "currency": "USD",
                        "edit_auto_pay": True,
                        "price_book_id": None,
                        "cmrr": 78433.0,
                        "discounted_cmrr": 8894.0,
                        "delivery_preferences": {"print": True, "email": True},
                        "created_by": "hpv5jj1b@example.com",
                        "updated_by": "6qcu87p0@example.com",
                        "created_date": "2020-10-16T20:00:02.722000Z",
                        "updated_date": "2020-10-16T20:02:25.228000Z",
                        "pay_now_url": "xuj6l44hzqoymcv5p8ld0fbads2ovlgiq8oqrv6ua",
                        "update_payment_method_url": "gdqj1hbma0gfvptn982isax7rk68vf09c7",
                        "open_invoices_url": "etj7pqvz4p5w0yp288auf9buitc",
                        "custom_fields": {},
                        "company_id": "test_company",
                        "customer_id": "yFCMqkp73scyiQcv",
                    },
                    version=123000,
                ),
                RecordMessage(
                    "customers",
                    record={
                        "name": "belczvbzhbtc1seqrrftkenvd",
                        "description": None,
                        "parent_customer": None,
                        "website": None,
                        "payment_terms": "Due on Receipt",
                        "billing_cycle_day": "16th Day Of The Month",
                        "billing_contact_id": "CT-88159",
                        "shipping_contact_id": "CT-88159",
                        "status": "Active",
                        "billing_batch": "business",
                        "tax_exempt": False,
                        "balance": 74522.0,
                        "auto_pay": True,
                        "currency": "USD",
                        "edit_auto_pay": True,
                        "price_book_id": None,
                        "cmrr": 93601.0,
                        "discounted_cmrr": 2060.0,
                        "delivery_preferences": {"print": True, "email": True},
                        "created_by": "5bqy8zrr@example.com",
                        "updated_by": "b1jr05ms@example.com",
                        "created_date": "2020-10-16T19:41:18.673000Z",
                        "updated_date": "2020-10-16T19:43:38.073000Z",
                        "pay_now_url": "ht4a337rcece6rlik9f5cy",
                        "update_payment_method_url": "2vu23my8",
                        "open_invoices_url": "qpd3pu7tbor",
                        "custom_fields": {},
                        "company_id": "test_company",
                        "customer_id": "cozNeo20GBHCWLu",
                    },
                    version=123000,
                ),
                RecordMessage(
                    "customers",
                    record={
                        "name": "zbsc2gvvxakz6nibsimyax43",
                        "description": None,
                        "parent_customer": None,
                        "website": None,
                        "payment_terms": "Due on Receipt",
                        "billing_cycle_day": "16th Day Of The Month",
                        "billing_contact_id": "CT-88167",
                        "shipping_contact_id": "CT-88167",
                        "status": "Active",
                        "billing_batch": "business",
                        "tax_exempt": False,
                        "balance": 1970.0,
                        "auto_pay": True,
                        "currency": "USD",
                        "edit_auto_pay": True,
                        "price_book_id": None,
                        "cmrr": 92360.0,
                        "discounted_cmrr": 64188.0,
                        "delivery_preferences": {"print": True, "email": True},
                        "created_by": "letfxogi@example.com",
                        "updated_by": "pz1d6zxx@example.com",
                        "created_date": "2020-10-16T20:19:22.440000Z",
                        "updated_date": "2020-10-20T17:01:22.101000Z",
                        "pay_now_url": "f7yka8ysgcfgecgb7ohee58vjr",
                        "update_payment_method_url": "8vitcekglzid7xi99y8b8tbqnui03jay639pk851fc531d",
                        "open_invoices_url": "026xtvzuahmr76v7xqpmqip3ezggamwrb16ptjscbxevvcosnx",
                        "custom_fields": {},
                        "company_id": "test_company",
                        "customer_id": "spg65QiZr828hvP",
                    },
                    version=123000,
                ),
            ],
            ignored_keys=["company_id"],
        )

    def test_unselected_substreams_not_in_output(self):
        self.assertStreamNotInOutput("contacts")
        self.assertStreamNotInOutput("payments")
        self.assertStreamNotInOutput("customer_notes")
