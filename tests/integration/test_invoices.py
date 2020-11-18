from singer.messages import RecordMessage, ActivateVersionMessage, SchemaMessage
from .testing_tools.mask import StrFormat, Format
from .testing_tools.utils import alter_nested_value
from .base import BaseOrdwayTestCase, DEFAULT_MASKED_RESPONSE_FIELDS

MASKED_RESPONSE_FIELDS = {
    "quantity": Format.AUTO,
    "list_price": StrFormat.FLOAT,
    "effective_price": StrFormat.FLOAT,
    "line_tax": StrFormat.FLOAT,
    "invoice_pdf_url": StrFormat.RANDOM,
    "payment_amount": StrFormat.FLOAT,
    "fee_amount": StrFormat.FLOAT,
    "applied_amount": StrFormat.FLOAT,
    "unapplied_amount": StrFormat.FLOAT,
    "refunded_amount": StrFormat.FLOAT,
    "payment_method": StrFormat.RANDOM,
    "reference_number": StrFormat.RANDOM,
    "customer_name": StrFormat.RANDOM,
    "product_name": StrFormat.RANDOM,
    "plan_name": StrFormat.RANDOM,
    "charge_name": StrFormat.RANDOM,
    "invoice_amount": StrFormat.FLOAT,
    "paid_amount": StrFormat.FLOAT,
    "balance": StrFormat.FLOAT,
    "unit_price": StrFormat.FLOAT,
    "subtotal": StrFormat.FLOAT,
    "invoice_tax": StrFormat.FLOAT,
    "discount": StrFormat.FLOAT,
    # PMT
    "failed_attempt": Format.AUTO,
    "retried_attempts": Format.AUTO,
}

MASKED_RESPONSE_FIELDS.update(DEFAULT_MASKED_RESPONSE_FIELDS)


def _remove_custom_fields(response):
    for item in response:
        alter_nested_value(("custom_fields",), item, {})
        alter_nested_value(("line_items", "custom_fields"), item, {})
        alter_nested_value(("payments", "custom_fields"), item, {})
        alter_nested_value(("credits", "custom_fields"), item, {})


class InvoicesTestCase(BaseOrdwayTestCase):
    START_DATE = "2020-11-10"
    SELECTED_STREAMS = ("invoices",)

    MASKED_RESPONSE_FIELDS = MASKED_RESPONSE_FIELDS
    MASKED_RESPONSE_FORMATS = [StrFormat.EMAIL]
    RESPONSE_MUTATORS = (_remove_custom_fields,)

    def test_stream_version(self):
        self.assertStreamVersion("invoices", 1)

    def test_records_follow_schema(self):
        self.assertRecordMessagesFollowSchema()

    def test_message_count(self):
        self.assertStreamInOutput("invoices")
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=RecordMessage, tap_stream_id="invoices"
                    )
                )
            ),
            56,
            msg='"invoices" stream should emit 52 RECORDs',
        )
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=ActivateVersionMessage, tap_stream_id="invoices"
                    )
                )
            ),
            1,
        )
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=SchemaMessage, tap_stream_id="invoices"
                    )
                )
            ),
            1,
        )
        self.assertMessageCountEqual(58, "invoices")

    def test_records_included(self):
        self.assertMessagesIncludesAll(
            [
                RecordMessage(
                    "invoices",
                    record={
                        "subscription_id": "S-01245",
                        "customer_id": "Tf4lCBZLFQwJJHM",
                        "customer_name": "c6n02hig9ttf",
                        "product_id": "P-00005",
                        "product_name": "hgaac9t09crpais2k8",
                        "plan_id": "PLN-00013",
                        "plan_name": "1z40xx8tjasz8wzu1k4boxa8amedj0ivrk",
                        "charge_id": "CHG-00013",
                        "subscription_line_id": "SCHG-01",
                        "charge_name": "n2agax0y1u39t3r25acn0euzfultx",
                        "charge_type": "recurring",
                        "description": "b85rjlkgpypzupw14vsxb9do515deo",
                        "start_date": "2020-10-20",
                        "end_date": "2020-11-19",
                        "unit_price": 1521.0,
                        "list_price": 31562.0,
                        "list_price_base": "Billing Period",
                        "taxable": False,
                        "quantity": 11116.0,
                        "discount": 0.0,
                        "effective_price": 97861.0,
                        "line_tax": 21599.0,
                        "tax_lines": [],
                        "applied_tiers": None,
                        "custom_fields": {},
                        "company_id": "test_company",
                        "invoice_id": "INV-06011",
                        "invoice_line_no": "1",
                        "billing_contact": {
                            "zip": "mohcr4c15u7zn30tji",
                            "city": "6czu4",
                            "email": "qvbjgdte@example.com",
                            "phone": "",
                            "state": "46tnbhnz8tmhgklvv2hhkyzikd48nt9qsxsjy18yrdcsw0",
                            "county": "",
                            "mobile": "",
                            "country": "yoluvps6febu2cuhrbsy3do2ksdwp9ya5l",
                            "address1": "xyq68200cstjd2kwgf3rjiads8bglnvtjqglr",
                            "address2": "",
                            "job_title": "",
                            "last_name": "4c4cmwdb6c3udqf786frq",
                            "department": "",
                            "first_name": "sndp3b0ybap6bcp7aosamw3bxp",
                            "display_name": "hjuvr8zntv6e2qjbznkckgvtnb0k5f1jp9qkax3",
                        },
                        "shipping_contact": {
                            "zip": "y0qbdsu94rjlueqq72",
                            "city": "96ljgtobqwz3th69dlcmzhd6gakpbu4",
                            "email": "hzyu1aj7@example.com",
                            "phone": "",
                            "state": "kqdhyo52fulkr5friktj0958k7wqt93usfcf",
                            "county": "",
                            "mobile": "",
                            "country": "g0xc6ft3zfkwfxq0lea3opw5lp4eps3di00se57gm4",
                            "address1": "p7g6bmd7dwp2qefkyteuctitila5qdxg04t34w6ldxs",
                            "address2": "",
                            "job_title": "",
                            "last_name": "mzjkz5tihgqrez8fh935",
                            "department": "",
                            "first_name": "gx85kbtndretb5zd6t3qo3rww",
                            "display_name": "umrp56mbkenwopve9fu5c4vnymgviw7vbv4gdt",
                        },
                        "invoice_date": "2020-11-06",
                        "due_date": "2020-11-06",
                        "billing_run_id": "BR-02340",
                        "subtotal": 22160.0,
                        "invoice_tax": 70856.0,
                        "invoice_amount": 44735.0,
                        "paid_amount": 46311.0,
                        "balance": 19552.0,
                        "status": "Partially paid",
                        "notes": None,
                        "currency": "USD",
                        "payment_terms": "0",
                        "line_custom_fields": {},
                        "updated_date": "2020-11-10T21:27:26.955000Z",
                        "created_date": "2020-11-06T08:01:30.410000Z",
                        "created_by": "heeyeg5i@example.com",
                        "updated_by": "s1rkw1w4@example.com",
                    },
                    version=1,
                ),
                RecordMessage(
                    "invoices",
                    record={
                        "subscription_id": "S-01748",
                        "customer_id": "uiPfZCmRf6YcmM3",
                        "customer_name": "ccfr5daarxvyg3kuo776gcl1wfvxdfdl9gpgcu6hx",
                        "product_id": "P-00122",
                        "product_name": "uh40oj4etcnmbhcb99q9esnsy7r3khwv",
                        "plan_id": "PLN-00022",
                        "plan_name": "0bl72pu8mca0vez6be3o",
                        "charge_id": "CHG-00022",
                        "subscription_line_id": "SCHG-02",
                        "charge_name": "82eif3fzu4vvsj7ehowubc360jb88xpgpnuf9h49kuay",
                        "charge_type": "recurring",
                        "description": "6oz2e05gx3dizne28b20x9h5sv3lafzs9v0jxpru1",
                        "start_date": "2020-10-18",
                        "end_date": "2021-01-17",
                        "unit_price": 37774.0,
                        "list_price": 93161.0,
                        "list_price_base": "Billing Period",
                        "taxable": False,
                        "quantity": 79808.0,
                        "discount": 0.0,
                        "effective_price": 77944.0,
                        "line_tax": 74460.0,
                        "tax_lines": [],
                        "applied_tiers": None,
                        "custom_fields": {},
                        "company_id": "test_company",
                        "invoice_id": "INV-04720",
                        "invoice_line_no": "1",
                        "billing_contact": {
                            "zip": "",
                            "city": "",
                            "email": "8ssykjcp@example.com",
                            "phone": "",
                            "state": "",
                            "county": "",
                            "mobile": "",
                            "country": "",
                            "address1": "",
                            "address2": "",
                            "job_title": "",
                            "last_name": "pfegs42j8xhpa4117h4r8qxsjjic4k3ul17somsltgr",
                            "department": "",
                            "first_name": "ch2zjx66v6dknyv9t05k",
                            "display_name": "ctlysi39w3p6ftohbsfftxg07ioij",
                        },
                        "shipping_contact": {
                            "zip": "",
                            "city": "",
                            "email": "9i4tcjcj@example.com",
                            "phone": "",
                            "state": "",
                            "county": "",
                            "mobile": "",
                            "country": "",
                            "address1": "",
                            "address2": "",
                            "job_title": "",
                            "last_name": "zcwgenue175in2kuoftq9beiqn6m1phxlmhe27qycibg9",
                            "department": "",
                            "first_name": "v97c6gbz3oq35v95gopca99eqb4r83dus8nd4cq3ez5jtwfzk",
                            "display_name": "adjlxfp1uf1dwbo0g2wyrbj355znt9l91zrrydxvgcrsqzb3mr",
                        },
                        "invoice_date": "2020-10-18",
                        "due_date": "2020-10-18",
                        "billing_run_id": "BR-02010",
                        "subtotal": 26170.0,
                        "invoice_tax": 31694.0,
                        "invoice_amount": 74369.0,
                        "paid_amount": 83388.0,
                        "balance": 61611.0,
                        "status": "Paid",
                        "notes": None,
                        "currency": "USD",
                        "payment_terms": "0",
                        "line_custom_fields": {},
                        "updated_date": "2020-11-10T21:46:36.606000Z",
                        "created_date": "2020-10-18T08:06:03.598000Z",
                        "created_by": "73tr1lsh@example.com",
                        "updated_by": "022pl4kr@example.com",
                    },
                    version=1,
                ),
                RecordMessage(
                    "invoices",
                    record={
                        "subscription_id": "S-01748",
                        "customer_id": "uiPfZCmRf6YcmM3",
                        "customer_name": "ccfr5daarxvyg3kuo776gcl1wfvxdfdl9gpgcu6hx",
                        "product_id": "P-00005",
                        "product_name": "tzgzblnfsna0ic8",
                        "plan_id": "PLN-00013",
                        "plan_name": "exg6mf67o5vcw37hga299rl0ticxe0zxxnvq",
                        "charge_id": "CHG-00013",
                        "subscription_line_id": "SCHG-01",
                        "charge_name": "wmgqwbqlma52fzgzzasl4ympoxcwjbiw6j6k0",
                        "charge_type": "recurring",
                        "description": "xv4higzl",
                        "start_date": "2020-10-18",
                        "end_date": "2021-01-17",
                        "unit_price": 19906.0,
                        "list_price": 5535.0,
                        "list_price_base": "Billing Period",
                        "taxable": False,
                        "quantity": 3264.0,
                        "discount": 0.0,
                        "effective_price": 33077.0,
                        "line_tax": 80632.0,
                        "tax_lines": [],
                        "applied_tiers": None,
                        "custom_fields": {},
                        "company_id": "test_company",
                        "invoice_id": "INV-04720",
                        "invoice_line_no": "2",
                        "billing_contact": {
                            "zip": "",
                            "city": "",
                            "email": "8ssykjcp@example.com",
                            "phone": "",
                            "state": "",
                            "county": "",
                            "mobile": "",
                            "country": "",
                            "address1": "",
                            "address2": "",
                            "job_title": "",
                            "last_name": "pfegs42j8xhpa4117h4r8qxsjjic4k3ul17somsltgr",
                            "department": "",
                            "first_name": "ch2zjx66v6dknyv9t05k",
                            "display_name": "ctlysi39w3p6ftohbsfftxg07ioij",
                        },
                        "shipping_contact": {
                            "zip": "",
                            "city": "",
                            "email": "9i4tcjcj@example.com",
                            "phone": "",
                            "state": "",
                            "county": "",
                            "mobile": "",
                            "country": "",
                            "address1": "",
                            "address2": "",
                            "job_title": "",
                            "last_name": "zcwgenue175in2kuoftq9beiqn6m1phxlmhe27qycibg9",
                            "department": "",
                            "first_name": "v97c6gbz3oq35v95gopca99eqb4r83dus8nd4cq3ez5jtwfzk",
                            "display_name": "adjlxfp1uf1dwbo0g2wyrbj355znt9l91zrrydxvgcrsqzb3mr",
                        },
                        "invoice_date": "2020-10-18",
                        "due_date": "2020-10-18",
                        "billing_run_id": "BR-02010",
                        "subtotal": 26170.0,
                        "invoice_tax": 31694.0,
                        "invoice_amount": 74369.0,
                        "paid_amount": 83388.0,
                        "balance": 61611.0,
                        "status": "Paid",
                        "notes": None,
                        "currency": "USD",
                        "payment_terms": "0",
                        "line_custom_fields": {},
                        "updated_date": "2020-11-10T21:46:36.606000Z",
                        "created_date": "2020-10-18T08:06:03.598000Z",
                        "created_by": "73tr1lsh@example.com",
                        "updated_by": "022pl4kr@example.com",
                    },
                    version=1,
                ),
                RecordMessage(
                    "invoices",
                    record={
                        "subscription_id": "S-01607",
                        "customer_id": "mvlm8gBHhTf1mB6",
                        "customer_name": "6ht3arffdok81b0mh1oc7721jjdang6i8pn4",
                        "product_id": "P-00005",
                        "product_name": "kz3kn03qcw7v0a2ceg",
                        "plan_id": "PLN-00013",
                        "plan_name": "10hjst75p1ls0ckhado0xpno2h101fqgulb1y8zal",
                        "charge_id": "CHG-00013",
                        "subscription_line_id": "SCHG-01",
                        "charge_name": "7xi4xztofgn9r6z90r1crq",
                        "charge_type": "recurring",
                        "description": "s6vryanbun9x2ynpbbd7vf5h9pqzsn",
                        "start_date": "2020-11-10",
                        "end_date": "2020-12-09",
                        "unit_price": 27153.0,
                        "list_price": 86154.0,
                        "list_price_base": "Billing Period",
                        "taxable": False,
                        "quantity": 11941.0,
                        "discount": 0.0,
                        "effective_price": 56298.0,
                        "line_tax": 55107.0,
                        "tax_lines": [],
                        "applied_tiers": None,
                        "custom_fields": {},
                        "company_id": "test_company",
                        "invoice_id": "INV-06184",
                        "invoice_line_no": "1",
                        "billing_contact": {
                            "zip": "b66g6uqmtjpyiwf5wt",
                            "city": "3mxzraht",
                            "email": "0b8wh7zg@example.com",
                            "phone": "",
                            "state": "xrtu82a8011ppk58xogx5ai7c94w8m3435q5siufwcn5d0sm",
                            "county": "",
                            "mobile": "",
                            "country": "jskchmcjyp",
                            "address1": "8rka",
                            "address2": "",
                            "job_title": "",
                            "last_name": "riljonfa7ckiqd18gh2mg5c7y4h3earoboah9e0nq8p9lcaqe",
                            "department": "",
                            "first_name": "oqhgoewao8me1h1135dq07",
                            "display_name": "kz7mxvjf1waogamblajvxr2f",
                        },
                        "shipping_contact": {
                            "zip": "0wuawdo",
                            "city": "3maxesensw64dnz70uqp8796y",
                            "email": "58d7stml@example.com",
                            "phone": "",
                            "state": "5t7hnk0dshallnm8ap59ey9q3154q5hxz0q",
                            "county": "",
                            "mobile": "",
                            "country": "0st69usdzeto74a2s8qpeh580p2kdz",
                            "address1": "lj",
                            "address2": "",
                            "job_title": "",
                            "last_name": "9fc252s3stkqx2qa36iiml23nw12fwa0i22vjiq661459wdyz",
                            "department": "",
                            "first_name": "ej",
                            "display_name": "yfas0zucjobtws2rq3rzdx1ecv1z1jo2",
                        },
                        "invoice_date": "2020-11-10",
                        "due_date": "2020-11-10",
                        "billing_run_id": "BR-02363",
                        "subtotal": 57006.0,
                        "invoice_tax": 88965.0,
                        "invoice_amount": 49136.0,
                        "paid_amount": 59809.0,
                        "balance": 14074.0,
                        "status": "Paid",
                        "notes": None,
                        "currency": "USD",
                        "payment_terms": "Due on Receipt",
                        "line_custom_fields": {},
                        "updated_date": "2020-11-10T08:17:59.594000Z",
                        "created_date": "2020-11-10T08:03:37.974000Z",
                        "created_by": "wx8626qy@example.com",
                        "updated_by": "lxkxssmk@example.com",
                    },
                    version=1,
                ),
            ],
            ignored_keys=["company_id"],
        )
