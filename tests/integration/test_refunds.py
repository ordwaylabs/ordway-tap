from singer.messages import ActivateVersionMessage, RecordMessage, SchemaMessage
from .base import DEFAULT_MASKED_RESPONSE_FIELDS, BaseOrdwayTestCase
from .testing_tools.mask import Format, StrFormat

MASKED_RESPONSE_FIELDS = {
    "refund_amount": StrFormat.FLOAT,
    "reference_number": StrFormat.RANDOM,
    "conversion_rate": Format.AUTO,
}

MASKED_RESPONSE_FIELDS.update(DEFAULT_MASKED_RESPONSE_FIELDS)


class RefundsTestCase(BaseOrdwayTestCase):
    START_DATE = "2020-11-01"
    SELECTED_STREAMS = ("refunds",)

    MASKED_RESPONSE_FIELDS = MASKED_RESPONSE_FIELDS
    MASKED_RESPONSE_FORMATS = [StrFormat.EMAIL]

    def test_stream_version(self):
        self.assertStreamVersion("refunds", None)

    def test_message_count(self):
        self.assertStreamInOutput("refunds")
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=RecordMessage, tap_stream_id="refunds"
                    )
                )
            ),
            13,
            msg='"refunds" stream should emit 13 RECORDs',
        )
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=ActivateVersionMessage, tap_stream_id="refunds"
                    )
                )
            ),
            0,
        )
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=SchemaMessage, tap_stream_id="refunds"
                    )
                )
            ),
            1,
        )
        self.assertMessageCountEqual(14, "refunds")

    def test_records_included(self):
        self.assertMessagesIncludesAll(
            [
                RecordMessage(
                    "refunds",
                    record={
                        "customer_id": "rLk3NloE8Cn1Pvo",
                        "refund_date": "2020-11-02",
                        "refund_amount": 33198.0,
                        "refund_type": "Electronic",
                        "payment_id": "PMT-05318",
                        "notes": "m661adkvdc0d7u3em2ky",
                        "currency": "USD",
                        "reference_number": "x7b6zlj21702xdxc0jyy30i85wd5l",
                        "created_by": "50qkpn6g@example.com",
                        "updated_by": "01rrvf6z@example.com",
                        "created_date": "2020-11-02T18:02:37.250000Z",
                        "updated_date": "2020-11-02T18:02:38.317000Z",
                        "company_id": "test_company",
                        "refund_id": "REF-00120",
                    },
                    version=None,
                ),
                RecordMessage(
                    "refunds",
                    record={
                        "customer_id": "XFhLE2GY6eNcSQl",
                        "refund_date": "2020-11-02",
                        "refund_amount": 96069.0,
                        "refund_type": "Electronic",
                        "payment_id": "PMT-05353",
                        "notes": "6727rxtwkksv80g92x6sh34",
                        "currency": "USD",
                        "reference_number": "f87vqnewvdcjpqp5jlspvvo77d6hsp9qsdfzgoe1213vwx4g4",
                        "created_by": "zxdifhe4@example.com",
                        "updated_by": "et051qsv@example.com",
                        "created_date": "2020-11-02T18:44:46.172000Z",
                        "updated_date": "2020-11-02T18:44:47.532000Z",
                        "company_id": "test_company",
                        "refund_id": "REF-00121",
                    },
                    version=None,
                ),
                RecordMessage(
                    "refunds",
                    record={
                        "customer_id": "NBMaZz9VZejISz0",
                        "refund_date": "2020-11-02",
                        "refund_amount": 22353.0,
                        "refund_type": "Electronic",
                        "payment_id": "PMT-05305",
                        "notes": "pp7zbsk4wottfz7745sly00b9pa",
                        "currency": "USD",
                        "reference_number": "xhigeq7graa5l2qdhnij3t",
                        "created_by": "kpgzw1t1@example.com",
                        "updated_by": "43q8ld85@example.com",
                        "created_date": "2020-11-03T07:40:47.001000Z",
                        "updated_date": "2020-11-03T07:40:48.222000Z",
                        "company_id": "test_company",
                        "refund_id": "REF-00122",
                    },
                    version=None,
                ),
                RecordMessage(
                    "refunds",
                    record={
                        "customer_id": "Gj2bYtxgmlJKDTW",
                        "refund_date": "2020-11-11",
                        "refund_amount": 77829.0,
                        "refund_type": "Electronic",
                        "payment_id": "PMT-03859",
                        "notes": "capwlc8yyni4nnblm",
                        "currency": "USD",
                        "reference_number": "vx4p7nz9v4tivnobcb4j9p1g5iykottqn2o8ztia08rn5zzjk",
                        "created_by": "t5knct62@example.com",
                        "updated_by": "7qyph4wo@example.com",
                        "created_date": "2020-11-11T18:35:50.911000Z",
                        "updated_date": "2020-11-11T18:35:52.362000Z",
                        "company_id": "test_company",
                        "refund_id": "REF-00131",
                    },
                    version=None,
                ),
                RecordMessage(
                    "refunds",
                    record={
                        "customer_id": "kOR9WGPnR1KSt5I",
                        "refund_date": "2020-11-11",
                        "refund_amount": 71864.0,
                        "refund_type": "Electronic",
                        "payment_id": "PMT-05514",
                        "notes": "afjhm202xrhzdk3sj3ndyuwuyaqb6qpwkxhgr6",
                        "currency": "USD",
                        "reference_number": "em39h71s4iz9jkchexvghs62lhef9ot0x4odcl",
                        "created_by": "6wldqeom@example.com",
                        "updated_by": "xizaqorr@example.com",
                        "created_date": "2020-11-11T17:51:07.300000Z",
                        "updated_date": "2020-11-11T17:51:08.589000Z",
                        "company_id": "test_company",
                        "refund_id": "REF-00130",
                    },
                    version=None,
                ),
            ],
            ignored_keys=["company_id"],
        )


class RefundsSecondRunTestCase(BaseOrdwayTestCase):
    START_DATE = "2020-11-10"
    SELECTED_STREAMS = ("refunds",)
    STATE = {
        "currently_syncing": "refunds",
        "bookmarks": {"refunds": {"updated_date": "2020-11-11T22:24:58.660000Z"}},
    }

    MASKED_RESPONSE_FIELDS = MASKED_RESPONSE_FIELDS
    MASKED_RESPONSE_FORMATS = [StrFormat.EMAIL]

    def test_records_follow_schema(self):
        self.assertRecordMessagesFollowSchema()

    def test_stream_version(self):
        self.assertStreamVersion("refunds", None)

    def test_message_count(self):
        self.assertStreamInOutput("refunds")
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=RecordMessage, tap_stream_id="refunds"
                    )
                )
            ),
            0,
            msg='"refunds" stream should emit 0 RECORD',
        )
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=ActivateVersionMessage, tap_stream_id="refunds"
                    )
                )
            ),
            0,
        )
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=SchemaMessage, tap_stream_id="refunds"
                    )
                )
            ),
            1,
        )
        self.assertMessageCountEqual(1, "refunds")
