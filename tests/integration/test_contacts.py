from unittest.mock import patch
from singer.messages import ActivateVersionMessage, RecordMessage, SchemaMessage
from base import BaseOrdwayTestCase
from test_customers import MASKED_RESPONSE_FIELDS, _remove_custom_fields
from testing_tools.mask import StrFormat


class ContactsTestCase(BaseOrdwayTestCase):
    START_DATE = "2020-10-16"
    SELECTED_STREAMS = ("customers", "contacts")

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
        self.assertStreamVersion("contacts", 123000)

    def test_message_count(self):
        self.assertStreamInOutput("contacts")
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=RecordMessage, tap_stream_id="contacts"
                    )
                )
            ),
            18,
            msg='"contacts" stream should emit 18 RECORDs',
        )
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=ActivateVersionMessage, tap_stream_id="contacts"
                    )
                )
            ),
            2,
        )
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=SchemaMessage, tap_stream_id="contacts"
                    )
                )
            ),
            1,
        )
        self.assertMessageCountEqual(21, "contacts")

    def test_records_included(self):
        self.assertMessagesIncludesAll(
            [
                RecordMessage(
                    "contacts",
                    record={
                        "customer_id": "e4dHaYUv4bAunAd",
                        "first_name": "p02y0lfu7m47pu6yqxcxzhn0suesb5",
                        "last_name": "grjexiyh7rjolq3h62",
                        "display_name": "xlrt27lloqchvw5965wfyc9lk",
                        "email": "ykxh2751@example.com",
                        "phone": None,
                        "mobile": None,
                        "job_title": None,
                        "department": None,
                        "notes": None,
                        "address1": None,
                        "address2": None,
                        "county": None,
                        "city": None,
                        "state": None,
                        "zip": None,
                        "country": None,
                        "created_by": "wnd0q76h@example.com",
                        "updated_by": "fe446pas@example.com",
                        "created_date": "2020-10-16T19:45:32.447000Z",
                        "updated_date": "2020-10-16T19:45:32.447000Z",
                        "custom_fields": {},
                        "company_id": "test_company",
                        "contact_id": "CT-88160",
                    },
                    version=123000,
                ),
                RecordMessage(
                    "contacts",
                    record={
                        "customer_id": "N7v1duJIv8rvrUr",
                        "first_name": "nl0o6ovri528wjrmlvrigsy4g81qihterlwh7gl",
                        "last_name": "fdim652c4f2zkcv7yebehn9wwq",
                        "display_name": "hmf39rknmu0i4xnaqjil1hks019i0teb3nn2",
                        "email": "cet5b3uc@example.com",
                        "phone": None,
                        "mobile": None,
                        "job_title": None,
                        "department": None,
                        "notes": None,
                        "address1": None,
                        "address2": None,
                        "city": None,
                        "county": None,
                        "state": None,
                        "zip": None,
                        "country": None,
                        "created_by": "7ad5l6p4@example.com",
                        "updated_by": "fk3etztr@example.com",
                        "created_date": "2020-10-16T19:31:19.549000Z",
                        "updated_date": "2020-10-16T19:31:19.549000Z",
                        "custom_fields": {},
                        "company_id": "test_company",
                        "contact_id": "CT-88158",
                    },
                    version=123000,
                ),
                RecordMessage(
                    "contacts",
                    record={
                        "customer_id": "A0EQe9nw7DclBnX",
                        "first_name": "5u15gjpn0f7uw1rowku95zv",
                        "last_name": "j4",
                        "display_name": "vwexegsm3n28f5boiiyz1k9mia7xuwgu60hizgc0sdgve0",
                        "email": "b62ptsjx@example.com",
                        "phone": None,
                        "mobile": None,
                        "job_title": None,
                        "department": None,
                        "notes": None,
                        "address1": None,
                        "address2": None,
                        "city": None,
                        "county": None,
                        "state": None,
                        "zip": None,
                        "country": "8faz6oomg86jpf9wjdo7mkqxdnvg9cjyaihspjwjur4h81",
                        "created_by": "19j178q5@example.com",
                        "updated_by": "uj8cmexk@example.com",
                        "created_date": "2020-10-16T22:55:37.406000Z",
                        "updated_date": "2020-10-27T23:45:57.487000Z",
                        "custom_fields": {},
                        "company_id": "test_company",
                        "contact_id": "CT-88178",
                    },
                    version=123000,
                ),
                RecordMessage(
                    "contacts",
                    record={
                        "customer_id": "LKhGmEpWAugaPjh",
                        "first_name": "autv8md9l57khvzyr21g0lubnt8kgwv7x3xvpxw70ga8h9",
                        "last_name": "lyqvguco0w8s1p6c2sheqpwd9i6fyc689ku",
                        "display_name": "75f7sk9qpt33y41yrwhll6z1oh7",
                        "email": "9yw5lqs0@example.com",
                        "phone": None,
                        "mobile": None,
                        "job_title": None,
                        "department": None,
                        "notes": None,
                        "address1": None,
                        "address2": None,
                        "city": None,
                        "county": None,
                        "state": None,
                        "zip": None,
                        "country": None,
                        "created_by": "6eassxlx@example.com",
                        "updated_by": "p57epekj@example.com",
                        "created_date": "2020-10-16T22:45:12.855000Z",
                        "updated_date": "2020-10-16T22:45:12.856000Z",
                        "custom_fields": {},
                        "company_id": "test_company",
                        "contact_id": "CT-88176",
                    },
                    version=123000,
                ),
                RecordMessage(
                    "contacts",
                    record={
                        "customer_id": "kOR9WGPnR1KSt5I",
                        "first_name": None,
                        "last_name": None,
                        "display_name": "jm",
                        "email": None,
                        "phone": None,
                        "mobile": None,
                        "job_title": None,
                        "department": None,
                        "notes": None,
                        "address1": None,
                        "address2": None,
                        "city": None,
                        "county": None,
                        "state": None,
                        "zip": None,
                        "country": "537htd4hooc",
                        "created_by": "nwienrmz@example.com",
                        "updated_by": "j6wwta66@example.com",
                        "created_date": "2020-10-16T19:55:35.770000Z",
                        "updated_date": "2020-10-16T19:55:35.770000Z",
                        "custom_fields": {},
                        "company_id": "test_company",
                        "contact_id": "CT-88161",
                    },
                    version=123000,
                ),
                RecordMessage(
                    "contacts",
                    record={
                        "customer_id": "kkwBsUdKdC1JBS2",
                        "first_name": "eob5tx31aiy7s",
                        "last_name": "ox3h22zhfg8zethi7yg1g2ifviw5e",
                        "display_name": "7orr6b6y5nftrajo6o0p86mmkrbie8bij1",
                        "email": "n9sjetv2@example.com",
                        "phone": None,
                        "mobile": None,
                        "job_title": None,
                        "department": None,
                        "notes": None,
                        "address1": "zh9edtwpwmtpdt1pb5vba0r5hdgxp0tsb6fuhh83z8i1gqps4r",
                        "address2": None,
                        "city": "JlU8tGhYGbcp02d",
                        "county": None,
                        "state": "lqr4w3ko47o4k3vmp8b84lbcradofba",
                        "zip": "6pm37ou0ebg05rccrji73mfkxc8ugnqo7lrr8sj04w9uvg",
                        "country": "wj6oraqg9ovb4v45lq7yqdyt6ctznvrflu8tjnyk",
                        "created_by": "r48zihtq@example.com",
                        "updated_by": "59g1ef1f@example.com",
                        "created_date": "2020-10-16T21:21:41.067000Z",
                        "updated_date": "2020-10-16T21:21:41.067000Z",
                        "custom_fields": {},
                        "company_id": "test_company",
                        "contact_id": "CT-88174",
                    },
                    version=123000,
                ),
            ],
            ignored_keys=["company_id"],
        )
