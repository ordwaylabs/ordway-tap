from unittest.mock import patch
from singer.messages import ActivateVersionMessage, RecordMessage, SchemaMessage
from .base import DEFAULT_MASKED_RESPONSE_FIELDS, BaseOrdwayTestCase
from .testing_tools.mask import Format, StrFormat

MASKED_RESPONSE_FIELDS = {
    "quantity": Format.AUTO,
    "list_price": StrFormat.FLOAT,
    "list_price_base": StrFormat.FLOAT,
    "public_url": StrFormat.RANDOM,
}

MASKED_RESPONSE_FIELDS.update(DEFAULT_MASKED_RESPONSE_FIELDS)


class PlansTestCase(BaseOrdwayTestCase):
    START_DATE = "2020-08-01"
    SELECTED_STREAMS = ("plans",)

    MASKED_RESPONSE_FIELDS = MASKED_RESPONSE_FIELDS
    MASKED_RESPONSE_FORMATS = [StrFormat.EMAIL]

    @classmethod
    def setUpClass(cls):
        with patch("tap_ordway.utils.time", return_value=123):
            super().setUpClass()

    def test_records_follow_schema(self):
        self.assertRecordMessagesFollowSchema()

    def test_stream_version(self):
        self.assertStreamVersion("plans", 123000)

    def test_message_count(self):
        self.assertStreamInOutput("plans")
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=RecordMessage, tap_stream_id="plans"
                    )
                )
            ),
            5,
            msg='"plans" stream should emit 5 RECORDs',
        )
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=ActivateVersionMessage, tap_stream_id="plans"
                    )
                )
            ),
            2,
        )
        self.assertEqual(
            len(
                list(
                    self.tap_executor.output.filter_messages(
                        message_type=SchemaMessage, tap_stream_id="plans"
                    )
                )
            ),
            1,
        )
        self.assertMessageCountEqual(8, "plans")

    def test_all_records_included(self):
        self.assertMessagesIncludesAll(
            [
                RecordMessage(
                    "plans",
                    record={
                        "name": "fhynhq1m8siyza0ij360kph7nk9",
                        "status": "Active",
                        "description": None,
                        "created_by": "k7hhqmld@example.com",
                        "updated_by": "8e4tz48k@example.com",
                        "created_date": "2020-08-20T21:52:27.245000Z",
                        "updated_date": "2020-08-20T21:52:27.245000Z",
                        "public_url": None,
                        "custom_fields": {"Type": ""},
                        "company_id": "test_company",
                        "plan_id": "PLN-00121",
                    },
                    version=123000,
                ),
                RecordMessage(
                    "plans",
                    record={
                        "name": "x7dh315h4o",
                        "status": "Active",
                        "description": None,
                        "created_by": "zhq788si@example.com",
                        "updated_by": "144gd9p8@example.com",
                        "created_date": "2020-08-20T21:51:57.921000Z",
                        "updated_date": "2020-08-20T21:51:57.921000Z",
                        "public_url": None,
                        "custom_fields": {"Type": ""},
                        "company_id": "test_company",
                        "plan_id": "PLN-00120",
                    },
                    version=123000,
                ),
                RecordMessage(
                    "plans",
                    record={
                        "name": "jsqcu0qygpx4z8k6uqnk54obb1t3lk7u7",
                        "status": "Active",
                        "description": None,
                        "created_by": "53z6qbwi@example.com",
                        "updated_by": "5i98uacm@example.com",
                        "created_date": "2020-08-06T18:05:16.454000Z",
                        "updated_date": "2020-08-06T18:05:16.454000Z",
                        "public_url": None,
                        "custom_fields": {"Type": ""},
                        "company_id": "test_company",
                        "plan_id": "PLN-00119",
                    },
                    version=123000,
                ),
                RecordMessage(
                    "plans",
                    record={
                        "name": "kioycx9av4c99zddrxq3pkuxhkfkdexo4f275k4ubl51g5nh",
                        "status": "Active",
                        "description": None,
                        "created_by": "j7thkub3@example.com",
                        "updated_by": "yredynx6@example.com",
                        "created_date": "2020-03-20T19:29:49.142000Z",
                        "updated_date": "2020-08-24T17:26:32.059000Z",
                        "public_url": None,
                        "custom_fields": {"Type": ""},
                        "company_id": "test_company",
                        "plan_id": "PLN-00027",
                    },
                    version=123000,
                ),
                RecordMessage(
                    "plans",
                    record={
                        "name": "3ocsblb1by38d03hrb58trojs7uedm6p",
                        "status": "Active",
                        "description": None,
                        "created_by": "5571ibxn@example.com",
                        "updated_by": "jwfjkbta@example.com",
                        "created_date": "2020-03-20T19:27:02.679000Z",
                        "updated_date": "2020-09-17T00:15:57.107000Z",
                        "public_url": None,
                        "custom_fields": {"Type": ""},
                        "company_id": "test_company",
                        "plan_id": "PLN-00025",
                    },
                    version=123000,
                ),
            ],
            ignored_keys=["company_id"],
        )
