from unittest import TestCase
from unittest.mock import MagicMock, patch
from tap_ordway.property import (
    get_key_properties,
    get_replication_key,
    get_replication_method,
    get_stream_metadata,
)
from ..utils import load_schema


def test_get_key_properties():
    foo_stream = MagicMock()
    foo_stream.key_properties = ["template_id"]
    bar_stream = MagicMock()
    bar_stream.key_properties = []

    with patch.dict(
        "tap_ordway.property.AVAILABLE_STREAMS",
        {"foo": foo_stream, "bar": bar_stream},
        clear=True,
    ):
        assert get_key_properties("foo") == ["template_id"]
        assert get_key_properties("bar") is None


def test_get_replication_key():
    foo_stream = MagicMock()
    foo_stream.replication_key = "updated_date"
    bar_stream = MagicMock()
    bar_stream.replication_key = None

    with patch.dict(
        "tap_ordway.property.AVAILABLE_STREAMS",
        {"foo": foo_stream, "bar": bar_stream},
        clear=True,
    ):
        assert get_replication_key("foo") == "updated_date"
        assert get_replication_key("bar") is None


def test_get_replication_method():
    foo_stream = MagicMock()
    foo_stream.replication_method = "FULL_TABLE"
    bar_stream = MagicMock()
    bar_stream.replication_method = "INCREMENTAL"

    with patch.dict(
        "tap_ordway.property.AVAILABLE_STREAMS",
        {"foo": foo_stream, "bar": bar_stream},
        clear=True,
    ):
        assert get_replication_method("foo") == "FULL_TABLE"
        assert get_replication_method("bar") == "INCREMENTAL"


class _TestStream:
    valid_replication_keys = []
    key_properties = ["name"]
    replication_method = "FULL_TABLE"


class GetStreamMetadataTestCase(TestCase):
    @patch.dict(
        "tap_ordway.property.AVAILABLE_STREAMS",
        {"foo": _TestStream},
        clear=True,
    )
    def test_name_in_key_properties(self):
        mdata = get_stream_metadata("foo", load_schema("webhooks.json"))

        self.assertIn("table-key-properties", mdata[0]["metadata"])
        self.assertListEqual(mdata[0]["metadata"]["table-key-properties"], ["name"])
