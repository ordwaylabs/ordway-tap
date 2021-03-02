from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch
from tests.utils import generate_catalog
from tap_ordway.streams.base import ResponseSubstream, Stream, Substream


class StreamTestCase(TestCase):
    def setUp(self):
        class TestSubstream(ResponseSubstream):
            tap_stream_id = "test_response_substream"
            key_properties = []
            path = ("",)
            transformer_class = MagicMock()

        class TestStream(Stream):
            tap_stream_id = "test_stream"
            substream_definitions = [TestSubstream]
            key_properties = []
            request_handler = MagicMock()
            valid_replication_keys = ["updated_date"]
            transformer_class = MagicMock()

        self.TestSubstream = TestSubstream
        self.TestStream = TestStream

        self.test_catalog = generate_catalog(
            [
                {"tap_stream_id": "test_stream", "selected": True},
                {"tap_stream_id": "test_response_substream", "selected": True},
            ]
        )
        self.test_stream = self.TestStream(self.test_catalog, {})

    def test_instantiate_substreams(self):
        self.test_stream.instantiate_substreams(self.test_catalog)

        for sub in self.test_stream.substreams:
            self.assertIsInstance(
                sub,
                Substream,
                msg=f"instantiate_substreams did not instantiate substream: {sub}",
            )

    def test_sync_substreams_with_non_substream(self):
        """A TypeError should be raised if substreams aren't instantiated yet"""

        self.test_stream.substreams = [Mock()]

        with self.assertRaises(TypeError):
            list(self.test_stream.sync_substreams({}, MagicMock()))

    def test_has_substreams(self):
        self.assertTrue(self.test_stream.has_substreams)
        del self.test_stream.substream_definitions[0]
        self.assertFalse(self.test_stream.has_substreams)

    @patch.object(Stream, "_check_replication_config")
    def test_check_replication_config_invoked_on_instantiation(
        self, mocked_check_replication_config
    ):
        self.TestStream(self.test_catalog, {})

        mocked_check_replication_config.assert_called_once()

    def test_check_replication_config(self):
        self.test_stream.replication_key = None
        self.assertIsNone(
            self.test_stream._check_replication_config()  # pylint: disable=protected-access
        )

        self.test_stream.replication_key = "NOT_IN_VALID_REPLICATION_KEYS"
        with self.assertRaises(ValueError):
            self.test_stream._check_replication_config()  # pylint: disable=protected-access

        # Test that valid config doesn't cause issues
        self.test_stream.valid_replication_keys = ["valid_key"]
        self.test_stream.replication_key = "valid_key"
        try:
            self.test_stream._check_replication_config()  # pylint: disable=protected-access
        except ValueError:
            self.fail(
                "`Stream._check_replication_config()` raised a ValueError for a valid replication config"
            )

    def test_is_valid_incremental(self):
        self.test_stream.replication_method = "INCREMENTAL"
        self.test_stream.replication_key = "valid_key"
        self.assertTrue(self.test_stream.is_valid_incremental)

        # Test if FULL_TABLE over INCREMENTAL
        self.test_stream.replication_method = "FULL_TABLE"
        self.assertFalse(self.test_stream.is_valid_incremental)

        # Test if replication_key is None
        self.test_stream.replication_method = "INCREMENTAL"
        self.test_stream.replication_key = None
        self.assertFalse(self.test_stream.is_valid_incremental)

    def test_updates_replication_config_from_catalog_entry(self):
        test_catalog = generate_catalog(
            [
                {
                    "tap_stream_id": "test_stream",
                    "selected": True,
                    "replication_key": "modified_at",
                    "replication_method": "FULL_TABLE",
                }
            ]
        )

        self.TestStream.valid_replication_keys = ["modified_at"]
        test_stream = self.TestStream(catalog=test_catalog, config={})

        self.assertEqual(test_stream.replication_key, "modified_at")
        self.assertEqual(test_stream.replication_method, "FULL_TABLE")
