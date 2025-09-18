from unittest import TestCase
from unittest.mock import MagicMock, patch
from datetime import datetime
from pytz import UTC
from tests.utils import generate_catalog
from tap_ordway import filter_record, handle_record, prepare_stream


class PrepareStreamTestCase(TestCase):
    @patch("tap_ordway.write_activate_version", autospec=True)
    def test_is_first_run_true_full_table(self, mock_write_activate_version):
        """Ensure a FULL_TABLE run without `wrote_initial_activate_version` being set to True in
        bookmarks results in an initial ACTIVATE_VERSION message being sent
        """

        prepare_stream(
            tap_stream_id="webhooks",
            stream_defs={},
            stream_versions={},
            catalog=generate_catalog([
                {"tap_stream_id": "webhooks", "selected": True, "replication_key": None, "replication_method": "FULL_TABLE"},
            ]),
            config={"start_date": "2021-01-01"},
            state={"bookmarks": {"webhooks": {}}},
        )

        mock_write_activate_version.assert_called_once()

    @patch("tap_ordway.get_full_table_version", autospec=True)
    @patch("tap_ordway.write_activate_version", autospec=True)
    def test_is_first_run_full_table_as_substream(self, mock_write_activate_version, mock_get_full_table_version):
        """Ensure a FULL_TABLE run without `wrote_initial_activate_version` being set to True in
        bookmarks results in an initial ACTIVATE_VERSION message being sent for SUBSTREAMS
        """
        mock_get_full_table_version.return_value = 123

        prepare_stream(
            tap_stream_id="plans",
            stream_defs={},
            stream_versions={},
            catalog=generate_catalog([
                {"tap_stream_id": "plans", "selected": True, "replication_key": None, "replication_method": "FULL_TABLE"},
                {"tap_stream_id": "charges", "selected": True, "replication_key": None, "replication_method": "FULL_TABLE"},
            ]),
            config={"start_date": "2021-01-01"},
            state={"bookmarks": {"charges": {}}},
        )

        mock_write_activate_version.assert_any_call(
            "charges", 
            123
        )

    @patch("tap_ordway.write_activate_version", autospec=True)
    def test_is_first_run_false_full_table(self, mock_write_activate_version):
        """Ensure a FULL_TABLE run with `wrote_initial_activate_version` being set to True in
        bookmarks DOES NOT result in an initial ACTIVATE_VERSION message being sent
        """
        prepare_stream(
            tap_stream_id="webhooks",
            stream_defs={},
            stream_versions={},
            catalog=generate_catalog([
                {"tap_stream_id": "webhooks", "selected": True, "replication_key": None, "replication_method": "FULL_TABLE"},
            ]),
            config={"start_date": "2021-01-01"},
            state={"bookmarks": {"webhooks": {"wrote_initial_activate_version": True}}},
        )

        mock_write_activate_version.assert_not_called()

    @patch("tap_ordway.write_activate_version", autospec=True)
    def test_is_first_run_true_incremental(self, mock_write_activate_version):
        """Ensure an INCREMENTAL run with `wrote_initial_activate_version` being set to True in
        bookmarks DOES NOT result in an initial ACTIVATE_VERSION message being sent
        (expected not to be sent in general for INCREMENTAL)
        """
        prepare_stream(
            tap_stream_id="invoices",
            stream_defs={},
            stream_versions={},
            catalog=generate_catalog([
                {"tap_stream_id": "invoices", "selected": True, "replication_key": "updated_date", "replication_method": "INCREMENTAL"},
            ]),
            config={"start_date": "2021-01-01"},
            state={"bookmarks": {"invoices": {"wrote_initial_activate_version": True}}},
        )

        mock_write_activate_version.assert_not_called()


class FilterRecordTestCase(TestCase):
    def test_update_date_none(self):
        """Ensure False is returned if updated_date is not found"""

        self.assertFalse(
            filter_record(
                {}, MagicMock(filter_datetime=datetime(2020, 1, 1, tzinfo=UTC))
            )
        )

    def test_updated_date_lte_filter_datetime(self):
        """Ensure True is returned when updated_date is less than or equal to
        the filtering datetime
        """
        self.assertTrue(
            filter_record(
                {"updated_date": "2019-01-01"},
                MagicMock(filter_datetime=datetime(2020, 1, 1, tzinfo=UTC)),
            )
        )
        self.assertTrue(
            filter_record(
                {"updated_date": "2020-01-01"},
                MagicMock(filter_datetime=datetime(2020, 1, 1, tzinfo=UTC)),
            )
        )

    def test_updated_date_gt_filter_datetime(self):
        self.assertFalse(
            filter_record(
                {"updated_date": "2020-01-02"},
                MagicMock(filter_datetime=datetime(2020, 1, 1, tzinfo=UTC)),
            )
        )


class HandleRecordTestCase(TestCase):
    def test_with_full_table_stream(self):
        state = handle_record(
            "foo",
            record={"bar": "biz"},
            stream_def=MagicMock(is_valid_incremental=False, replication_key=None),
            stream_version=1234,
            state={"foo": "bar"},
        )

        self.assertDictEqual(state, {"currently_syncing": "foo", "foo": "bar"})

    def test_with_incremental_stream(self):
        """Ensure currently_syncing is set and bookmark add/updated"""

        state = handle_record(
            "foo",
            record={"bar": "biz", "modified_at": "2020-01-01"},
            stream_def=MagicMock(
                is_valid_incremental=True, replication_key="modified_at"
            ),
            stream_version=1,
            state={"foo": "bar"},
        )

        self.assertDictEqual(
            state,
            {
                "currently_syncing": "foo",
                "bookmarks": {"foo": {"modified_at": "2020-01-01"}},
                "foo": "bar",
            },
        )

    @patch("tap_ordway.is_substream", return_value=True)
    def test_with_incremental_substream(self, _):
        """Ensure substreams don't set currently_syncing"""

        # There's no point in setting currently_syncing
        # substreams since we rely on the parent stream, so
        # we can't immediately pick-up there.

        state = handle_record(
            "foo",
            record={"bar": "biz", "modified_at": "2020-01-01"},
            stream_def=MagicMock(
                is_valid_incremental=True, replication_key="modified_at"
            ),
            stream_version=1,
            state={"foo": "bar"},
        )

        self.assertDictEqual(
            state,
            {
                "bookmarks": {"foo": {"modified_at": "2020-01-01"}},
                "foo": "bar",
            },
        )

    def test_with_missing_replication_key(self):
        """Ensure the bookmarks aren't touched when the replication_key
        is missing from the record
        """

        state = handle_record(
            "foo",
            record={"bar": "biz"},
            stream_def=MagicMock(
                is_valid_incremental=True, replication_key="modified_at"
            ),
            stream_version=1,
            state={"foo": "bar"},
        )

        self.assertDictEqual(
            state,
            {
                "currently_syncing": "foo",
                "foo": "bar",
            },
        )
