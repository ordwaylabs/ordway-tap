from unittest import TestCase
from unittest.mock import MagicMock, patch
from datetime import datetime
from pytz import UTC
from tap_ordway import filter_record, handle_record


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
