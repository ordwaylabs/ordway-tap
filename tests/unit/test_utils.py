from unittest import TestCase
from unittest.mock import MagicMock, patch
from datetime import datetime
from pytz import UTC
from tap_ordway.utils import denest, get_company_id, get_full_table_version, get_version


@patch.dict(
    "tap_ordway.configs.api_credentials",
    {"company": "foo-bar"},
    clear=True,
)
def test_get_company_id():
    assert get_company_id() == "foo_bar"


@patch("tap_ordway.utils.time", return_value=1337)
def test_get_full_table_version(mocked_time):
    """ Ensure get_full_table_version is based off of time.time converted to ms """

    assert get_full_table_version() == 1337000
    assert mocked_time.call_count == 1


class GetVersionTestCase(TestCase):
    @patch("tap_ordway.utils.get_full_table_version")
    def test_full_table_stream(self, mocked_get_full_table_version):
        """ Ensure get_full_table_version is invoked on FULL_TABLE streams """

        mocked_stream = MagicMock()
        mocked_stream.is_valid_incremental = False

        get_version(mocked_stream, "2020-01-01", None)

        self.assertEqual(mocked_get_full_table_version.call_count, 1)

    def test_version_is_1_when_incremental_first_run(self):
        mocked_stream = MagicMock()
        mocked_stream.is_valid_incremental = True

        version = get_version(
            mocked_stream, "2020-01-01", datetime(2018, 12, 1, tzinfo=UTC)
        )
        self.assertEqual(version, 1)

        version = get_version(mocked_stream, "2020-01-01", None)
        self.assertEqual(version, 1)

    def test_version_is_None_on_incremental_non_first_runs(self):
        mocked_stream = MagicMock()
        mocked_stream.is_valid_incremental = True

        version = get_version(
            mocked_stream, "2020-01-01", datetime(2020, 2, 1, tzinfo=UTC)
        )
        self.assertIsNone(version)


class DenestTestCase(TestCase):
    def test_empty_path_returns_original_value(self):
        """ Ensure path being empty returns the original result in a list """

        results = denest({"foo": "bar"}, tuple([]))

        self.assertIsInstance(results, list)
        self.assertListEqual(results, [{"foo": "bar"}])

    def test_unmatched_path_returns_empty_list(self):
        results = denest({"foo": "bar"}, ("can", "not", "follow"))

        self.assertListEqual(results, [])

    def test_denests_with_dict(self):
        results = denest(
            {"plans": [{"charge": {"id": 1}}, {"charge": {"id": 2}}]},
            ("plans", "charge"),
        )

        self.assertListEqual(results, [{"id": 1}, {"id": 2}])
