from unittest import TestCase
from unittest.mock import patch
from tap_ordway.utils import (
    denest,
    get_company_id,
    get_full_table_version,
    is_first_run,
)


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


def test_is_first_run():
    """Ensure returns True if `wrote_initial_activate_version` is either: a non-True value or not
    found in bookmarks - else returns False
    """

    assert (
        is_first_run("payments", {"bookmarks": {"payments": {"wrote_initial_activate_version": True}}})
        is False
    )
    assert (
        is_first_run("payments", {"bookmarks": {"payments": {"wrote_initial_activate_version": False}}})
        is True
    )
    assert is_first_run("payments", {"bookmarks": {}}) is True
    assert (
        is_first_run("payments", {"bookmarks": {"payments": {"wrote_initial_activate_version": 1}}})
        is True
    )
    assert (
        is_first_run("payments", {"bookmarks": {"payments": {"wrote_initial_activate_version": "true"}}})
        is True
    )


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
