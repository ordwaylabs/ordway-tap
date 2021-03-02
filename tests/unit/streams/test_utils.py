from unittest import TestCase
from unittest.mock import MagicMock, patch
from tests.utils import generate_catalog
from tap_ordway.streams import EndpointSubstream, ResponseSubstream, Stream
from tap_ordway.streams.exceptions import DependencyConflict
from tap_ordway.streams.utils import check_dependency_conflicts, is_substream


class ChildStream(ResponseSubstream):
    tap_stream_id = "child"
    path = ("charges",)
    key_properties = []
    transformer_class = MagicMock()


class Child2Stream(EndpointSubstream):
    tap_stream_id = "child2"
    key_properties = []
    transformer_class = MagicMock()
    request_handler = MagicMock()


class ParentStream(Stream):
    tap_stream_id = "parent"
    substream_definitions = [ChildStream, Child2Stream]
    key_properties = []
    transformer_class = MagicMock()
    request_handler = MagicMock()


class CheckDependencyConflictsTestCase(TestCase):
    @patch.dict(
        "tap_ordway.streams.utils.AVAILABLE_STREAMS",
        {"parent": ParentStream, "child": ChildStream},
        clear=True,
    )
    def test_with_conflicting_parent_and_child_stream(self):
        test_catalog = generate_catalog(
            [
                {"tap_stream_id": "parent", "selected": False},
                {"tap_stream_id": "child", "selected": True},
            ]
        )

        with self.assertRaises(DependencyConflict):
            check_dependency_conflicts(test_catalog)

    @patch.dict(
        "tap_ordway.streams.utils.AVAILABLE_STREAMS",
        {"parent": ParentStream, "child": ChildStream},
        clear=True,
    )
    def test_with_non_conflicting_streams(self):
        test_catalog = generate_catalog(
            [
                {"tap_stream_id": "parent", "selected": True},
                {"tap_stream_id": "child", "selected": False},
            ]
        )

        try:
            check_dependency_conflicts(test_catalog)
        except DependencyConflict:
            self.fail(
                "check_dependency_conflicts raised a DependencyConflict exception"
            )

        test_catalog_2 = generate_catalog(
            [
                {"tap_stream_id": "parent", "selected": True},
                {"tap_stream_id": "child", "selected": True},
            ]
        )

        try:
            check_dependency_conflicts(test_catalog_2)
        except DependencyConflict:
            self.fail(
                "check_dependency_conflicts raised a DependencyConflict exception"
            )


class IsSubstreamTestCase(TestCase):
    def test_with_substream_instances(self):
        test_catalog = generate_catalog(
            [
                {"tap_stream_id": "parent", "selected": True},
                {"tap_stream_id": "child", "selected": True},
                {"tap_stream_id": "child2", "selected": True},
            ]
        )

        self.assertTrue(is_substream(ChildStream(test_catalog, {})))
        self.assertTrue(is_substream(Child2Stream(test_catalog, {})))
        self.assertFalse(is_substream(ParentStream(test_catalog, {})))

    def test_with_substream_classes(self):
        self.assertTrue(is_substream(ChildStream))
        self.assertTrue(is_substream(Child2Stream))
        self.assertFalse(is_substream(ParentStream))
