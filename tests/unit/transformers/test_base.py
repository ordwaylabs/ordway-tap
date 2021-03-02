from unittest import TestCase
from unittest.mock import MagicMock, patch
from tap_ordway.transformers.base import (
    NO_INTEGER_DATETIME_PARSING,
    DataContext,
    RecordTransformer,
    _transform_boolean,
    _transform_string,
    transformer_prehook,
)


def test_transform_string():
    assert _transform_string("") is None
    assert _transform_string(" ") == " "
    assert _transform_string("foo") == "foo"


def test_transform_boolean():
    assert _transform_boolean("-") == False is False
    # This is intentional behavior. We want the transformer to raise
    # this as a SchemaMismatch exception.
    assert _transform_boolean("foo") == "foo"
    assert _transform_boolean(True) is True
    assert _transform_boolean(False) is False


@patch("tap_ordway.transformers.base._transform_string")
@patch("tap_ordway.transformers.base._transform_boolean")
def test_tranform_prehook(mock_transform_boolean, mock_transform_string):
    transformer_prehook("foo", "string", None)
    mock_transform_string.assert_called_with("foo")

    transformer_prehook("-", "boolean", None)
    mock_transform_boolean.assert_called_with("-")

    transformer_prehook({}, "object", None)
    assert mock_transform_string.call_count == 1
    assert mock_transform_boolean.call_count == 1

    assert transformer_prehook("-", "null", None) is None


class RecordTransformerTestCase(TestCase):
    def setUp(self):
        self.transformer = RecordTransformer()

        self.get_company_id_patcher = patch(
            "tap_ordway.transformers.base.get_company_id"
        )
        self.mocked_get_company_id = self.get_company_id_patcher.start()
        self.mocked_get_company_id.return_value = "Sirius Cybernetics Corp"

        self.mocked_context = MagicMock(spec=DataContext)
        self.mocked_context.tap_stream_id = "charges"

    def tearDown(self):
        self.get_company_id_patcher.stop()

    def test_pre_transform_converts_ids(self):
        """A tap_stream_id of 'charges' should result in:
        - A charge_id property being added based on the original id
        - A company_id property being added
        - The original id key being removed
        """

        results = self.transformer.pre_transform(
            {"id": "CHG-55", "foo": "bar"}, context=self.mocked_context
        )
        self.assertDictEqual(
            results,
            {
                "foo": "bar",
                "company_id": "Sirius Cybernetics Corp",
                "charge_id": "CHG-55",
            },
        )

        results = self.transformer.pre_transform(
            {"charge_id": "CHG-55", "foo": "bar"}, context=self.mocked_context
        )
        self.assertDictEqual(
            results,
            {
                "charge_id": "CHG-55",
                "foo": "bar",
                "company_id": "Sirius Cybernetics Corp",
            },
        )

    def test_wrapped_property_getters(self):
        self.assertEqual(self.transformer.pre_hook, transformer_prehook)
        self.assertEqual(
            self.transformer.integer_datetime_fmt, NO_INTEGER_DATETIME_PARSING
        )
        self.assertIs(
            self.transformer.removed,
            self.transformer._schema_transformer.removed,  # pylint: disable=protected-access
        )
        self.assertIs(
            self.transformer.filtered,
            self.transformer._schema_transformer.filtered,  # pylint: disable=protected-access
        )
        self.assertIs(
            self.transformer.errors,
            self.transformer._schema_transformer.errors,  # pylint: disable=protected-access
        )

    def test_wrapped_property_setters(self):
        mocked_pre_hook = MagicMock()
        self.transformer.pre_hook = mocked_pre_hook
        self.assertIs(
            self.transformer._schema_transformer.pre_hook,  # pylint: disable=protected-access
            mocked_pre_hook,
        )

        mocked_integer_datetime_fmt = MagicMock()
        self.transformer.integer_datetime_fmt = mocked_integer_datetime_fmt
        self.assertIs(
            self.transformer._schema_transformer.integer_datetime_fmt,  # pylint: disable=protected-access
            mocked_integer_datetime_fmt,
        )

    def test_ctx_manager_exit_invokes_log_warning(self):
        with patch.object(
            self.transformer._schema_transformer,  # pylint: disable=protected-access
            "log_warning",
        ) as mocked_log_warning:
            self.transformer.__exit__(None)

            self.assertEqual(mocked_log_warning.call_count, 1)
