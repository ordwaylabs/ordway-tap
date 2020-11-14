from typing import TYPE_CHECKING, Callable, Union, Optional, Dict, Any, Generator
from inspect import isgeneratorfunction
from inflection import singularize
from singer.transform import Transformer, NO_INTEGER_DATETIME_PARSING
from ..utils import get_company_id
from ..base import DataContext

if TYPE_CHECKING:
    from datetime import datetime


def _transform_string(data: Any) -> Optional[str]:
    if isinstance(data, str) and len(data) == 0:
        return None

    return data


def _transform_boolean(data: Any) -> Optional[bool]:
    return data if data != "-" else False


def transformer_prehook(data: Any, property_type: str, _) -> Any:
    if property_type == "boolean":
        return _transform_boolean(data)
    if property_type == "string":
        return _transform_string(data)
    if property_type == "null":
        # Treat '-' as equivalent to None
        if data == "-":
            return None

    return data


class RecordTransformer:
    """A wrapper around Singer's Transformer that allows transforming record data
    as a whole.
    """

    def __init__(
        self,
        integer_datetime_fmt=NO_INTEGER_DATETIME_PARSING,
        pre_hook=transformer_prehook,
    ):
        self._schema_transformer = Transformer(integer_datetime_fmt, pre_hook)
        self.removed = self._schema_transformer.removed
        self.filtered = self._schema_transformer.filtered
        self.errors = self._schema_transformer.errors

    @property
    def pre_hook(self) -> Callable:
        return self._schema_transformer.pre_hook

    @pre_hook.setter
    def pre_hook(self, pre_hook):
        self._schema_transformer.pre_hook = pre_hook

    @property
    def integer_datetime_fmt(self):
        return self._schema_transformer.integer_datetime_fmt

    @integer_datetime_fmt.setter
    def integer_datetime_fmt(self, integer_datetime_fmt):
        self._schema_transformer.integer_datetime_fmt = integer_datetime_fmt

    def __enter__(self):
        return self  # pragma: no cover

    def __exit__(self, *args):
        self._schema_transformer.log_warning()

    def pre_transform(
        self,
        data: Dict[str, Any],
        context: DataContext,
    ) -> Union[Dict[str, Any], Generator[Dict[str, Any], None, None]]:
        """Transforms `data` as a whole - as opposed to a pre_hook"""

        data["company_id"] = get_company_id()

        singular_tap_stream_id = f"{singularize(context.tap_stream_id)}_id"

        if singular_tap_stream_id not in data:
            data[singular_tap_stream_id] = data.get("id")

        if "id" in data:
            del data["id"]

        return data

    def transform(
        self,
        data: Dict[str, Any],
        schema,
        context: DataContext,
        metadata=None,
    ) -> Generator[Dict[str, Any], None, None]:
        if isgeneratorfunction(self.pre_transform):
            for pretransformed_data in self.pre_transform(data, context):
                yield self._schema_transformer.transform(
                    pretransformed_data, schema, metadata
                )
        else:
            yield self._schema_transformer.transform(
                self.pre_transform(data, context), schema, metadata
            )
