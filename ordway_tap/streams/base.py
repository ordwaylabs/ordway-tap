from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
)
from abc import ABC, abstractmethod
from singer import get_logger
from singer.metadata import to_map as mdata_to_map
from ..base import DataContext
from ..utils import denest

if TYPE_CHECKING:
    from datetime import datetime
    from singer.catalog import Catalog, CatalogEntry
    from ..api import RequestHandler
    from ..transformers import RecordTransformer

LOGGER = get_logger()

# pylint: disable=invalid-name
_FILTER_HOOK = Callable[[Dict[str, str], DataContext], bool]


def _attach_tap_stream_id(
    tap_stream_id: str, record_generator: Generator[Dict[str, Any], None, None]
) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
    """ Appends the related tap_stream_id to a Record. """

    for record in record_generator:
        yield (tap_stream_id, record)


# pylint: disable=too-many-instance-attributes
class StreamABC(ABC):
    """ Stream abstract base class """

    replication_method = "INCREMENTAL"
    replication_key: Optional[str] = "updated_date"
    valid_replication_keys: Sequence[str] = ["updated_date"]

    def __init__(
        self,
        catalog: "Catalog",
        config: Dict[str, Any],
        filter_hook: Optional[_FILTER_HOOK] = None,
    ):
        self.catalog_entry = catalog.get_stream(self.tap_stream_id)
        self.config = config
        self.replication_key = self.catalog_entry.replication_key
        self.replication_method = self.catalog_entry.replication_method

        self.mapped_metadata = mdata_to_map(self.catalog_entry.metadata)
        self.schema_dict = self.catalog_entry.schema.to_dict()
        self.filter_hook: _FILTER_HOOK = (
            (lambda *_, **__: False) if filter_hook is None else filter_hook
        )

        self._check_replication_config()

        self._is_selected: Optional[bool] = None

    @property
    def is_valid_incremental(self) -> bool:
        """ Whether or not the stream is valid incremental """

        return (
            self.replication_method == "INCREMENTAL"
            and self.replication_key is not None
        )

    @property
    def is_selected(self) -> bool:
        """ Whether the stream is selected """

        if self._is_selected is None:
            self._is_selected = self.catalog_entry.is_selected()

        return self._is_selected

    @property
    @abstractmethod
    def transformer_class(self) -> Type["RecordTransformer"]:
        """The transformer class to apply to all RECORDs"""

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        pass  # pragma: no cover

    @property
    @abstractmethod
    def key_properties(self) -> Sequence[str]:
        pass  # pragma: no cover

    def _check_replication_config(self) -> None:
        if self.replication_key is None:
            return None  # type: ignore[unreachable]

        if self.replication_key not in self.valid_replication_keys:
            raise ValueError(
                f'"{self.replication_key}" is not a valid replication key for {self.tap_stream_id}'
            )

        return None


class Substream(StreamABC):
    """ Base class for all substreams """


class ResponseSubstream(Substream):
    """A substream derived from a parent stream's response

    When syncing its records, the parent stream will follow the path
    defined by this stream and invoke its transformer_class on any
    "sub records" found.
    """

    @property
    @abstractmethod
    def path(self) -> Tuple[str, ...]:
        """A tuple of dictionary keys from which the substream records can be found

        Example:
        {
            "A": {
                "B": [{"id": "sub_record"}]
            }
        }

        A `path` of ("A", "B") would result in {"id": "sub_record"} being synchronized.
        """


class EndpointSubstream(Substream):
    """A substream derived from a parent's endpoint """

    @property
    @abstractmethod
    def request_handler(self) -> "RequestHandler":
        pass

    def sync(
        self, parent_record: Dict[str, Any], filter_datetime: "datetime"
    ) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
        with self.transformer_class() as transformer:
            context = DataContext(
                stream=self,
                filter_datetime=filter_datetime,
                parent_record=parent_record,
                tap_stream_id=self.tap_stream_id,
            )

            for record in self.request_handler.fetch(context=context):
                if self.filter_hook(record, context):
                    continue

                yield from _attach_tap_stream_id(
                    self.tap_stream_id,
                    transformer.transform(
                        record,
                        self.schema_dict,
                        context=context,
                        metadata=self.mapped_metadata,
                    ),
                )


class Stream(StreamABC):
    substream_definitions: List[Type[Substream]] = []

    def __init__(
        self,
        catalog: "Catalog",
        config: Dict[str, Any],
        filter_hook: Optional[_FILTER_HOOK] = None,
    ):
        super().__init__(catalog, config, filter_hook)

        self.substreams: List[Substream] = []

    @property
    @abstractmethod
    def request_handler(self) -> "RequestHandler":
        pass

    @property
    def has_substreams(self) -> bool:
        """ Whether the stream has any substreams """

        return len(self.substream_definitions) > 0

    def instantiate_substreams(
        self,
        catalog: "Catalog",
        filter_hook: Optional[Callable[[Dict[str, str], DataContext], bool]] = None,
    ) -> None:
        self.substreams = [
            substream_class(catalog, self.config, filter_hook)
            for substream_class in self.substream_definitions
        ]

    def sync(
        self, filter_datetime: "datetime"
    ) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
        with self.transformer_class() as transformer:
            context = DataContext(
                stream=self,
                filter_datetime=filter_datetime,
                tap_stream_id=self.tap_stream_id,
            )

            for record in self.request_handler.fetch(context=context):
                yield from self.sync_substreams(record, filter_datetime)

                # Skip primary stream if record is filtered,
                # but give substreams a chance to perform
                # their own filtering.
                if self.filter_hook(record, context):
                    continue

                yield from _attach_tap_stream_id(
                    self.tap_stream_id,
                    transformer.transform(
                        record,
                        self.schema_dict,
                        context=context,
                        metadata=self.mapped_metadata,
                    ),
                )

    def sync_sub_records(
        self,
        substream: ResponseSubstream,
        parent_record: Dict[str, Any],
        filter_datetime: "datetime",
    ) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
        """Syncs an ResponseSubstream records given the `parent_record`"""

        context = DataContext(
            stream=substream,
            filter_datetime=filter_datetime,
            parent_record=parent_record,
            tap_stream_id=substream.tap_stream_id,
        )

        denested_value = denest(parent_record, substream.path)

        with substream.transformer_class() as transformer:
            for sub_record in denested_value:
                if self.filter_hook(sub_record, context):
                    continue

                yield from _attach_tap_stream_id(
                    substream.tap_stream_id,
                    transformer.transform(
                        sub_record,
                        substream.schema_dict,
                        context=context,
                        metadata=substream.mapped_metadata,
                    ),
                )

    def sync_substreams(
        self, parent_record: Dict[str, Any], filter_datetime: "datetime"
    ) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
        for substream in self.substreams:
            if not isinstance(substream, Substream):
                raise TypeError(
                    f"Substream {substream.tap_stream_id} should be initialized before calling."
                )

            if not substream.is_selected:
                continue

            if isinstance(substream, ResponseSubstream):
                yield from self.sync_sub_records(
                    substream, parent_record, filter_datetime
                )
            elif isinstance(substream, EndpointSubstream):
                yield from substream.sync(parent_record, filter_datetime)
