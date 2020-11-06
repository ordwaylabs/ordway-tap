from typing import (
    TYPE_CHECKING,
    Dict,
    Any,
    List,
    Optional,
    Sequence,
    Union,
    Type,
    Tuple,
    Generator,
)
from abc import ABC, abstractmethod
from collections.abc import Iterable as IterableCollection
from singer import get_logger
from singer.metadata import to_map as mdata_to_map
from ..base import DataContext
from ..utils import (
    denest,
)

if TYPE_CHECKING:
    from datetime import datetime
    from singer.catalog import CatalogEntry
    from ..transformers import RecordTransformer
    from ..api import RequestHandler

LOGGER = get_logger()


def _attach_tap_stream_id(
    tap_stream_id: str, record_generator: Generator[Dict[str, Any], None, None]
) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
    """ Appends the related tap_stream_id to a Record. """

    for record in record_generator:
        yield (tap_stream_id, record)


class StreamABC(ABC):
    """ Stream abstract base class """

    replication_method = "INCREMENTAL"
    replication_key = "updated_date"
    valid_replication_keys = ["updated_date"]

    def __init__(self, catalog: "Catalog", config: Dict[str, Any]):
        self.catalog_entry = catalog.get_stream(self.tap_stream_id)
        self.config = config
        self.replication_key = self.catalog_entry.replication_key
        self.replication_method = self.catalog_entry.replication_method

        self.mapped_metadata = mdata_to_map(self.catalog_entry.metadata)
        self.schema_dict = self.catalog_entry.schema.to_dict()

        self._check_replication_config()

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

        return self.catalog_entry.is_selected()

    @property
    @abstractmethod
    def transformer(self) -> "RecordTransformer":
        pass  # pragma: no cover

    @property
    @abstractmethod
    def tap_stream_id(self) -> Optional[str]:
        pass  # pragma: no cover

    @property
    @abstractmethod
    def key_properties(self) -> Sequence[str]:
        pass  # pragma: no cover

    def should_output(self, record: Dict[str, Any]) -> bool:
        return True

    def _check_replication_config(self) -> None:
        if self.replication_key is None:
            return None

        if self.replication_key not in self.valid_replication_keys:
            raise ValueError(
                f'"{self.replication_key}" is not a valid replication key for {self.tap_stream_id}'
            )


class Substream(StreamABC):
    """ Base class for all substreams """


class ResponseSubstream(Substream):
    """ A substream derived from a parent stream's response """

    @property
    @abstractmethod
    def path(self) -> Tuple[str, ...]:
        pass  # pragma: no cover


class EndpointSubstream(Substream):
    """ A substream derived from a parent's endpoint """

    @property
    @abstractmethod
    def request_handler(self) -> "RequestHandler":
        pass

    def sync(self, parent_record: Dict[str, Any], filter_datetime: "datetime") -> None:
        with self.transformer() as transformer:
            context = DataContext(
                stream=self,
                filter_datetime=filter_datetime,
                parent_record=parent_record,
                tap_stream_id=self.tap_stream_id,
            )

            for record in self.request_handler.fetch(context=context):
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
    substreams: List[Union[Substream, Type[Substream]]] = []

    @property
    @abstractmethod
    def request_handler(self) -> "RequestHandler":
        pass

    @property
    def has_substreams(self) -> bool:
        """ Whether the stream has any substreams """

        return len(self.substreams) > 0

    def instantiate_substreams(self, catalog: "Catalog") -> None:
        for i in range(len(self.substreams)):
            self.substreams[i] = self.substreams[i](catalog, self.config)

    def sync(
        self, filter_datetime: "datetime"
    ) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
        with self.transformer() as transformer:
            context = DataContext(
                stream=self,
                filter_datetime=filter_datetime,
                tap_stream_id=self.tap_stream_id,
            )

            for record in self.request_handler.fetch(context=context):
                yield from self.sync_substreams(record, filter_datetime)

                yield from _attach_tap_stream_id(
                    self.tap_stream_id,
                    transformer.transform(
                        record,
                        self.schema_dict,
                        context=context,
                        metadata=self.mapped_metadata,
                    ),
                )

    def sync_substreams(
        self, parent_record: Dict[str, Any], filter_datetime: "datetime"
    ) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
        for substream in self.substreams:
            if not isinstance(substream, (ResponseSubstream, EndpointSubstream)):
                raise TypeError(
                    f"Substream {substream.tap_stream_id} should be initialized before calling."
                )

            if not substream.is_selected:
                continue

            if isinstance(substream, ResponseSubstream):
                denested_value = denest(parent_record, substream.path)

                if not isinstance(denested_value, IterableCollection):
                    denested_value = [denested_value]

                with substream.transformer() as transformer:
                    for sub_record in denested_value:
                        yield from _attach_tap_stream_id(
                            substream.tap_stream_id,
                            transformer.transform(
                                sub_record,
                                substream.schema_dict,
                                context=DataContext(
                                    stream=substream,
                                    filter_datetime=filter_datetime,
                                    parent_record=parent_record,
                                    tap_stream_id=substream.tap_stream_id,
                                ),
                                metadata=substream.mapped_metadata,
                            ),
                        )
            elif isinstance(substream, EndpointSubstream):
                yield from substream.sync(parent_record, filter_datetime)
