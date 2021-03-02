from typing import TYPE_CHECKING, Any, Dict, List, Union
from json import load as json_load
from pathlib import Path
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

if TYPE_CHECKING:
    from typing_extensions import TypedDict

    class StreamCatalogEntry(TypedDict):
        tap_stream_id: str
        selected: bool
        replication_method: str
        replication_key: str


FIXTURES_DIR = Path.cwd() / "tests" / "fixtures"


def load_fixture_json(rel_file_path: Path) -> Dict[str, Any]:
    abs_file_path = FIXTURES_DIR / rel_file_path

    with abs_file_path.open() as fp:  # pylint: disable=invalid-name
        return json_load(fp)


def load_schema(file_name: Union[str, Path]) -> Dict[str, Any]:
    """ Loads a schema file from fixtures """

    schema_file = Path("schemas") / file_name

    return load_fixture_json(schema_file)


def generate_catalog(streams: List["StreamCatalogEntry"]) -> Catalog:
    """Generates a catalog with an entry for each stream in `streams`"""

    entries = []

    for stream_entry in streams:
        if "replication_method" not in stream_entry:
            stream_entry["replication_method"] = "INCREMENTAL"
        if "replication_key" not in stream_entry:
            stream_entry["replication_key"] = "updated_date"

        entries.append(
            CatalogEntry(
                tap_stream_id=stream_entry["tap_stream_id"],
                schema=Schema(),
                replication_method=stream_entry["replication_method"],
                replication_key=stream_entry["replication_key"],
                metadata=[
                    {
                        "breadcrumb": tuple([]),
                        "metadata": {
                            "selected": stream_entry["selected"],
                        },
                    }
                ],
            )
        )

    return Catalog(entries)
