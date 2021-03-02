from typing import TYPE_CHECKING, Any, Dict, Hashable, Iterable, Tuple

if TYPE_CHECKING:
    from singer.catalog import Catalog


def deselect_all_streams_except(
    catalog: "Catalog", tap_stream_ids: Iterable[str]
) -> None:
    """ Mutates `catalog` to deselect all streams except those within `tap_stream_ids`. """

    for selected_stream in catalog.get_selected_streams({}):
        if selected_stream.tap_stream_id in tap_stream_ids:
            continue

        selected_stream.schema.selected = False
        selected_stream.metadata[0].get("metadata", {})["selected"] = False


def alter_nested_value(
    path: Tuple[Hashable, ...], obj: Dict[Hashable, Any], new_value: Any
) -> None:
    """Mutates an dictionary's value based on the `path` thereto"""

    if len(path) == 1:
        obj[path[0]] = new_value

        return None

    reach = obj[path[0]]

    for i, direction in enumerate(path[1:]):
        if isinstance(reach, (list, tuple)):
            for item in reach:
                alter_nested_value(path[i + 1 :], item, new_value)

            break

        if i == len(path[1:]) - 1:
            reach[direction] = new_value
        else:
            reach = reach[direction]

    return None


def dict_subset(
    original_dict: Dict[Hashable, Any], excluded_keys: Iterable[str]
) -> Dict[Hashable, Any]:
    """Shallowly removes `excluded_keys` from `orginal_dict`"""

    return {key: val for key, val in original_dict.items() if key not in excluded_keys}
