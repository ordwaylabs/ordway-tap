""" Provides simple masking/anonymizing utilities """

from typing import Any, Dict, Optional, Sequence, Union
from copy import deepcopy
from enum import Enum
from random import choices, randint
from re import sub as re_sub
from string import ascii_lowercase, digits


class StrFormat(Enum):
    """ The particular format a `str` is in. """

    FLOAT = "float"
    PHONE = "phone"
    EMAIL = "email"
    RANDOM = "random"


class Format:
    AUTO = "auto"


DEFAULT_MASKED_FORMATS = (StrFormat.FLOAT, StrFormat.EMAIL, StrFormat.PHONE)


def get_str_format(value: str) -> StrFormat:
    """Attempts to determine format of str

    Not intended to be sophisticated, but gives
    us a decent starting point, so we don't have
    to specify each field name.
    """

    # Probably good enough for this use case
    if "@" in value:
        return StrFormat.EMAIL

    if 10 <= len(re_sub(r"[\+\-\(\)]", "", value)) <= 11:
        return StrFormat.PHONE

    try:
        float(value)

        return StrFormat.FLOAT
    except (ValueError, TypeError):
        return StrFormat.RANDOM


def _rand_str(k: int = 8) -> str:
    return "".join(choices(digits + ascii_lowercase, k=k))


def _rand_num(
    exclude: Union[int, float] = None,
    negative: Optional[bool] = None,
    as_float: bool = False,
) -> Union[int, float]:
    rand_num = randint(0, 100000)

    if as_float:
        rand_num: float = float(rand_num)  # type: ignore

    if rand_num == exclude:  # pragma: no cover
        return _rand_num(exclude, negative, as_float=as_float)
    if negative:
        rand_num = -rand_num

    return rand_num


def _rand_email() -> str:
    return f"{_rand_str()}@example.com"


def _rand_phone() -> str:
    return "".join(choices(digits, k=11))


def _is_empty(value: Any):
    return value is None or (isinstance(value, str) and len(value) == 0)


def mask_value(
    value: Any, desired_format: Union[StrFormat, Format] = Format.AUTO
) -> Any:
    """ Returns a masked value """

    if isinstance(value, str):
        str_format = desired_format

        if desired_format is Format.AUTO:
            str_format = get_str_format(value)

        if str_format is StrFormat.FLOAT:
            try:
                val_float = float(value)
                rand_float = _rand_num(
                    exclude=val_float, negative=val_float < 0, as_float=True
                )

                return str(rand_float)
            except ValueError:
                return str(_rand_num(as_float=True))
        elif str_format is StrFormat.EMAIL:
            return _rand_email()
        elif str_format is StrFormat.PHONE:
            return _rand_phone()

        return _rand_str(k=randint(1, 50))
    elif isinstance(value, int):
        return _rand_num(exclude=value, negative=value < 0)
    elif isinstance(value, float):
        return _rand_num(exclude=value, negative=value < 0, as_float=True)

    return value


def mask_values(
    unmasked_dict,
    masked_formats: Optional[Sequence[StrFormat]] = None,
    masked_fields: Optional[Dict[str, Union[StrFormat, Format]]] = None,
):
    """ Returns a new dict with particular values masked. """

    if not isinstance(unmasked_dict, dict):
        return unmasked_dict

    masked_dict = deepcopy(unmasked_dict)

    if masked_formats is None:
        masked_formats = DEFAULT_MASKED_FORMATS
    if masked_fields is None:
        masked_fields = {}

    for key, value in masked_dict.items():
        if isinstance(value, dict):
            masked_dict[key] = mask_values(
                value, masked_formats=masked_formats, masked_fields=masked_fields
            )
        elif isinstance(value, list):
            masked_dict[key] = [
                mask_values(
                    _, masked_formats=masked_formats, masked_fields=masked_fields
                )
                for _ in value
            ]

        if key in masked_fields or (
            isinstance(value, str) and get_str_format(value) in masked_formats
        ):
            if not _is_empty(value):
                masked_dict[key] = mask_value(
                    value, desired_format=masked_fields.get(key, Format.AUTO)
                )

    return masked_dict


__all__ = ["mask_values", "mask_value", "StrFormat", "Format", "DEFAULT_MASKED_FORMATS"]
